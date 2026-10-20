"""
Unit tests for src/main.py
"""

import logging
import textwrap
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import Mock, patch

import pytest
from click.testing import CliRunner

from src.common.config_loader import (
    ConfigError,
    ServerConfig,
    from_legacy,
    get_config,
    parse_config,
    resolve_passwords,
    set_config,
)
from src.main import RedisMCPServer, apply_api_key, cli, main

CONFIG_API_KEY = "0123456789abcdef0123456789abcdef"
# what --api-key / REDIS_MCP_API_KEY set (e.g. from a Secret), different from the config's apiKey
OVERRIDE_API_KEY = "fedcba9876543210fedcba9876543210"

# values api_key_error refuses: blank, padded (could never match a trimmed header) and template placeholders
INVALID_API_KEYS = [
    ("   ", "blank"),
    (" padded-key-0123456789 ", "whitespace"),
    ("<api-key-here>", "placeholder"),
    ("${REDIS_MCP_API_KEY}", "placeholder"),
]

# --url values that do not count as a URL (gemini-cli passes "${REDIS_URL}" verbatim when it is not set)
PLACEHOLDER_URLS = ["", "   ", "${REDIS_URL}", " ${REDIS_URL} "]
PLACEHOLDER_URL_IDS = ["empty", "blank", "placeholder", "padded-placeholder"]

CONFIG_YAML = """
apiKey: {api_key}
readOnly: true
toolsets: [core, data]
passwordsDir: {passwords_dir}
clusters:
  - name: cbtstrds
    mode: standalone
    hosts: host:6379
    db: 0
    username: mcp-reader
    readOnly: false
  - name: cbprdrds
    mode: cluster
    hosts: [h1:6379, h2:6379]
"""


def write(path: Path, text: str) -> Path:
    path.write_text(textwrap.dedent(text).lstrip("\n"), encoding="utf-8")
    return path


def write_config(
    tmp_path: Path, extra: str = "", api_key: str = CONFIG_API_KEY
) -> Path:
    text = CONFIG_YAML.format(
        passwords_dir=tmp_path, api_key=api_key
    ) + textwrap.dedent(extra)
    return write(tmp_path / "redis-mcp.yaml", text)


def make_config(tmp_path: Path, **root) -> ServerConfig:
    data = {
        "passwordsDir": str(tmp_path),
        "clusters": [{"name": "a", "hosts": "a:6379"}],
    }
    data.update(root)
    return parse_config(data)


@contextmanager
def legacy_settings():
    """REDIS_CFG as the CLI writes it (src.common.config) and reads it (src.main), restored afterwards.
    tests/test_config.py reloads src.common.config, after which src.main still holds the old dict."""
    import src.common.config as config_module

    with (
        patch.dict(config_module.REDIS_CFG),
        patch("src.main.REDIS_CFG", config_module.REDIS_CFG),
    ):
        yield config_module.REDIS_CFG


@pytest.fixture(autouse=True)
def preserve_logging():
    """RedisMCPServer configures logging; restore the root and mcp loggers after each test."""
    root = logging.getLogger()
    mcp_logger = logging.getLogger("mcp")
    saved_handlers = [(h, h.level) for h in root.handlers]
    saved_level = root.level
    saved_mcp_level = mcp_logger.level
    yield
    for h in list(root.handlers):
        root.removeHandler(h)
    for h, level in saved_handlers:
        root.addHandler(h)
        h.setLevel(level)
    root.setLevel(saved_level)
    mcp_logger.setLevel(saved_mcp_level)
    logging.captureWarnings(False)


@pytest.fixture(autouse=True)
def no_config_env(monkeypatch):
    """The host environment must not select a config file or an API key for these tests."""
    monkeypatch.delenv("REDIS_MCP_CONFIG", raising=False)
    monkeypatch.delenv("REDIS_MCP_CONFIG_DIR", raising=False)
    monkeypatch.delenv("REDIS_MCP_API_KEY", raising=False)


@pytest.fixture
def logging_setup():
    """Replaces both logging setups so a test can tell which one the server picked."""
    with (
        patch("src.main.configure_logging") as plain,
        patch("src.main.configure_json_logging") as json_logging,
    ):
        yield Mock(plain=plain, json=json_logging)


class TestRedisMCPServer:
    """Test cases for RedisMCPServer class."""

    def test_init_logs_startup_message(self, capsys, caplog, monkeypatch):
        """Startup should emit an INFO log; client may route it via handlers.
        Accept either stderr output or log record text.
        """
        monkeypatch.setenv("MCP_REDIS_LOG_LEVEL", "INFO")

        with caplog.at_level(logging.INFO):
            server = RedisMCPServer()
            assert server is not None

        captured = capsys.readouterr()
        stderr_text = captured.err or ""
        log_text = caplog.text or ""  # collected by pytest logging handler
        combined = stderr_text + "\n" + log_text
        assert "Starting the Redis MCP Server" in combined

    def test_run_calls_mcp_run(self):
        """Test that RedisMCPServer.run() calls mcp.run()."""
        server = RedisMCPServer()

        with patch.object(server.mcp, "run") as mock_mcp_run:
            server.run()

        mock_mcp_run.assert_called_once_with()

    @patch("src.main.build_server")
    def test_run_propagates_exceptions(self, mock_build_server):
        """Test that exceptions from mcp.run() are propagated."""
        mock_build_server.return_value.run.side_effect = Exception("MCP run failed")
        server = RedisMCPServer()

        with pytest.raises(Exception, match="MCP run failed"):
            server.run()

    def test_without_config_uses_active_config(self, tmp_path, logging_setup):
        config = make_config(tmp_path)
        set_config(config)

        server = RedisMCPServer()

        assert server.config is config

    def test_without_any_config_uses_legacy_default_cluster(self, logging_setup):
        server = RedisMCPServer()

        assert server.config.cluster_names() == ["default"]
        assert server.transport == "stdio"

    def test_given_config_becomes_active(self, tmp_path, logging_setup):
        config = make_config(tmp_path)

        server = RedisMCPServer(config=config)

        assert server.config is config
        assert get_config() is config

    def test_builds_server_from_config(self, tmp_path, logging_setup):
        config = make_config(tmp_path)

        with patch("src.main.build_server") as mock_build_server:
            server = RedisMCPServer(config=config)

        mock_build_server.assert_called_once_with(config)
        assert server.mcp is mock_build_server.return_value

    async def test_real_server_lists_config_tools(self, tmp_path, logging_setup):
        server = RedisMCPServer(config=make_config(tmp_path, toolsets=["core"]))

        names = [t.name for t in await server.mcp.list_tools()]
        assert "clusters_list" in names
        assert "set" not in names

    @pytest.mark.parametrize(
        "port, expected",
        [(None, "stdio"), ("0", "stdio"), ("8080", "http")],
        ids=["no-port", "port-0", "port-set"],
    )
    def test_transport_default(self, tmp_path, logging_setup, port, expected):
        root = {} if port is None else {"port": port}

        server = RedisMCPServer(config=make_config(tmp_path, **root))

        assert server.transport == expected

    @pytest.mark.parametrize(
        "port, transport",
        [("8080", "stdio"), (None, "http")],
        ids=["stdio-with-port", "http-without-port"],
    )
    def test_explicit_transport_wins(self, tmp_path, logging_setup, port, transport):
        root = {} if port is None else {"port": port}

        server = RedisMCPServer(
            config=make_config(tmp_path, **root), transport=transport
        )

        assert server.transport == transport

    def test_stdio_configures_plain_logging(self, tmp_path, logging_setup):
        RedisMCPServer(config=make_config(tmp_path))

        logging_setup.plain.assert_called_once_with()
        logging_setup.json.assert_not_called()

    def test_http_configures_json_logging(self, tmp_path, logging_setup):
        RedisMCPServer(config=make_config(tmp_path, port="8080"))

        logging_setup.json.assert_called_once_with()
        logging_setup.plain.assert_not_called()

    def test_resolves_passwords_after_configuring_logging(self, tmp_path):
        config = make_config(tmp_path)
        calls = Mock()

        with (
            patch("src.main.configure_logging", calls.configure_logging),
            patch("src.main.resolve_passwords", calls.resolve_passwords),
            patch("src.main.build_server", calls.build_server),
        ):
            RedisMCPServer(config=config)

        names = [c[0] for c in calls.mock_calls if "." not in c[0]]
        assert names == ["configure_logging", "resolve_passwords", "build_server"]
        calls.resolve_passwords.assert_called_once_with(config)

    def test_resolves_password_from_env(self, tmp_path, monkeypatch, logging_setup):
        monkeypatch.setenv("a_pass", "from-env")
        config = make_config(tmp_path)

        RedisMCPServer(config=config)

        assert config.passwords_resolved
        assert config.cluster("a").password == "from-env"

    def test_resolves_password_from_file(self, tmp_path, monkeypatch, logging_setup):
        monkeypatch.delenv("a_pass", raising=False)
        (tmp_path / "a_pass").write_text("from-file\n", encoding="utf-8")
        config = make_config(tmp_path)

        RedisMCPServer(config=config)

        assert config.cluster("a").password == "from-file"

    def test_does_not_resolve_resolved_passwords(
        self, tmp_path, monkeypatch, logging_setup
    ):
        monkeypatch.setenv("a_pass", "first")
        config = make_config(tmp_path)
        resolve_passwords(config)
        monkeypatch.setenv("a_pass", "second")

        with patch("src.main.resolve_passwords") as mock_resolve:
            RedisMCPServer(config=config)

        mock_resolve.assert_not_called()
        assert config.cluster("a").password == "first"

    def test_legacy_config_keeps_its_password(self, logging_setup):
        config = from_legacy({"host": "h", "port": 6379, "password": "legacy-pw"})

        with patch("src.main.resolve_passwords") as mock_resolve:
            RedisMCPServer(config=config)

        mock_resolve.assert_not_called()
        assert config.cluster("default").password == "legacy-pw"

    def test_logs_tool_count_and_clusters(self, tmp_path, logging_setup, caplog):
        caplog.set_level(logging.INFO, logger="src.main")
        config = make_config(tmp_path, readOnly=True)

        RedisMCPServer(config=config)

        messages = [r.getMessage() for r in caplog.records if r.name == "src.main"]
        assert "Starting the Redis MCP Server" in messages
        summary = [m for m in messages if "tools listed" in m]
        assert len(summary) == 1
        assert "['a']" in summary[0]
        assert "readOnly=True" in summary[0]

    def test_unknown_toolset_raises_config_error(self, tmp_path, logging_setup):
        with pytest.raises(ConfigError, match="Unknown toolset"):
            RedisMCPServer(config=make_config(tmp_path, toolsets=["nope"]))

    def test_run_stdio_calls_mcp_run_only(self, tmp_path, logging_setup):
        with patch("src.main.build_server") as mock_build_server:
            server = RedisMCPServer(config=make_config(tmp_path), transport="stdio")

        with patch("src.common.http.run_http") as mock_run_http:
            server.run()

        mock_build_server.return_value.run.assert_called_once_with()
        mock_run_http.assert_not_called()

    def test_run_http_calls_run_http(self, tmp_path, logging_setup):
        with patch("src.main.build_server") as mock_build_server:
            server = RedisMCPServer(config=make_config(tmp_path, port="8080"))

        with patch("src.common.http.run_http") as mock_run_http:
            server.run()

        mock_run_http.assert_called_once_with(server.mcp, server.config)
        mock_build_server.return_value.run.assert_not_called()

    def test_run_http_propagates_exceptions(self, tmp_path, logging_setup):
        server = RedisMCPServer(config=make_config(tmp_path), transport="http")

        with patch("src.common.http.run_http", side_effect=OSError("address in use")):
            with pytest.raises(OSError, match="address in use"):
                server.run()


class TestCLI:
    """Test cases for CLI interface."""

    def setup_method(self):
        """Set up test fixtures."""
        self.runner = CliRunner()

    @patch("src.main.parse_redis_uri")
    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_cli_with_url_parameter(
        self, mock_server_class, mock_set_config, mock_parse_uri
    ):
        """Test CLI with --url parameter."""
        mock_parse_uri.return_value = {"host": "localhost", "port": 6379}
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(cli, ["--url", "redis://localhost:6379/0"])

        assert result.exit_code == 0
        mock_parse_uri.assert_called_once_with("redis://localhost:6379/0")
        mock_set_config.assert_called_once_with({"host": "localhost", "port": 6379})
        mock_server_class.assert_called_once()
        mock_server.run.assert_called_once()

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_cli_with_individual_parameters(self, mock_server_class, mock_set_config):
        """Test CLI with individual connection parameters."""
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(
            cli,
            [
                "--host",
                "redis.example.com",
                "--port",
                "6380",
                "--db",
                "1",
                "--username",
                "testuser",
                "--password",
                "testpass",
                "--ssl",
            ],
        )

        assert result.exit_code == 0
        mock_set_config.assert_called_once()

        # Verify the config passed to set_redis_config_from_cli
        call_args = mock_set_config.call_args[0][0]
        assert call_args["host"] == "redis.example.com"
        assert call_args["port"] == 6380
        assert call_args["db"] == 1
        assert call_args["username"] == "testuser"
        assert call_args["password"] == "testpass"
        assert call_args["ssl"] is True

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_cli_with_ssl_parameters(self, mock_server_class, mock_set_config):
        """Test CLI with SSL-specific parameters."""
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(
            cli,
            [
                "--ssl",
                "--ssl-ca-path",
                "/path/to/ca.pem",
                "--ssl-keyfile",
                "/path/to/key.pem",
                "--ssl-certfile",
                "/path/to/cert.pem",
                "--ssl-cert-reqs",
                "optional",
                "--ssl-ca-certs",
                "/path/to/ca-bundle.pem",
            ],
        )

        assert result.exit_code == 0
        call_args = mock_set_config.call_args[0][0]
        assert call_args["ssl"] is True
        assert call_args["ssl_ca_path"] == "/path/to/ca.pem"
        assert call_args["ssl_keyfile"] == "/path/to/key.pem"
        assert call_args["ssl_certfile"] == "/path/to/cert.pem"
        assert call_args["ssl_cert_reqs"] == "optional"
        assert call_args["ssl_ca_certs"] == "/path/to/ca-bundle.pem"

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_cli_with_cluster_mode(self, mock_server_class, mock_set_config):
        """Test CLI with cluster mode enabled."""
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(cli, ["--cluster-mode"])

        assert result.exit_code == 0
        call_args = mock_set_config.call_args[0][0]
        assert call_args["cluster_mode"] is True

    @patch("src.main.parse_redis_uri")
    def test_cli_with_invalid_url(self, mock_parse_uri):
        """Test CLI with invalid Redis URL."""
        mock_parse_uri.side_effect = ValueError("Invalid Redis URI")

        result = self.runner.invoke(cli, ["--url", "invalid://url"])

        assert result.exit_code != 0
        assert "Invalid Redis URI" in result.output

    @patch("src.main.RedisMCPServer")
    def test_cli_server_initialization_failure(self, mock_server_class):
        """Test CLI when server initialization fails."""
        mock_server_class.side_effect = Exception("Server init failed")

        result = self.runner.invoke(cli, [])

        assert result.exit_code != 0

    @patch("src.main.RedisMCPServer")
    def test_cli_server_run_failure(self, mock_server_class):
        """Test CLI when server run fails."""
        mock_server = Mock()
        mock_server.run.side_effect = Exception("Server run failed")
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(cli, [])

        assert result.exit_code != 0

    def test_cli_help(self):
        """Test CLI help output."""
        result = self.runner.invoke(cli, ["--help"])

        assert result.exit_code == 0
        assert "Redis connection URI" in result.output
        assert "--host" in result.output
        assert "--port" in result.output
        assert "--ssl" in result.output
        assert "--config" in result.output
        assert "--config-dir" in result.output
        assert "--transport" in result.output
        assert "--http-port" in result.output
        assert "--api-key" in result.output
        assert "REDIS_MCP_API_KEY" in result.output

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_cli_default_values(self, mock_server_class, mock_set_config):
        """Test CLI with default values."""
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(cli, [])

        assert result.exit_code == 0
        # Should be called with empty config when no parameters provided
        mock_set_config.assert_called_once()
        call_args = mock_set_config.call_args[0][0]

        # Check that only non-None values are in the config
        for key, value in call_args.items():
            if value is not None:
                # These should be the default values or explicitly set values
                assert isinstance(value, (str, int, bool))

    @patch("src.main.parse_redis_uri")
    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_cli_url_overrides_individual_params(
        self, mock_server_class, mock_set_config, mock_parse_uri
    ):
        """Test that --url parameter takes precedence over individual parameters."""
        mock_parse_uri.return_value = {"host": "uri-host", "port": 9999}
        mock_server = Mock()
        mock_server_class.return_value = mock_server

        result = self.runner.invoke(
            cli,
            [
                "--url",
                "redis://uri-host:9999/0",
                "--host",
                "individual-host",
                "--port",
                "6379",
            ],
        )

        assert result.exit_code == 0
        mock_parse_uri.assert_called_once_with("redis://uri-host:9999/0")
        # Should use URI config, not individual parameters
        call_args = mock_set_config.call_args[0][0]
        assert call_args["host"] == "uri-host"
        assert call_args["port"] == 9999

    @patch("src.main.RedisMCPServer")
    def test_cli_cluster_mode_with_ca_path_is_refused(self, mock_server_class):
        """RedisCluster ignores ssl_ca_path, so the nodes would be verified against the system CAs instead."""
        with legacy_settings():
            result = self.runner.invoke(
                cli, ["--cluster-mode", "--ssl", "--ssl-ca-path", "/etc/ssl/certs"]
            )

        assert result.exit_code == 2
        assert result.stderr.startswith("redis-mcp: Invalid config")
        mock_server_class.assert_not_called()

    @pytest.mark.xfail(
        strict=True,
        reason="src bug: in the legacy mode the error names the YAML keys (clusters[0]: tls.caPath, caFile)"
        " instead of --ssl-ca-path / REDIS_SSL_CA_PATH and --ssl-ca-certs, which the user actually set",
    )
    @patch("src.main.RedisMCPServer")
    def test_cli_cluster_mode_ca_path_error_names_the_flag(self, mock_server_class):
        with legacy_settings():
            result = self.runner.invoke(
                cli, ["--cluster-mode", "--ssl", "--ssl-ca-path", "/etc/ssl/certs"]
            )

        assert result.exit_code == 2
        assert "--ssl-ca-path" in result.stderr
        assert "--ssl-ca-certs" in result.stderr

    @pytest.mark.parametrize("url", PLACEHOLDER_URLS, ids=PLACEHOLDER_URL_IDS)
    @patch("src.main.parse_redis_uri")
    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_cli_empty_or_placeholder_url_uses_individual_params(
        self, mock_server_class, mock_set_config, mock_parse_uri, url
    ):
        """gemini-cli passes the raw "${REDIS_URL}" when the variable is not set."""
        result = self.runner.invoke(
            cli, ["--url", url, "--host", "individual-host", "--port", "6390"]
        )

        assert result.exit_code == 0, result.output
        mock_parse_uri.assert_not_called()
        call_args = mock_set_config.call_args[0][0]
        assert call_args["host"] == "individual-host"
        assert call_args["port"] == 6390

    @patch("src.main.set_redis_config_from_cli")
    @patch("src.main.RedisMCPServer")
    def test_cli_legacy_mode_builds_default_cluster(
        self, mock_server_class, mock_set_config
    ):
        result = self.runner.invoke(cli, [])

        assert result.exit_code == 0
        kwargs = mock_server_class.call_args.kwargs
        assert kwargs["config"].cluster_names() == ["default"]
        assert kwargs["config"].port is None
        assert kwargs["config"].api_key is None
        assert kwargs["transport"] is None


class TestCLIConfig:
    """--config / --config-dir / --transport / --http-port."""

    def setup_method(self):
        self.runner = CliRunner()

    @pytest.fixture(autouse=True)
    def server_class(self):
        with patch("src.main.RedisMCPServer") as mock_server_class:
            yield mock_server_class

    @pytest.fixture(autouse=True)
    def legacy_setters(self):
        with (
            patch("src.main.set_redis_config_from_cli") as redis_setter,
            patch("src.main.set_entraid_config_from_cli") as entraid_setter,
        ):
            yield Mock(redis=redis_setter, entraid=entraid_setter)

    @staticmethod
    def server_kwargs(server_class):
        server_class.assert_called_once()
        return server_class.call_args.kwargs

    def test_config_file_is_loaded(self, tmp_path, server_class, legacy_setters):
        path = write_config(tmp_path)

        result = self.runner.invoke(cli, ["--config", str(path)])

        assert result.exit_code == 0, result.output
        kwargs = self.server_kwargs(server_class)
        config = kwargs["config"]
        assert isinstance(config, ServerConfig)
        assert config.cluster_names() == ["cbtstrds", "cbprdrds"]
        assert config.read_only is True
        assert config.toolsets == ["core", "data"]
        assert config.cluster("cbtstrds").username == "mcp-reader"
        assert config.cluster("cbprdrds").addresses() == [("h1", 6379), ("h2", 6379)]
        assert config.port is None
        assert kwargs["transport"] is None
        server_class.return_value.run.assert_called_once_with()
        legacy_setters.redis.assert_not_called()

    def test_config_passwords_left_to_the_server(self, tmp_path, server_class):
        (tmp_path / "cbtstrds_pass").write_text("12345\n", encoding="utf-8")
        path = write_config(tmp_path)

        result = self.runner.invoke(cli, ["--config", str(path)])

        assert result.exit_code == 0, result.output
        config = self.server_kwargs(server_class)["config"]
        assert config.passwords_resolved is False

    def test_config_dir_is_loaded(self, tmp_path, server_class):
        drop_ins = tmp_path / "conf.d"
        drop_ins.mkdir()
        write(
            drop_ins / "10-a.yaml",
            """
            toolsets: [core]
            clusters:
              - name: a
                hosts: a:6379
            """,
        )
        write(
            drop_ins / "20-b.yml",
            """
            clusters:
              - name: b
                hosts: b:6379
            """,
        )

        result = self.runner.invoke(cli, ["--config-dir", str(drop_ins)])

        assert result.exit_code == 0, result.output
        config = self.server_kwargs(server_class)["config"]
        assert config.cluster_names() == ["a", "b"]

    def test_config_dir_merges_over_config(self, tmp_path, server_class):
        path = write_config(tmp_path)
        drop_ins = tmp_path / "conf.d"
        drop_ins.mkdir()
        write(
            drop_ins / "override.yaml",
            """
            readOnly: false
            clusters:
              - name: cbprdrds
                readOnly: true
              - name: extra
                hosts: extra:6379
            """,
        )

        result = self.runner.invoke(
            cli, ["--config", str(path), "--config-dir", str(drop_ins)]
        )

        assert result.exit_code == 0, result.output
        config = self.server_kwargs(server_class)["config"]
        assert config.cluster_names() == ["cbtstrds", "cbprdrds", "extra"]
        assert config.read_only is False
        assert config.cluster("cbprdrds").read_only is True
        assert config.cluster("cbprdrds").mode == "cluster"

    def test_env_config_is_used(self, tmp_path, server_class, legacy_setters):
        path = write_config(tmp_path)

        result = self.runner.invoke(cli, [], env={"REDIS_MCP_CONFIG": str(path)})

        assert result.exit_code == 0, result.output
        config = self.server_kwargs(server_class)["config"]
        assert config.cluster_names() == ["cbtstrds", "cbprdrds"]
        legacy_setters.redis.assert_not_called()

    def test_env_config_dir_is_used(self, tmp_path, server_class):
        drop_ins = tmp_path / "conf.d"
        drop_ins.mkdir()
        write(
            drop_ins / "a.yaml",
            """
            clusters:
              - name: fromdir
                hosts: d:6379
            """,
        )

        result = self.runner.invoke(
            cli, [], env={"REDIS_MCP_CONFIG_DIR": str(drop_ins)}
        )

        assert result.exit_code == 0, result.output
        config = self.server_kwargs(server_class)["config"]
        assert config.cluster_names() == ["fromdir"]

    def test_option_overrides_env(self, tmp_path, server_class):
        env_file = write(
            tmp_path / "env.yaml",
            """
            clusters:
              - name: fromenv
                hosts: e:6379
            """,
        )
        cli_file = write(
            tmp_path / "cli.yaml",
            """
            clusters:
              - name: fromcli
                hosts: c:6379
            """,
        )

        result = self.runner.invoke(
            cli, ["--config", str(cli_file)], env={"REDIS_MCP_CONFIG": str(env_file)}
        )

        assert result.exit_code == 0, result.output
        config = self.server_kwargs(server_class)["config"]
        assert config.cluster_names() == ["fromcli"]

    @pytest.mark.parametrize(
        "args, flag",
        [
            (["--host", "redis.example.com"], "--host"),
            (["--port", "6380"], "--port"),
            (["--url", "redis://localhost:6379/0"], "--url"),
            (["--cluster-mode"], "--cluster-mode"),
            (["--db", "1"], "--db"),
            (["--username", "u"], "--username"),
            (["--password", "p"], "--password"),
            (["--ssl"], "--ssl"),
            (["--ssl-ca-certs", "/ca.pem"], "--ssl-ca-certs"),
        ],
    )
    def test_legacy_flags_cannot_be_combined_with_config(
        self, tmp_path, server_class, args, flag
    ):
        path = write_config(tmp_path)

        result = self.runner.invoke(cli, ["--config", str(path), *args])

        assert result.exit_code == 2
        assert result.stderr.startswith("redis-mcp: ")
        assert "cannot be combined" in result.stderr
        assert flag in result.stderr
        server_class.assert_not_called()

    def test_legacy_flags_cannot_be_combined_with_config_dir(
        self, tmp_path, server_class
    ):
        result = self.runner.invoke(cli, ["--config-dir", str(tmp_path), "--host", "h"])

        assert result.exit_code == 2
        assert "cannot be combined" in result.stderr
        server_class.assert_not_called()

    def test_legacy_flags_cannot_be_combined_with_env_config(
        self, tmp_path, server_class
    ):
        path = write_config(tmp_path)

        result = self.runner.invoke(
            cli, ["--port", "6380"], env={"REDIS_MCP_CONFIG": str(path)}
        )

        assert result.exit_code == 2
        assert "--port cannot be combined" in result.stderr
        server_class.assert_not_called()

    def test_every_given_legacy_flag_is_named(self, tmp_path):
        path = write_config(tmp_path)

        result = self.runner.invoke(
            cli, ["--config", str(path), "--host", "h", "--port", "1", "--cluster-mode"]
        )

        assert result.exit_code == 2
        assert "--host, --port, --cluster-mode cannot be combined" in result.stderr

    @pytest.mark.parametrize("url", PLACEHOLDER_URLS, ids=PLACEHOLDER_URL_IDS)
    def test_empty_or_placeholder_url_is_accepted_with_config(
        self, tmp_path, server_class, legacy_setters, url
    ):
        """gemini-cli passes the raw "${REDIS_URL}" when the variable is not set; that is no --url."""
        path = write_config(tmp_path)

        with patch("src.main.parse_redis_uri") as mock_parse_uri:
            result = self.runner.invoke(cli, ["--config", str(path), "--url", url])

        assert result.exit_code == 0, result.output
        config = self.server_kwargs(server_class)["config"]
        assert config.cluster_names() == ["cbtstrds", "cbprdrds"]
        mock_parse_uri.assert_not_called()
        legacy_setters.redis.assert_not_called()

    def test_placeholder_url_is_not_named_next_to_a_real_flag(
        self, tmp_path, server_class
    ):
        path = write_config(tmp_path)

        result = self.runner.invoke(
            cli, ["--config", str(path), "--url", "${REDIS_URL}", "--host", "h"]
        )

        assert result.exit_code == 2
        assert "--host cannot be combined" in result.stderr
        assert "--url" not in result.stderr
        server_class.assert_not_called()

    def test_real_url_is_rejected_without_echoing_it(self, tmp_path, server_class):
        path = write_config(tmp_path)

        result = self.runner.invoke(
            cli, ["--config", str(path), "--url", "redis://u:url-secret@h:6379/0"]
        )

        assert result.exit_code == 2
        assert "--url cannot be combined" in result.stderr
        assert "url-secret" not in result.output
        server_class.assert_not_called()

    def test_invalid_config_exits_2(self, tmp_path, server_class):
        path = write(tmp_path / "bad.yaml", "clusters: []\n")

        result = self.runner.invoke(cli, ["--config", str(path)])

        assert result.exit_code == 2
        assert result.stderr.startswith("redis-mcp: Invalid config")
        assert "at least one cluster" in result.stderr
        server_class.assert_not_called()

    def test_unknown_config_key_exits_2(self, tmp_path, server_class):
        path = write(
            tmp_path / "bad.yaml",
            """
            nope: 1
            clusters:
              - name: a
                hosts: a:6379
            """,
        )

        result = self.runner.invoke(cli, ["--config", str(path)])

        assert result.exit_code == 2
        assert "redis-mcp: Invalid config" in result.stderr
        assert "nope" in result.stderr

    def test_missing_config_file_exits_2(self, tmp_path, server_class):
        result = self.runner.invoke(cli, ["--config", str(tmp_path / "missing.yaml")])

        assert result.exit_code == 2
        assert "redis-mcp: Could not read config file" in result.stderr
        server_class.assert_not_called()

    def test_config_dir_that_is_a_file_exits_2(self, tmp_path, server_class):
        path = write_config(tmp_path)

        result = self.runner.invoke(cli, ["--config-dir", str(path)])

        assert result.exit_code == 2
        server_class.assert_not_called()

    def test_unknown_toolset_exits_2(self, tmp_path, server_class):
        path = write_config(tmp_path, "toolsets: [core, nope]\n")
        server_class.side_effect = RedisMCPServer

        with patch("src.main.configure_logging"):
            result = self.runner.invoke(cli, ["--config", str(path)])

        assert result.exit_code == 2
        assert "redis-mcp: Unknown toolset" in result.stderr
        assert "nope" in result.stderr

    def test_server_config_error_exits_2(self, tmp_path, server_class):
        server_class.side_effect = ConfigError("Could not read password file x")
        path = write_config(tmp_path)

        result = self.runner.invoke(cli, ["--config", str(path)])

        assert result.exit_code == 2
        assert result.stderr == "redis-mcp: Could not read password file x\n"

    def test_config_api_key_is_kept_without_override(self, tmp_path, server_class):
        path = write_config(tmp_path)

        result = self.runner.invoke(cli, ["--config", str(path)])

        assert result.exit_code == 0, result.output
        assert self.server_kwargs(server_class)["config"].api_key == CONFIG_API_KEY

    def test_api_key_option_overrides_config(self, tmp_path, server_class):
        path = write_config(tmp_path)

        result = self.runner.invoke(
            cli, ["--config", str(path), "--api-key", OVERRIDE_API_KEY]
        )

        assert result.exit_code == 0, result.output
        config = self.server_kwargs(server_class)["config"]
        assert config.api_key == OVERRIDE_API_KEY
        assert config.api_key_enabled

    def test_api_key_env_overrides_config(self, tmp_path, server_class):
        path = write_config(tmp_path)

        result = self.runner.invoke(
            cli,
            ["--config", str(path)],
            env={"REDIS_MCP_API_KEY": OVERRIDE_API_KEY},
        )

        assert result.exit_code == 0, result.output
        assert self.server_kwargs(server_class)["config"].api_key == OVERRIDE_API_KEY

    def test_api_key_option_wins_over_env(self, tmp_path, server_class):
        path = write_config(tmp_path)

        result = self.runner.invoke(
            cli,
            ["--config", str(path), "--api-key", OVERRIDE_API_KEY],
            env={"REDIS_MCP_API_KEY": "env-key-0123456789abcdef"},
        )

        assert result.exit_code == 0, result.output
        assert self.server_kwargs(server_class)["config"].api_key == OVERRIDE_API_KEY

    @pytest.mark.parametrize("api_key", ["", '""'], ids=["unset", "empty"])
    def test_api_key_turns_on_authentication_of_a_config_without_one(
        self, tmp_path, server_class, api_key
    ):
        path = write_config(tmp_path, api_key=api_key)

        result = self.runner.invoke(
            cli, ["--config", str(path)], env={"REDIS_MCP_API_KEY": OVERRIDE_API_KEY}
        )

        assert result.exit_code == 0, result.output
        config = self.server_kwargs(server_class)["config"]
        assert config.api_key == OVERRIDE_API_KEY
        assert config.api_key_enabled

    @pytest.mark.parametrize(
        "args, env",
        [(["--api-key", ""], {}), ([], {"REDIS_MCP_API_KEY": ""})],
        ids=["option", "env"],
    )
    def test_empty_api_key_is_ignored(self, tmp_path, server_class, args, env):
        """An empty value (e.g. an unset variable in a manifest) cannot switch authentication off."""
        path = write_config(tmp_path)

        result = self.runner.invoke(cli, ["--config", str(path), *args], env=env)

        assert result.exit_code == 0, result.output
        config = self.server_kwargs(server_class)["config"]
        assert config.api_key == CONFIG_API_KEY
        assert config.api_key_enabled

    @pytest.mark.parametrize("source", ["option", "env"])
    @pytest.mark.parametrize(
        "api_key",
        [v for v, _ in INVALID_API_KEYS],
        ids=[i for _, i in INVALID_API_KEYS],
    )
    def test_invalid_api_key_exits_2(self, tmp_path, server_class, source, api_key):
        path = write_config(tmp_path)
        if source == "option":
            args, env = ["--config", str(path), "--api-key", api_key], {}
        else:
            args, env = ["--config", str(path)], {"REDIS_MCP_API_KEY": api_key}

        result = self.runner.invoke(cli, args, env=env)

        assert result.exit_code == 2
        assert result.stderr.startswith("redis-mcp: --api-key / REDIS_MCP_API_KEY: ")
        assert result.stderr.count("\n") == 1
        if api_key.strip():
            # the error explains the problem without repeating the value
            assert api_key.strip() not in result.output
        server_class.assert_not_called()

    def test_invalid_api_key_also_fails_in_legacy_mode(self, server_class):
        result = self.runner.invoke(cli, ["--api-key", " key-with-spaces "])

        assert result.exit_code == 2
        assert "redis-mcp: --api-key / REDIS_MCP_API_KEY: " in result.stderr
        assert "whitespace" in result.stderr
        server_class.assert_not_called()

    def test_api_key_is_not_a_legacy_connection_flag(self, tmp_path, server_class):
        """--api-key goes with --config (unlike the Redis connection flags)."""
        path = write_config(tmp_path)

        result = self.runner.invoke(
            cli,
            [
                "--config",
                str(path),
                "--api-key",
                OVERRIDE_API_KEY,
                "--http-port",
                "9090",
            ],
        )

        assert result.exit_code == 0, result.output
        assert "cannot be combined" not in result.output

    def test_legacy_mode_api_key_option(self, server_class, legacy_setters):
        result = self.runner.invoke(cli, ["--api-key", OVERRIDE_API_KEY])

        assert result.exit_code == 0, result.output
        config = self.server_kwargs(server_class)["config"]
        assert config.cluster_names() == ["default"]
        assert config.api_key == OVERRIDE_API_KEY
        legacy_setters.redis.assert_called_once()

    def test_legacy_mode_transport_http_with_api_key_env(self, server_class):
        result = self.runner.invoke(
            cli,
            ["--transport", "http"],
            env={"REDIS_MCP_API_KEY": OVERRIDE_API_KEY},
        )

        assert result.exit_code == 0, result.output
        kwargs = self.server_kwargs(server_class)
        assert kwargs["transport"] == "http"
        assert kwargs["config"].port == 8080
        assert kwargs["config"].cluster_names() == ["default"]
        assert kwargs["config"].api_key == OVERRIDE_API_KEY

    def test_legacy_mode_without_api_key_has_none(self, server_class):
        result = self.runner.invoke(cli, ["--transport", "http"])

        assert result.exit_code == 0, result.output
        config = self.server_kwargs(server_class)["config"]
        assert config.api_key is None
        assert not config.api_key_enabled

    def test_http_port_sets_port(self, tmp_path, server_class):
        path = write_config(tmp_path)

        result = self.runner.invoke(cli, ["--config", str(path), "--http-port", "9090"])

        assert result.exit_code == 0, result.output
        kwargs = self.server_kwargs(server_class)
        assert kwargs["config"].port == 9090
        assert kwargs["transport"] is None

    def test_http_port_overrides_config_port(self, tmp_path, server_class):
        path = write_config(tmp_path, "port: 8081\n")

        result = self.runner.invoke(cli, ["--config", str(path), "--http-port", "9090"])

        assert result.exit_code == 0, result.output
        assert self.server_kwargs(server_class)["config"].port == 9090

    def test_transport_http_defaults_port_to_8080(self, tmp_path, server_class):
        path = write_config(tmp_path)

        result = self.runner.invoke(cli, ["--config", str(path), "--transport", "http"])

        assert result.exit_code == 0, result.output
        kwargs = self.server_kwargs(server_class)
        assert kwargs["transport"] == "http"
        assert kwargs["config"].port == 8080

    def test_transport_http_keeps_config_port(self, tmp_path, server_class):
        path = write_config(tmp_path, "port: 9000\n")

        result = self.runner.invoke(cli, ["--config", str(path), "--transport", "http"])

        assert result.exit_code == 0, result.output
        assert self.server_kwargs(server_class)["config"].port == 9000

    def test_transport_http_with_http_port(self, tmp_path, server_class):
        path = write_config(tmp_path)

        result = self.runner.invoke(
            cli,
            ["--config", str(path), "--transport", "http", "--http-port", "9443"],
        )

        assert result.exit_code == 0, result.output
        kwargs = self.server_kwargs(server_class)
        assert kwargs["transport"] == "http"
        assert kwargs["config"].port == 9443

    def test_transport_stdio_is_passed_through(self, tmp_path, server_class):
        path = write_config(tmp_path, "port: 8080\n")

        result = self.runner.invoke(
            cli, ["--config", str(path), "--transport", "stdio"]
        )

        assert result.exit_code == 0, result.output
        kwargs = self.server_kwargs(server_class)
        assert kwargs["transport"] == "stdio"
        assert kwargs["config"].port == 8080

    @pytest.mark.parametrize("port", ["0", "65536", "http"])
    def test_http_port_out_of_range(self, tmp_path, server_class, port):
        path = write_config(tmp_path)

        result = self.runner.invoke(cli, ["--config", str(path), "--http-port", port])

        assert result.exit_code == 2
        assert "--http-port" in result.stderr
        server_class.assert_not_called()

    def test_unknown_transport(self, tmp_path, server_class):
        path = write_config(tmp_path)

        result = self.runner.invoke(cli, ["--config", str(path), "--transport", "sse"])

        assert result.exit_code == 2
        assert "--transport" in result.stderr
        server_class.assert_not_called()

    def test_legacy_mode_http_port(self, server_class, legacy_setters):
        result = self.runner.invoke(cli, ["--http-port", "9000"])

        assert result.exit_code == 0, result.output
        kwargs = self.server_kwargs(server_class)
        assert kwargs["config"].cluster_names() == ["default"]
        assert kwargs["config"].port == 9000
        assert kwargs["transport"] is None
        legacy_setters.redis.assert_called_once()

    def test_legacy_mode_transport_http(self, server_class):
        result = self.runner.invoke(cli, ["--transport", "http"])

        assert result.exit_code == 0, result.output
        kwargs = self.server_kwargs(server_class)
        assert kwargs["transport"] == "http"
        assert kwargs["config"].port == 8080

    @pytest.mark.parametrize(
        "extra, expected",
        [("", "stdio"), ("port: 8080\n", "http")],
        ids=["no-port", "port"],
    )
    def test_transport_default_follows_config_port(
        self, tmp_path, server_class, extra, expected
    ):
        server_class.side_effect = RedisMCPServer
        path = write_config(tmp_path, extra)

        with (
            patch("src.main.build_server"),
            patch("src.main.configure_logging"),
            patch("src.main.configure_json_logging"),
            patch.object(RedisMCPServer, "run", autospec=True) as mock_run,
        ):
            result = self.runner.invoke(cli, ["--config", str(path)])

        assert result.exit_code == 0, result.output
        server = mock_run.call_args[0][0]
        assert server.transport == expected


class TestMain:
    """main(): the entry point without CLI parsing (python src/main.py, the container image)."""

    @pytest.fixture(autouse=True)
    def server_class(self):
        with patch("src.main.RedisMCPServer") as mock_server_class:
            yield mock_server_class

    def test_env_config(self, tmp_path, monkeypatch, server_class):
        monkeypatch.setenv("REDIS_MCP_CONFIG", str(write_config(tmp_path)))

        main()

        server_class.assert_called_once()
        config = server_class.call_args.kwargs["config"]
        assert config.cluster_names() == ["cbtstrds", "cbprdrds"]
        server_class.return_value.run.assert_called_once_with()

    def test_env_config_dir(self, tmp_path, monkeypatch, server_class):
        drop_ins = tmp_path / "conf.d"
        drop_ins.mkdir()
        write(
            drop_ins / "a.yaml",
            """
            port: 8080
            clusters:
              - name: fromdir
                hosts: d:6379
            """,
        )
        monkeypatch.setenv("REDIS_MCP_CONFIG_DIR", str(drop_ins))

        main()

        config = server_class.call_args.kwargs["config"]
        assert config.cluster_names() == ["fromdir"]
        assert config.port == 8080

    def test_env_config_and_dir_merge(self, tmp_path, monkeypatch, server_class):
        drop_ins = tmp_path / "conf.d"
        drop_ins.mkdir()
        write(
            drop_ins / "a.yaml",
            """
            clusters:
              - name: more
                hosts: m:6379
            """,
        )
        monkeypatch.setenv("REDIS_MCP_CONFIG", str(write_config(tmp_path)))
        monkeypatch.setenv("REDIS_MCP_CONFIG_DIR", str(drop_ins))

        main()

        config = server_class.call_args.kwargs["config"]
        assert config.cluster_names() == ["cbtstrds", "cbprdrds", "more"]

    def test_without_env_uses_legacy(self, server_class):
        with patch.dict(
            "src.common.config.REDIS_CFG",
            {"host": "legacy-host", "port": 6390, "password": "legacy-pw"},
        ):
            main()

        server_class.assert_called_once()
        assert set(server_class.call_args.kwargs) == {"config"}
        config = server_class.call_args.kwargs["config"]
        # the config get_config() builds from the REDIS_* settings, which also becomes the active one
        assert config is get_config()
        assert config.cluster_names() == ["default"]
        assert config.cluster("default").hosts == "legacy-host:6390"
        assert config.cluster("default").password == "legacy-pw"
        assert config.passwords_resolved
        assert config.api_key is None
        server_class.return_value.run.assert_called_once_with()

    def test_legacy_config_comes_from_get_config(self, tmp_path, server_class):
        legacy = make_config(tmp_path)

        with patch("src.main.get_config", return_value=legacy) as mock_get_config:
            main()

        mock_get_config.assert_called_once_with()
        server_class.assert_called_once_with(config=legacy)

    def test_empty_env_is_ignored(self, monkeypatch, server_class):
        monkeypatch.setenv("REDIS_MCP_CONFIG", "")
        monkeypatch.setenv("REDIS_MCP_CONFIG_DIR", "")
        monkeypatch.setenv("REDIS_MCP_API_KEY", "")

        main()

        config = server_class.call_args.kwargs["config"]
        assert config.cluster_names() == ["default"]
        assert config.api_key is None

    def test_api_key_env_in_legacy_mode(self, monkeypatch, server_class):
        monkeypatch.setenv("REDIS_MCP_API_KEY", OVERRIDE_API_KEY)

        main()

        config = server_class.call_args.kwargs["config"]
        assert config.cluster_names() == ["default"]
        assert config.api_key == OVERRIDE_API_KEY
        server_class.return_value.run.assert_called_once_with()

    def test_api_key_env_overrides_config(self, tmp_path, monkeypatch, server_class):
        monkeypatch.setenv("REDIS_MCP_CONFIG", str(write_config(tmp_path)))
        monkeypatch.setenv("REDIS_MCP_API_KEY", OVERRIDE_API_KEY)

        main()

        config = server_class.call_args.kwargs["config"]
        assert config.cluster_names() == ["cbtstrds", "cbprdrds"]
        assert config.api_key == OVERRIDE_API_KEY

    def test_empty_api_key_env_keeps_config_key(
        self, tmp_path, monkeypatch, server_class
    ):
        monkeypatch.setenv("REDIS_MCP_CONFIG", str(write_config(tmp_path)))
        monkeypatch.setenv("REDIS_MCP_API_KEY", "")

        main()

        assert server_class.call_args.kwargs["config"].api_key == CONFIG_API_KEY

    @pytest.mark.parametrize("config_file", [False, True], ids=["legacy", "config"])
    @pytest.mark.parametrize(
        "api_key",
        [v for v, _ in INVALID_API_KEYS],
        ids=[i for _, i in INVALID_API_KEYS],
    )
    def test_invalid_api_key_env_exits_2(
        self, tmp_path, monkeypatch, capsys, server_class, config_file, api_key
    ):
        if config_file:
            monkeypatch.setenv("REDIS_MCP_CONFIG", str(write_config(tmp_path)))
        monkeypatch.setenv("REDIS_MCP_API_KEY", api_key)

        with pytest.raises(SystemExit) as exc:
            main()

        assert exc.value.code == 2
        err = capsys.readouterr().err
        assert err.startswith("redis-mcp: --api-key / REDIS_MCP_API_KEY: ")
        if api_key.strip():
            assert api_key.strip() not in err
        server_class.assert_not_called()

    def test_invalid_config_exits_2(self, tmp_path, monkeypatch, capsys, server_class):
        path = write(tmp_path / "bad.yaml", "clusters: []\n")
        monkeypatch.setenv("REDIS_MCP_CONFIG", str(path))

        with pytest.raises(SystemExit) as exc:
            main()

        assert exc.value.code == 2
        assert capsys.readouterr().err.startswith("redis-mcp: Invalid config")
        server_class.assert_not_called()

    def test_server_config_error_exits_2(self, monkeypatch, capsys, server_class):
        server_class.side_effect = ConfigError("Unknown toolset: ['nope']")

        with pytest.raises(SystemExit) as exc:
            main()

        assert exc.value.code == 2
        assert "redis-mcp: Unknown toolset" in capsys.readouterr().err


class TestApplyApiKey:
    """apply_api_key(): --api-key / REDIS_MCP_API_KEY over the config's apiKey."""

    @pytest.mark.parametrize("value", [None, ""], ids=["none", "empty"])
    def test_no_value_changes_nothing(self, tmp_path, value):
        config = make_config(tmp_path, apiKey=CONFIG_API_KEY)

        apply_api_key(config, value)

        assert config.api_key == CONFIG_API_KEY

    @pytest.mark.parametrize("value", [None, ""], ids=["none", "empty"])
    def test_no_value_keeps_authentication_off(self, tmp_path, value):
        config = make_config(tmp_path)

        apply_api_key(config, value)

        assert config.api_key is None
        assert not config.api_key_enabled

    def test_value_overrides_config_key(self, tmp_path):
        config = make_config(tmp_path, apiKey=CONFIG_API_KEY)

        apply_api_key(config, OVERRIDE_API_KEY)

        assert config.api_key == OVERRIDE_API_KEY
        assert config.api_key_enabled

    def test_value_turns_authentication_on(self, tmp_path):
        config = make_config(tmp_path, apiKey="")

        apply_api_key(config, OVERRIDE_API_KEY)

        assert config.api_key == OVERRIDE_API_KEY

    def test_legacy_config(self):
        config = from_legacy({"host": "h", "port": 6379})

        apply_api_key(config, OVERRIDE_API_KEY)

        assert config.api_key == OVERRIDE_API_KEY

    def test_short_key_is_accepted(self, tmp_path):
        """Like apiKey: a short key only produces a warning when the HTTP server starts."""
        config = make_config(tmp_path)

        apply_api_key(config, "short")

        assert config.api_key == "short"

    @pytest.mark.parametrize(
        "value, problem",
        [
            ("   ", "blank"),
            (" padded-key-0123456789", "whitespace"),
            ("padded-key-0123456789\n", "whitespace"),
            ("<api-key-here>", "placeholder"),
            ("${REDIS_MCP_API_KEY}", "placeholder"),
        ],
    )
    def test_invalid_value_raises_and_keeps_config_key(self, tmp_path, value, problem):
        config = make_config(tmp_path, apiKey=CONFIG_API_KEY)

        with pytest.raises(ConfigError) as exc:
            apply_api_key(config, value)

        message = str(exc.value)
        assert message.startswith("--api-key / REDIS_MCP_API_KEY: ")
        assert problem in message
        if value.strip():
            assert value.strip() not in message
        assert config.api_key == CONFIG_API_KEY
