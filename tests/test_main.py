"""
Unit tests for src/main.py
"""

import logging
import textwrap
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
from src.main import RedisMCPServer, cli, main

CONFIG_YAML = """
apiKey: 0123456789abcdef0123456789abcdef
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


def write_config(tmp_path: Path, extra: str = "") -> Path:
    text = CONFIG_YAML.format(passwords_dir=tmp_path) + textwrap.dedent(extra)
    return write(tmp_path / "redis-mcp.yaml", text)


def make_config(tmp_path: Path, **root) -> ServerConfig:
    data = {
        "passwordsDir": str(tmp_path),
        "clusters": [{"name": "a", "hosts": "a:6379"}],
    }
    data.update(root)
    return parse_config(data)


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
    """The host environment must not select a config file for these tests."""
    monkeypatch.delenv("REDIS_MCP_CONFIG", raising=False)
    monkeypatch.delenv("REDIS_MCP_CONFIG_DIR", raising=False)


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
        main()

        server_class.assert_called_once_with(config=None)
        server_class.return_value.run.assert_called_once_with()

    def test_empty_env_is_ignored(self, monkeypatch, server_class):
        monkeypatch.setenv("REDIS_MCP_CONFIG", "")
        monkeypatch.setenv("REDIS_MCP_CONFIG_DIR", "")

        main()

        server_class.assert_called_once_with(config=None)

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
