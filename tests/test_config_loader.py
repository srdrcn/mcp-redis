"""
Unit tests for src/common/config_loader.py
"""

import inspect
import logging
import textwrap
import traceback
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml
from pydantic import ValidationError
from redis import Redis
from redis.cluster import REDIS_ALLOWED_KEYS, RedisCluster
from redis.connection import AbstractConnection, Connection, ConnectionPool

from src.common import config_loader
from src.common.config_loader import (
    DEFAULT_PASSWORDS_DIR,
    LEGACY_CLUSTER_NAME,
    MODE_OPTIONS,
    ClusterConfig,
    ConfigError,
    ServerConfig,
    TlsConfig,
    api_key_error,
    drop_ins,
    from_legacy,
    get_config,
    load_config,
    merge,
    parse_config,
    parse_hosts,
    resolve_passwords,
    set_config,
)

LOGGER_NAME = "src.common.config_loader"


def write(path: Path, text: str) -> Path:
    path.write_text(textwrap.dedent(text).lstrip("\n"), encoding="utf-8")
    return path


def cluster(**fields):
    data = {"name": "a", "hosts": "h:6379"}
    data.update(fields)
    return data


def config_error(data) -> str:
    with pytest.raises(ConfigError) as exc:
        parse_config(data)
    return str(exc.value)


@pytest.fixture
def clean_password_env(monkeypatch):
    """Removes the <name>_pass variables the password tests use, so the host environment cannot leak in."""
    for name in ("pw1", "pw2", "pw3", "snt", "ent", "ent2", "nopw", "bad"):
        monkeypatch.delenv(f"{name}_pass", raising=False)
        monkeypatch.delenv(f"{name}_sentinel_pass", raising=False)
    return monkeypatch


FULL_CONFIG = """
port: 8080
apiKey: 0123456701234567abcdef
readOnly: true
disableDestructive: true
toolsets: [core, data, json]
passwordsDir: /etc/redis-mcp/passwords
clusters:
  - name: cbtstrds
    mode: standalone
    hosts: host:6379
    db: 3
    username: mcp-reader
    readOnly: false
  - name: cbprdrds
    mode: cluster
    hosts: [h1:6379, h2:6379]
    tls: true
    options:
      read_from_replicas: true
      require_full_coverage: false
      reinitialize_steps: 5
      max_connections: 32
  - name: x
    mode: sentinel
    hosts: s1:26379, s2:26379
    masterName: mymaster
    tls:
      caFile: /etc/redis-mcp/tls/ca.crt
      caPath: /etc/redis-mcp/tls
      certFile: /etc/redis-mcp/tls/tls.crt
      keyFile: /etc/redis-mcp/tls/tls.key
      certReqs: optional
      checkHostname: false
    modules: [json, search]
    entraid: false
    options:
      socket_timeout: 5
      socket_connect_timeout: 2.5
      health_check_interval: 1e2
      client_name: mcp
      socket_keepalive: true
  - name: v6
    hosts: "[::1]:6379"
"""


class TestValidConfig:
    """A complete config with every key."""

    @pytest.fixture
    def config(self, tmp_path) -> ServerConfig:
        return load_config(write(tmp_path / "redis-mcp.yaml", FULL_CONFIG))

    def test_top_level_keys(self, config):
        assert config.port == 8080
        assert config.api_key == "0123456701234567abcdef"
        assert config.api_key_enabled is True
        assert config.read_only is True
        assert config.disable_destructive is True
        assert config.toolsets == ["core", "data", "json"]
        assert config.passwords_dir == "/etc/redis-mcp/passwords"
        assert config.cluster_names() == ["cbtstrds", "cbprdrds", "x", "v6"]

    def test_standalone_cluster(self, config):
        c = config.cluster("cbtstrds")
        assert c.mode == "standalone"
        assert c.hosts == "host:6379"
        assert c.addresses() == [("host", 6379)]
        assert c.db == 3
        assert c.username == "mcp-reader"
        assert c.read_only is False
        assert c.tls is None
        assert c.master_name is None
        assert c.modules == []
        assert c.options == {}

    def test_hosts_list_is_joined(self, config):
        c = config.cluster("cbprdrds")
        assert c.mode == "cluster"
        assert c.hosts == "h1:6379,h2:6379"
        assert c.addresses() == [("h1", 6379), ("h2", 6379)]

    def test_tls_true_uses_defaults(self, config):
        tls = config.cluster("cbprdrds").tls
        assert tls == TlsConfig()
        assert tls.cert_reqs == "required"
        assert tls.client_kwargs() == {"ssl": True, "ssl_cert_reqs": "required"}

    def test_sentinel_cluster_with_tls_map(self, config):
        c = config.cluster("x")
        assert c.mode == "sentinel"
        assert c.master_name == "mymaster"
        assert c.addresses() == [("s1", 26379), ("s2", 26379)]
        assert c.tls == TlsConfig.model_validate(
            {
                "caFile": "/etc/redis-mcp/tls/ca.crt",
                "caPath": "/etc/redis-mcp/tls",
                "certFile": "/etc/redis-mcp/tls/tls.crt",
                "keyFile": "/etc/redis-mcp/tls/tls.key",
                "certReqs": "optional",
                "checkHostname": False,
            }
        )
        assert c.modules == ["json", "search"]
        assert c.entraid is False
        assert c.read_only is None

    def test_options_numbers_are_converted(self, config):
        options = config.cluster("x").options
        assert options == {
            "socket_timeout": 5,
            "socket_connect_timeout": 2.5,
            "health_check_interval": 100.0,
            "client_name": "mcp",
            "socket_keepalive": True,
        }
        assert type(options["socket_timeout"]) is int
        assert type(options["socket_connect_timeout"]) is float

    def test_cluster_mode_options(self, config):
        options = config.cluster("cbprdrds").options
        assert options == {
            "read_from_replicas": True,
            "require_full_coverage": False,
            "reinitialize_steps": 5,
            "max_connections": 32,
        }
        assert type(options["reinitialize_steps"]) is int

    def test_ipv6_host(self, config):
        c = config.cluster("v6")
        assert c.hosts == "[::1]:6379"
        assert c.addresses() == [("::1", 6379)]

    def test_unknown_cluster_lookup(self, config):
        assert config.cluster("nope") is None

    def test_passwords_are_not_resolved_by_load(self, config):
        assert config.passwords_resolved is False
        assert config.all_toolsets is False
        assert all(c.password is None for c in config.clusters)

    def test_defaults(self):
        config = parse_config({"clusters": [cluster()]})
        assert config.port is None
        assert config.api_key is None
        assert config.api_key_enabled is False
        assert config.read_only is False
        assert config.disable_destructive is False
        assert config.toolsets == ["core"]
        assert config.passwords_dir == DEFAULT_PASSWORDS_DIR
        c = config.clusters[0]
        assert c.mode == "standalone"
        assert c.db == 0
        assert c.username is None
        assert c.master_name is None
        assert c.tls is None
        assert c.read_only is None
        assert c.modules == []
        assert c.entraid is False
        assert c.options == {}
        assert c.password is None
        assert c.sentinel_password is None

    def test_empty_api_key_disables_authentication(self):
        config = parse_config({"apiKey": "", "clusters": [cluster()]})
        assert config.api_key == ""
        assert config.api_key_enabled is False

    def test_tls_false_means_no_tls(self):
        config = parse_config({"clusters": [cluster(tls=False)]})
        assert config.clusters[0].tls is None

    def test_cluster_mode_with_many_startup_nodes(self):
        config = parse_config(
            {"clusters": [cluster(mode="cluster", hosts="n1:7000, n2:7001,n3:7002")]}
        )
        assert config.clusters[0].addresses() == [
            ("n1", 7000),
            ("n2", 7001),
            ("n3", 7002),
        ]

    @pytest.mark.parametrize(
        "name",
        ["a", "0", "cbtstrds", "redis-prod_1.eu", "A" * 63],
    )
    def test_valid_cluster_names(self, name):
        assert parse_config({"clusters": [cluster(name=name)]}).clusters[0].name == name

    def test_port_bounds_are_accepted(self):
        assert parse_config({"port": "0", "clusters": [cluster()]}).port == 0
        assert parse_config({"port": "65535", "clusters": [cluster()]}).port == 65535

    def test_password_is_not_exposed(self):
        config = parse_config({"clusters": [cluster()]})
        config.clusters[0]._password = "TOPSECRET"
        config.clusters[0]._sentinel_password = "SENTINELSECRET"
        for text in (repr(config), str(config.model_dump()), config.model_dump_json()):
            assert "TOPSECRET" not in text
            assert "SENTINELSECRET" not in text


class TestYamlNumberTrap:
    """Numbers stay text in the YAML tree, and are converted only where a number is expected."""

    def test_numeric_looking_strings_keep_their_text(self, tmp_path):
        config = load_config(
            write(
                tmp_path / "c.yaml",
                """
                port: '8080'
                apiKey: 012345
                clusters:
                  - name: 2024-01-01
                    hosts: h:6379
                    db: 3
                    username: 0x1F
                  - name: s
                    mode: sentinel
                    hosts: s:26379
                    masterName: 1:20
                    username: 1_000
                """,
            )
        )
        assert config.port == 8080
        assert config.api_key == "012345"
        first, second = config.clusters
        assert first.name == "2024-01-01"
        assert first.db == 3
        assert first.username == "0x1F"
        assert second.master_name == "1:20"
        assert second.username == "1_000"

    def test_booleans_and_nulls_keep_their_type(self, tmp_path):
        config = load_config(
            write(
                tmp_path / "c.yaml",
                """
                readOnly: true
                disableDestructive: false
                apiKey: ~
                clusters:
                  - name: a
                    hosts: h:6379
                    readOnly: false
                    username: null
                    tls: true
                """,
            )
        )
        assert config.read_only is True
        assert config.disable_destructive is False
        assert config.api_key is None
        assert config.clusters[0].read_only is False
        assert config.clusters[0].username is None
        assert config.clusters[0].tls == TlsConfig()

    def test_loader_returns_text_for_numbers(self):
        data = yaml.load(  # nosec B506 - _TextLoader is a SafeLoader
            "a: 012345\nb: 0x1F\nc: 1_000\nd: 1:20\ne: 2024-01-01\nf: 1.5\ng: .inf\nh: true\ni: null\n",
            Loader=config_loader._TextLoader,
        )
        assert data == {
            "a": "012345",
            "b": "0x1F",
            "c": "1_000",
            "d": "1:20",
            "e": "2024-01-01",
            "f": "1.5",
            "g": ".inf",
            "h": True,
            "i": None,
        }

    def test_safe_loader_is_not_changed(self):
        assert yaml.safe_load("a: 8080\nb: 1.5") == {"a": 8080, "b": 1.5}

    def test_non_numeric_port_is_an_error(self, tmp_path):
        with pytest.raises(ConfigError) as exc:
            load_config(
                write(
                    tmp_path / "c.yaml",
                    "port: 0x1F90\nclusters: [{name: a, hosts: 'h:1'}]\n",
                )
            )
        assert "port: Input should be a valid integer" in str(exc.value)

    def test_python_tags_are_rejected(self, tmp_path):
        path = write(
            tmp_path / "c.yaml",
            "apiKey: !!python/object/apply:os.system ['true']\n",
        )
        with pytest.raises(ConfigError, match="Could not parse config file"):
            load_config(path)

    @pytest.mark.parametrize(
        "value, expected",
        [
            ("5", 5),
            ("+3", 3),
            ("007", 7),
            ("2.5", 2.5),
            (".5", 0.5),
            ("1e3", 1000.0),
            ("1.5E-1", 0.15),
            (7, 7),
            (0.25, 0.25),
        ],
    )
    def test_number_text_in_options_is_converted(self, value, expected):
        options = (
            parse_config({"clusters": [cluster(options={"socket_timeout": value})]})
            .clusters[0]
            .options
        )
        assert options == {"socket_timeout": expected}
        assert type(options["socket_timeout"]) is type(expected)

    @pytest.mark.parametrize(
        "value", ["-2", "abc", "5s", "", True, None, {"nested": "1"}]
    )
    def test_option_value_that_is_no_valid_number_is_rejected(self, value):
        # converted text is checked like any other value (see TestOptionValues)
        error = config_error({"clusters": [cluster(options={"socket_timeout": value})]})
        assert error == (
            "Invalid config:\n - clusters[0]: options.socket_timeout must be a number of seconds > 0"
        )

    @pytest.mark.parametrize("value", ["0123", "0x1F", "1.5", "1e3", "-2", "42"])
    def test_client_name_stays_text(self, value):
        # a name: "0123" must not become 123
        options = (
            parse_config({"clusters": [cluster(options={"client_name": value})]})
            .clusters[0]
            .options
        )
        assert options == {"client_name": value}
        assert type(options["client_name"]) is str

    def test_client_name_stays_text_from_yaml(self, tmp_path):
        config = load_config(
            write(
                tmp_path / "c.yaml",
                """
                clusters:
                  - name: a
                    hosts: h:6379
                    options:
                      client_name: 0123
                  - name: b
                    hosts: h:6380
                    options:
                      client_name: 1e3
                """,
            )
        )
        assert config.cluster("a").options == {"client_name": "0123"}
        assert config.cluster("b").options == {"client_name": "1e3"}

    def test_option_values_from_yaml(self, tmp_path):
        config = load_config(
            write(
                tmp_path / "c.yaml",
                """
                clusters:
                  - name: a
                    hosts: h:6379
                    options:
                      socket_timeout: 5
                      socket_connect_timeout: 0.5
                      health_check_interval: 1e1
                      max_connections: 012
                      client_name: 0x1F
                      socket_keepalive: true
                """,
            )
        )
        options = config.clusters[0].options
        # 012 is decimal 12 (not YAML 1.1 octal); 0x1F is not a decimal number, so it stays text
        assert options == {
            "socket_timeout": 5,
            "socket_connect_timeout": 0.5,
            "health_check_interval": 10.0,
            "max_connections": 12,
            "client_name": "0x1F",
            "socket_keepalive": True,
        }


class TestValidationErrors:
    """Every validation error, with the field path in the message."""

    @pytest.mark.parametrize(
        "data, path, message",
        [
            # cluster names
            (
                {"clusters": [cluster(name="a b")]},
                "clusters[0].name",
                "letters, digits",
            ),
            ({"clusters": [cluster(name="-a")]}, "clusters[0].name", "starting with"),
            ({"clusters": [cluster(name="")]}, "clusters[0].name", "at most 63"),
            ({"clusters": [cluster(name="a" * 64)]}, "clusters[0].name", "at most 63"),
            ({"clusters": [cluster(name="a/b")]}, "clusters[0].name", "letters"),
            (
                {"clusters": [cluster(), cluster(hosts="h:2"), cluster(name="b")]},
                "clusters",
                "duplicate cluster name: a",
            ),
            # mode
            (
                {"clusters": [cluster(), cluster(name="b", mode="bogus")]},
                "clusters[1].mode",
                "Input should be 'standalone', 'cluster' or 'sentinel'",
            ),
            (
                {"clusters": [cluster(hosts="h1:6379,h2:6379")]},
                "clusters[0]",
                "a standalone cluster takes exactly one host:port in hosts",
            ),
            (
                {"clusters": [cluster(), cluster(name="b", mode="sentinel")]},
                "clusters[1]",
                "masterName is required when mode is sentinel",
            ),
            (
                {"clusters": [cluster(masterName="m")]},
                "clusters[0]",
                "masterName is only used when mode is sentinel",
            ),
            (
                {"clusters": [cluster(mode="cluster", masterName="m")]},
                "clusters[0]",
                "masterName is only used when mode is sentinel",
            ),
            (
                {"clusters": [cluster(mode="cluster", db="1")]},
                "clusters[0]",
                "Redis Cluster only has database 0; remove db",
            ),
            (
                {"clusters": [cluster(db="-1")]},
                "clusters[0]",
                "db must be 0 or greater",
            ),
            (
                {"clusters": [cluster(db="x")]},
                "clusters[0].db",
                "Input should be a valid integer",
            ),
            # hosts
            (
                {"clusters": [{"name": "a"}]},
                "clusters[0].hosts",
                "Field required",
            ),
            (
                {"clusters": [cluster(hosts="h")]},
                "clusters[0].hosts",
                "hosts entry 1 is not host:port (or [IPv6 address]:port)",
            ),
            (
                {"clusters": [cluster(hosts="h:")]},
                "clusters[0].hosts",
                "hosts entry 1 is not host:port (or [IPv6 address]:port)",
            ),
            (
                {"clusters": [cluster(hosts=":6379")]},
                "clusters[0].hosts",
                "hosts entry 1 is not host:port (or [IPv6 address]:port)",
            ),
            (
                {"clusters": [cluster(hosts="h:abc")]},
                "clusters[0].hosts",
                "hosts entry 1 is not host:port (or [IPv6 address]:port)",
            ),
            (
                {"clusters": [cluster(hosts="h:-1")]},
                "clusters[0].hosts",
                "hosts entry 1 is not host:port (or [IPv6 address]:port)",
            ),
            (
                {"clusters": [cluster(hosts="")]},
                "clusters[0].hosts",
                "hosts entry 1 is not host:port (or [IPv6 address]:port)",
            ),
            (
                {"clusters": [cluster(hosts=[])]},
                "clusters[0].hosts",
                "hosts entry 1 is not host:port (or [IPv6 address]:port)",
            ),
            (
                {"clusters": [cluster(mode="cluster", hosts="h:1,")]},
                "clusters[0].hosts",
                "hosts entry 2 is not host:port (or [IPv6 address]:port)",
            ),
            (
                {"clusters": [cluster(hosts="my host:6379")]},
                "clusters[0].hosts",
                "hosts entry 1 is not host:port (or [IPv6 address]:port)",
            ),
            (
                {"clusters": [cluster(hosts="h:0")]},
                "clusters[0].hosts",
                "hosts entry 1: the port must be between 1 and 65535",
            ),
            (
                {"clusters": [cluster(mode="cluster", hosts="h:1,h:65536")]},
                "clusters[0].hosts",
                "hosts entry 2: the port must be between 1 and 65535",
            ),
            # top-level port
            (
                {"port": "-1", "clusters": [cluster()]},
                "port",
                "port must be between 0 and 65535",
            ),
            (
                {"port": "65536", "clusters": [cluster()]},
                "port",
                "port must be between 0 and 65535",
            ),
            (
                {"port": "http", "clusters": [cluster()]},
                "port",
                "Input should be a valid integer",
            ),
            # unknown keys
            (
                {"bogus": "1", "clusters": [cluster()]},
                "bogus",
                "Extra inputs are not permitted",
            ),
            (
                {"read_only": True, "clusters": [cluster()]},
                "read_only",
                "Extra inputs are not permitted",
            ),
            (
                {"clusters": [cluster(), cluster(name="b", password="x")]},
                "clusters[1].password",
                "Extra inputs are not permitted",
            ),
            (
                {"clusters": [cluster(master_name="m")]},
                "clusters[0].master_name",
                "Extra inputs are not permitted",
            ),
            (
                {"clusters": [cluster(tls={"verify": True})]},
                "clusters[0].tls.verify",
                "Extra inputs are not permitted",
            ),
            (
                {"clusters": [cluster(tls={"certReqs": "always"})]},
                "clusters[0].tls.certReqs",
                "Input should be 'required', 'optional' or 'none'",
            ),
            # hosts with credentials or a scheme (the entry is not echoed)
            (
                {"clusters": [cluster(hosts="user:pw@h:6379")]},
                "clusters[0].hosts",
                "hosts takes host:port entries without a scheme or credentials (user:password@);"
                " passwords come from the passwords directory",
            ),
            (
                {"clusters": [cluster(hosts="redis://h:6379")]},
                "clusters[0].hosts",
                "hosts takes host:port entries without a scheme or credentials",
            ),
            # options outside the mode's allowlist
            (
                {"clusters": [cluster(options={"password": "x"})]},
                "clusters[0]",
                "options password not supported in standalone mode (supported: ",
            ),
            (
                {"clusters": [cluster(options={"decode_responses": False})]},
                "clusters[0]",
                "options decode_responses not supported in standalone mode",
            ),
            (
                {
                    "clusters": [
                        cluster(),
                        cluster(name="b", mode="cluster", options={"retry": "x"}),
                    ]
                },
                "clusters[1]",
                "options retry not supported in cluster mode",
            ),
            (
                {"clusters": [cluster(options=["socket_timeout"])]},
                "clusters[0].options",
                "Input should be a valid dictionary",
            ),
            # option values
            (
                {"clusters": [cluster(), cluster(name="b", options={"protocol": "4"})]},
                "clusters[1]",
                "options.protocol must be 2 or 3",
            ),
            # tls
            (
                {"clusters": [cluster(mode="cluster", tls={"caPath": "/ca"})]},
                "clusters[0]",
                "tls.caPath is not supported in cluster mode; use caFile",
            ),
            # Sentinel password file clash
            (
                {
                    "clusters": [
                        cluster(
                            name="s", mode="sentinel", hosts="s:26379", masterName="m"
                        ),
                        cluster(name="s_sentinel"),
                    ]
                },
                "clusters",
                "cluster name s_sentinel clashes with the Sentinel password of cluster s"
                " (s_sentinel_pass); rename one of them",
            ),
            # modules
            (
                {"clusters": [cluster(modules=["timeseries"])]},
                "clusters[0].modules[0]",
                "Input should be 'json' or 'search'",
            ),
            (
                {"clusters": [cluster(modules=["json", "json"])]},
                "clusters[0].modules",
                "duplicate module",
            ),
            # apiKey
            (
                {"apiKey": "   ", "clusters": [cluster()]},
                "apiKey",
                "apiKey is blank",
            ),
            (
                {"apiKey": "\t", "clusters": [cluster()]},
                "apiKey",
                "apiKey is blank",
            ),
            (
                {"apiKey": " 0123456789abcdef", "clusters": [cluster()]},
                "apiKey",
                "apiKey has leading or trailing whitespace",
            ),
            (
                {"apiKey": "0123456789abcdef\n", "clusters": [cluster()]},
                "apiKey",
                "apiKey has leading or trailing whitespace",
            ),
            (
                {"apiKey": "<api-key>", "clusters": [cluster()]},
                "apiKey",
                "apiKey still holds a placeholder",
            ),
            (
                {"apiKey": "${REDIS_MCP_API_KEY}", "clusters": [cluster()]},
                "apiKey",
                "apiKey still holds a placeholder",
            ),
            (
                {"apiKey": "prefix-<key>", "clusters": [cluster()]},
                "apiKey",
                "apiKey still holds a placeholder",
            ),
            # toolsets
            (
                {"toolsets": ["core", ""], "clusters": [cluster()]},
                "toolsets",
                "the toolsets list contains an empty value",
            ),
            (
                {"toolsets": ["  "], "clusters": [cluster()]},
                "toolsets",
                "the toolsets list contains an empty value",
            ),
            (
                {"toolsets": ["core", "data", "core", "data"], "clusters": [cluster()]},
                "toolsets",
                "duplicate toolset: core, data",
            ),
            # clusters
            ({"clusters": []}, "clusters", "at least one cluster must be defined"),
            ({}, "clusters", "Field required"),
            ({"clusters": None}, "clusters", "Input should be a valid list"),
            # entraid
            (
                {"clusters": [cluster(entraid=True, username="u")]},
                "clusters[0]",
                "username cannot be combined with entraid",
            ),
            # types
            (
                {"readOnly": "maybe", "clusters": [cluster()]},
                "readOnly",
                "Input should be a valid boolean",
            ),
            (
                {"clusters": [cluster(), cluster(name="b", readOnly="maybe")]},
                "clusters[1].readOnly",
                "Input should be a valid boolean",
            ),
        ],
    )
    def test_error_names_the_field(self, data, path, message):
        error = config_error(data)
        assert error.startswith("Invalid config:\n - ")
        assert f" - {path}: " in error
        assert message in error
        assert "Value error, " not in error

    def test_all_errors_are_listed(self):
        error = config_error(
            {
                "port": "70000",
                "bogus": "1",
                "clusters": [cluster(), cluster(name="b", mode="bogus")],
            }
        )
        assert error.count("\n - ") == 3
        assert " - port: port must be between 0 and 65535" in error
        assert " - bogus: Extra inputs are not permitted" in error
        assert " - clusters[1].mode: " in error

    def test_error_without_a_path(self):
        error = config_error(["not", "a", "map"])
        assert error.startswith(
            "Invalid config:\n - Input should be a valid dictionary"
        )

    def test_errors_are_chained_to_the_validation_error(self):
        with pytest.raises(ConfigError) as exc:
            parse_config({"clusters": []})
        assert isinstance(exc.value.__cause__, ValidationError)


SECRET = "TOPSECRET" + "-input"


class TestErrorsHideTheInput:
    """A mistyped secret must not reach the log: neither the ConfigError text nor the chained pydantic
    ValidationError (shown in a traceback) carries the input values."""

    @pytest.mark.parametrize("model", [ServerConfig, ClusterConfig, TlsConfig])
    def test_models_hide_the_input(self, model):
        assert model.model_config["hide_input_in_errors"] is True

    @pytest.mark.parametrize(
        "data",
        [
            {"clusters": [cluster(hosts=f"user:{SECRET}@h:6379")]},
            {"clusters": [cluster(hosts=f"h:1,{SECRET}")]},
            {"clusters": [cluster(password=SECRET)]},
            {"clusters": [cluster(tls={"keyPassword": SECRET})]},
            {"clusters": [cluster(tls={"certReqs": SECRET})]},
            {"clusters": [cluster(options={"password": SECRET})]},
            {"clusters": [cluster(options={"client_name": SECRET + " x"})]},
            {"clusters": [cluster(options={"socket_timeout": SECRET})]},
            {"clusters": [cluster(name=SECRET + " x")]},
            {"clusters": [cluster(username=[SECRET])]},
            {"clusters": [cluster(db=SECRET)]},
            {"clusters": [cluster(mode=SECRET)]},
            {"clusters": [cluster(modules=[SECRET])]},
            {"clusters": [SECRET]},
            {"clusters": SECRET},
            {"apiKey": " " + SECRET, "clusters": [cluster()]},
            {"apiKey": [SECRET], "clusters": [cluster()]},
            {"apiKey": "<" + SECRET + ">", "clusters": [cluster()]},
            {"pasword": SECRET, "clusters": [cluster()]},
            {"port": SECRET, "clusters": [cluster()]},
        ],
    )
    def test_neither_message_nor_traceback_shows_the_input(self, data):
        with pytest.raises(ConfigError) as exc:
            parse_config(data)
        assert isinstance(exc.value.__cause__, ValidationError)
        assert SECRET not in str(exc.value)
        assert SECRET not in str(exc.value.__cause__)
        printed = "".join(traceback.format_exception(exc.value))
        assert SECRET not in printed
        assert "input_value" not in printed

    def test_from_yaml(self, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_text(
            f"apiKey: ' {SECRET}'\npasword: {SECRET}\n"
            f"clusters:\n  - name: a\n    hosts: h:6379\n    username: [{SECRET}]\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        assert str(exc.value).count("\n - ") == 3
        assert SECRET not in "".join(traceback.format_exception(exc.value))

    def test_from_legacy(self, redis_config):
        redis_config["host"] = f"user:{SECRET}@h"
        with pytest.raises(ConfigError) as exc:
            from_legacy(redis_config)
        assert SECRET not in "".join(traceback.format_exception(exc.value))


def mode_cluster(mode: str, **fields):
    """A valid cluster of the given mode."""
    if mode == "sentinel":
        fields = {"hosts": "s:26379", "masterName": "m", **fields}
    return cluster(mode=mode, **fields)


# The redis-py arguments each mode accepts in `options` (the contract of MODE_OPTIONS).
ALLOWED_OPTIONS = {
    "standalone": [
        "socket_timeout",
        "socket_connect_timeout",
        "socket_keepalive",
        "max_connections",
        "client_name",
        "protocol",
        "health_check_interval",
    ],
    "sentinel": [
        "socket_timeout",
        "socket_connect_timeout",
        "socket_keepalive",
        "max_connections",
        "client_name",
        "protocol",
        "health_check_interval",
    ],
    "cluster": [
        "socket_timeout",
        "socket_connect_timeout",
        "socket_keepalive",
        "max_connections",
        "client_name",
        "protocol",
        "read_from_replicas",
        "require_full_coverage",
        "reinitialize_steps",
        "dynamic_startup_nodes",
    ],
}

# A YAML-like (text) value per allowed option, and what the loader turns it into.
OPTION_SAMPLES = {
    "socket_timeout": ("5", 5),
    "socket_connect_timeout": ("2.5", 2.5),
    "socket_keepalive": (True, True),
    "max_connections": ("32", 32),
    "client_name": ("mcp", "mcp"),
    "protocol": ("3", 3),
    "health_check_interval": ("30", 30),
    "read_from_replicas": (True, True),
    "require_full_coverage": (False, False),
    "reinitialize_steps": ("5", 5),
    "dynamic_startup_nodes": (False, False),
}

# Keys `options` must never carry, in any mode: secrets, the client's address and identity, and settings the
# server manages itself. Also snake_case variants a user might write and an unknown key.
REJECTED_OPTIONS = [
    "password",
    "credential_provider",
    "decode_responses",
    "url",
    "host",
    "port",
    "db",
    "username",
    "ssl",
    "ssl_password",
    "ssl_ca_certs",
    "ssl_ca_path",
    "ssl_cert_reqs",
    "retry",
    "retry_on_timeout",
    "retry_on_error",
    "connection_pool",
    "connection_class",
    "startup_nodes",
    "sentinel_kwargs",
    "unix_socket_path",
    "encoding",
    "socketTimeout",
    "SOCKET_TIMEOUT",
    "bogus",
]

MODES = ["standalone", "sentinel", "cluster"]


def _params(fn) -> set:
    return set(inspect.signature(fn).parameters)


class TestOptions:
    """`options` is an allowlist of redis-py arguments per mode."""

    def test_mode_options(self):
        assert {mode: list(keys) for mode, keys in MODE_OPTIONS.items()} == (
            ALLOWED_OPTIONS
        )

    @pytest.mark.parametrize(
        "mode, key",
        [(mode, key) for mode in MODES for key in ALLOWED_OPTIONS[mode]],
    )
    def test_allowed_key_is_accepted(self, mode, key):
        value, expected = OPTION_SAMPLES[key]
        config = parse_config({"clusters": [mode_cluster(mode, options={key: value})]})
        assert config.clusters[0].options == {key: expected}

    @pytest.mark.parametrize("mode", MODES)
    def test_all_allowed_keys_together(self, mode):
        options = {key: OPTION_SAMPLES[key][0] for key in ALLOWED_OPTIONS[mode]}
        config = parse_config({"clusters": [mode_cluster(mode, options=options)]})
        assert config.clusters[0].options == {
            key: OPTION_SAMPLES[key][1] for key in ALLOWED_OPTIONS[mode]
        }

    @pytest.mark.parametrize("mode", MODES)
    def test_empty_options(self, mode):
        config = parse_config({"clusters": [mode_cluster(mode, options={})]})
        assert config.clusters[0].options == {}

    @pytest.mark.parametrize(
        "mode, key", [(mode, key) for mode in MODES for key in REJECTED_OPTIONS]
    )
    def test_rejected_in_every_mode(self, mode, key):
        error = config_error({"clusters": [mode_cluster(mode, options={key: "x"})]})
        assert (
            f" - clusters[0]: options {key} not supported in {mode} mode"
            f" (supported: {', '.join(ALLOWED_OPTIONS[mode])};"
        ) in error

    @pytest.mark.parametrize(
        "key",
        [
            "read_from_replicas",
            "require_full_coverage",
            "reinitialize_steps",
            "dynamic_startup_nodes",
        ],
    )
    @pytest.mark.parametrize("mode", ["standalone", "sentinel"])
    def test_cluster_only_keys_are_rejected_in_other_modes(self, mode, key):
        error = config_error({"clusters": [mode_cluster(mode, options={key: True})]})
        assert f"clusters[0]: options {key} not supported in {mode} mode" in error

    def test_health_check_interval_is_rejected_in_cluster_mode(self):
        # RedisCluster does not pass health_check_interval to its node connections
        error = config_error(
            {
                "clusters": [
                    mode_cluster("cluster", options={"health_check_interval": 5})
                ]
            }
        )
        assert (
            "clusters[0]: options health_check_interval not supported in cluster mode"
            in error
        )

    def test_exact_message(self):
        error = config_error({"clusters": [cluster(options={"password": "x"})]})
        assert error == (
            "Invalid config:\n - clusters[0]: options password not supported in standalone mode"
            " (supported: socket_timeout, socket_connect_timeout, socket_keepalive, max_connections,"
            " client_name, protocol, health_check_interval; addresses, db, username and tls have their"
            " own keys, passwords come from the passwords directory)"
        )

    def test_every_unsupported_key_is_named_in_sorted_order(self):
        error = config_error(
            {
                "clusters": [
                    cluster(
                        options={
                            "username": "u",
                            "socket_timeout": "1",
                            "password": "p",
                            "db": "1",
                        }
                    )
                ]
            }
        )
        assert "clusters[0]: options db, password, username not supported" in error
        assert error.count("not supported") == 1

    def test_each_cluster_is_checked(self):
        error = config_error(
            {
                "clusters": [
                    cluster(options={"socket_timeout": "1"}),
                    cluster(name="b", options={"password": "x"}),
                    mode_cluster("cluster", name="c", options={"db": "0"}),
                ]
            }
        )
        assert "clusters[0]" not in error
        assert " - clusters[1]: options password not supported in standalone" in error
        assert " - clusters[2]: options db not supported in cluster mode" in error

    def test_option_value_is_not_echoed(self):
        secret = "TOPSECRET" + "-option"
        options = {"password": secret, "ssl_password": secret, "url": secret}
        with pytest.raises(ConfigError) as exc:
            parse_config({"clusters": [cluster(options=options)]})
        assert secret not in str(exc.value)

    def test_unsupported_option_from_yaml(self, tmp_path):
        path = write(
            tmp_path / "c.yaml",
            """
            clusters:
              - name: a
                hosts: h:6379
                options:
                  socket_timeout: 5
                  retry_on_timeout: true
            """,
        )
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        assert (
            "clusters[0]: options retry_on_timeout not supported in standalone mode"
            in str(exc.value)
        )

    @pytest.mark.parametrize("key", ALLOWED_OPTIONS["standalone"])
    def test_standalone_keys_are_redis_arguments(self, key):
        assert key in _params(Redis.__init__)

    @pytest.mark.parametrize("key", ALLOWED_OPTIONS["sentinel"])
    def test_sentinel_keys_reach_the_master_connections(self, key):
        # Sentinel.master_for passes its kwargs to the connection pool and its (TCP) connections
        accepted = (
            _params(ConnectionPool.__init__)
            | _params(Connection.__init__)
            | _params(AbstractConnection.__init__)
        )
        assert key in accepted

    @pytest.mark.parametrize("key", ALLOWED_OPTIONS["cluster"])
    def test_cluster_keys_are_not_dropped_by_redis_cluster(self, key):
        # RedisCluster keeps only REDIS_ALLOWED_KEYS for its node connections, plus its own arguments
        assert key in set(REDIS_ALLOWED_KEYS) | _params(RedisCluster.__init__)


# What each option's error message says it must be.
OPTION_EXPECTED = {
    "socket_timeout": "a number of seconds > 0",
    "socket_connect_timeout": "a number of seconds > 0",
    "socket_keepalive": "true or false",
    "max_connections": "an integer >= 1",
    "client_name": "a name without spaces",
    "protocol": "2 or 3",
    "health_check_interval": "a number of seconds >= 0",
    "read_from_replicas": "true or false",
    "require_full_coverage": "true or false",
    "reinitialize_steps": "an integer >= 0",
    "dynamic_startup_nodes": "true or false",
}

_BOOL_GOOD = [(True, True), (False, False)]
# YAML 1.1 already reads yes/on/true as booleans; quoted text and numbers are not booleans
_BOOL_BAD = ["true", "yes", "1", "0", 1, 0, "", None, ["true"]]

# Accepted values per option (YAML text or Python values) and what the loader turns them into.
GOOD_OPTION_VALUES = {
    "socket_timeout": [
        ("5", 5),
        ("0.5", 0.5),
        ("1e-3", 0.001),
        (".1", 0.1),
        (30, 30),
        (2.5, 2.5),
    ],
    "socket_connect_timeout": [("2", 2), ("0.25", 0.25), (10, 10)],
    "socket_keepalive": _BOOL_GOOD,
    "max_connections": [("1", 1), ("32", 32), ("012", 12), (100, 100)],
    "client_name": [
        ("mcp", "mcp"),
        ("redis-mcp_1.eu", "redis-mcp_1.eu"),
        ("0123", "0123"),
        ("0x1F", "0x1F"),
        ("1.5", "1.5"),
        ("café", "café"),
    ],
    "protocol": [("2", 2), ("3", 3), (2, 2), (3, 3)],
    "health_check_interval": [("0", 0), ("30", 30), ("0.5", 0.5), (0, 0)],
    "read_from_replicas": _BOOL_GOOD,
    "require_full_coverage": _BOOL_GOOD,
    "reinitialize_steps": [("0", 0), ("5", 5), (10, 10)],
    "dynamic_startup_nodes": _BOOL_GOOD,
}

# Rejected values per option. None (YAML null or an empty value) would remove a timeout's time bound.
BAD_OPTION_VALUES = {
    "socket_timeout": [
        "0",
        "0.0",
        "-1",
        "-0.5",
        "abc",
        "5s",
        "",
        True,
        False,
        None,
        [5],
        {"s": "5"},
    ],
    "socket_connect_timeout": ["0", "-2", "2 s", True, None, float("nan")],
    "socket_keepalive": _BOOL_BAD,
    "max_connections": ["0", "-1", "1.5", "2.0", "many", True, None, 2.0],
    "client_name": [
        "",
        "has space",
        " mcp",
        "mcp ",
        "tab\there",
        "new\nline",
        "nbsp here",
        7,
        1.5,
        True,
        None,
        ["mcp"],
    ],
    "protocol": ["1", "4", "0", "-2", "three", True, False, None, 2.5],
    "health_check_interval": ["-1", "-0.5", "30s", True, None],
    "read_from_replicas": _BOOL_BAD,
    "require_full_coverage": _BOOL_BAD,
    "reinitialize_steps": ["-1", "1.5", "five", True, None],
    "dynamic_startup_nodes": _BOOL_BAD,
}


def mode_for(key: str) -> str:
    """A mode in which the option is allowed."""
    return "standalone" if key in ALLOWED_OPTIONS["standalone"] else "cluster"


def option_error(key: str, value, mode=None) -> str:
    return config_error(
        {"clusters": [mode_cluster(mode or mode_for(key), options={key: value})]}
    )


class TestOptionValues:
    """Each allowed option's value is checked, so a typo fails at startup instead of on the first call."""

    def test_every_allowed_key_has_a_value_check(self):
        allowed = {key for keys in MODE_OPTIONS.values() for key in keys}
        assert set(config_loader.OPTION_VALUES) == allowed
        assert set(OPTION_EXPECTED) == allowed
        assert set(GOOD_OPTION_VALUES) == allowed
        assert set(BAD_OPTION_VALUES) == allowed

    @pytest.mark.parametrize(
        "key, value, expected",
        [
            (key, value, expected)
            for key, pairs in GOOD_OPTION_VALUES.items()
            for value, expected in pairs
        ],
    )
    def test_good_value(self, key, value, expected):
        config = parse_config(
            {"clusters": [mode_cluster(mode_for(key), options={key: value})]}
        )
        options = config.clusters[0].options
        assert options == {key: expected}
        assert type(options[key]) is type(expected)

    @pytest.mark.parametrize(
        "key, value",
        [(key, value) for key, values in BAD_OPTION_VALUES.items() for value in values],
    )
    def test_bad_value(self, key, value):
        assert option_error(key, value) == (
            f"Invalid config:\n - clusters[0]: options.{key} must be {OPTION_EXPECTED[key]}"
        )

    @pytest.mark.parametrize(
        "mode, key", [(mode, key) for mode in MODES for key in ALLOWED_OPTIONS[mode]]
    )
    def test_values_are_checked_in_every_mode(self, mode, key):
        error = option_error(key, BAD_OPTION_VALUES[key][0], mode)
        assert f" - clusters[0]: options.{key} must be {OPTION_EXPECTED[key]}" in error

    def test_bad_value_is_not_echoed(self):
        error = option_error("client_name", "TOPSECRET with space")
        assert "TOPSECRET" not in error

    def test_unsupported_key_is_reported_before_the_values(self):
        # an unsupported key has no value check; it must not end in a KeyError
        error = config_error(
            {"clusters": [cluster(options={"socket_timeout": "0", "password": "x"})]}
        )
        assert " - clusters[0]: options password not supported in standalone" in error
        assert "must be" not in error

    def test_each_cluster_reports_its_own_value(self):
        error = config_error(
            {
                "clusters": [
                    cluster(options={"socket_timeout": "0"}),
                    cluster(name="b", options={"socket_timeout": "1"}),
                    mode_cluster(
                        "cluster", name="c", options={"reinitialize_steps": "-1"}
                    ),
                ]
            }
        )
        assert error == (
            "Invalid config:"
            "\n - clusters[0]: options.socket_timeout must be a number of seconds > 0"
            "\n - clusters[2]: options.reinitialize_steps must be an integer >= 0"
        )

    @pytest.mark.parametrize(
        "line, key, expected",
        [
            ("socket_timeout: 0", "socket_timeout", "a number of seconds > 0"),
            ("socket_timeout:", "socket_timeout", "a number of seconds > 0"),
            ("socket_timeout: null", "socket_timeout", "a number of seconds > 0"),
            ("socket_connect_timeout: ~", "socket_connect_timeout", "a number"),
            ("socket_keepalive: 'true'", "socket_keepalive", "true or false"),
            ("max_connections: 1.5", "max_connections", "an integer >= 1"),
            ("client_name: my mcp", "client_name", "a name without spaces"),
            ("client_name:", "client_name", "a name without spaces"),
            ("protocol: 4", "protocol", "2 or 3"),
            ("health_check_interval: -1", "health_check_interval", "a number"),
        ],
    )
    def test_bad_value_from_yaml(self, tmp_path, line, key, expected):
        path = write(
            tmp_path / "c.yaml",
            "clusters:\n  - name: a\n    hosts: h:6379\n    options:\n      "
            + line
            + "\n",
        )
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        assert f" - clusters[0]: options.{key} must be {expected}" in str(exc.value)

    @pytest.mark.parametrize("word", ["yes", "on", "True", "FALSE", "off"])
    def test_yaml_booleans_are_accepted(self, tmp_path, word):
        path = write(
            tmp_path / "c.yaml",
            "clusters:\n  - name: a\n    hosts: h:6379\n    options:\n"
            "      socket_keepalive: " + word + "\n",
        )
        value = load_config(path).clusters[0].options["socket_keepalive"]
        assert value is (word.lower() in ("yes", "on", "true"))

    def test_value_is_checked_after_the_merge(self, tmp_path):
        main = write(tmp_path / "main.yaml", MAIN_CONFIG)
        conf_d = tmp_path / "conf.d"
        conf_d.mkdir()
        write(
            conf_d / "10-a.yaml",
            "clusters: [{name: a, options: {socket_timeout: 0}}]\n",
        )
        with pytest.raises(ConfigError) as exc:
            load_config(main, conf_d)
        assert str(exc.value) == (
            "Invalid config:\n - clusters[0]: options.socket_timeout must be a number of seconds > 0"
        )

    @pytest.mark.xfail(
        strict=True,
        reason="bug: the timeouts are only checked for > 0, so a value beyond what a socket timeout can hold "
        "(1e10 s, or 1e400 which becomes inf) is accepted, and then every connection fails with "
        "OverflowError('timestamp out of range for C PyTime_t') instead of a config error at startup",
    )
    @pytest.mark.parametrize("key", ["socket_timeout", "socket_connect_timeout"])
    @pytest.mark.parametrize("value", ["1e10", "1e400"])
    def test_timeout_too_large_for_a_socket_is_rejected(self, key, value):
        error = option_error(key, value)
        assert f" - clusters[0]: options.{key} must be " in error


class TestTlsCaPath:
    """RedisCluster drops ssl_ca_path, so caPath is rejected in cluster mode."""

    @pytest.mark.parametrize(
        "tls",
        [
            {"caPath": "/ca"},
            {"caPath": "/ca", "caFile": "/ca.crt"},
            {"caPath": "/ca", "certReqs": "none"},
        ],
    )
    def test_rejected_in_cluster_mode(self, tls):
        error = config_error({"clusters": [mode_cluster("cluster", tls=tls)]})
        assert (
            " - clusters[0]: tls.caPath is not supported in cluster mode; use caFile"
            in error
        )

    def test_rejected_in_cluster_mode_from_yaml(self, tmp_path):
        path = write(
            tmp_path / "c.yaml",
            """
            clusters:
              - name: a
                hosts: h:6379
              - name: b
                mode: cluster
                hosts: [n1:7000, n2:7001]
                tls:
                  caPath: /etc/redis-mcp/tls
            """,
        )
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        assert "clusters[1]: tls.caPath is not supported in cluster mode" in str(
            exc.value
        )

    @pytest.mark.parametrize("mode", ["standalone", "sentinel"])
    def test_accepted_in_other_modes(self, mode):
        c = parse_config(
            {"clusters": [mode_cluster(mode, tls={"caPath": "/ca"})]}
        ).clusters[0]
        assert c.tls.client_kwargs()["ssl_ca_path"] == "/ca"

    def test_ca_file_is_accepted_in_cluster_mode(self):
        c = parse_config(
            {"clusters": [mode_cluster("cluster", tls={"caFile": "/ca.crt"})]}
        ).clusters[0]
        assert c.tls.client_kwargs()["ssl_ca_certs"] == "/ca.crt"

    def test_tls_true_is_accepted_in_cluster_mode(self):
        c = parse_config({"clusters": [mode_cluster("cluster", tls=True)]}).clusters[0]
        assert c.tls == TlsConfig()


class TestHostsWithCredentials:
    """hosts takes plain host:port entries; an entry with credentials or a scheme is rejected without being
    echoed, since it may hold a password."""

    SECRET = "TOPSECRET" + "pw"

    @pytest.mark.parametrize(
        "hosts",
        [
            "user:{s}@h:6379",
            ":{s}@h:6379",
            "{s}@h:6379",
            "user:{s}@[::1]:6379",
            "redis://h:6379",
            "rediss://user:{s}@h:6379",
            "redis://:{s}@h:6379/0",
            "unix:///tmp/{s}.sock",
            "h:1,user:{s}@h:2",
            "h:1, redis://user:{s}@h:2",
        ],
    )
    def test_parse_hosts_rejects_without_echo(self, hosts):
        hosts = hosts.format(s=self.SECRET)
        with pytest.raises(ValueError) as exc:
            parse_hosts(hosts)
        message = str(exc.value)
        assert message == (
            "hosts takes host:port entries without a scheme or credentials (user:password@);"
            " passwords come from the passwords directory"
        )
        assert self.SECRET not in message

    @pytest.mark.parametrize("mode", MODES)
    def test_config_error_does_not_echo_the_password(self, mode):
        hosts = f"user:{self.SECRET}@h:6379"
        error = config_error({"clusters": [mode_cluster(mode, hosts=hosts)]})
        assert " - clusters[0].hosts: hosts takes host:port entries" in error
        assert self.SECRET not in error

    def test_hosts_list_entry(self):
        hosts = ["n1:7000", f"user:{self.SECRET}@n2:7001"]
        error = config_error({"clusters": [mode_cluster("cluster", hosts=hosts)]})
        assert " - clusters[0].hosts: hosts takes host:port entries" in error
        assert self.SECRET not in error

    def test_from_yaml(self, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_text(
            "clusters:\n  - name: a\n    hosts: redis://default:"
            + self.SECRET
            + "@h:6379\n",
            encoding="utf-8",
        )
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        assert "clusters[0].hosts: hosts takes host:port entries" in str(exc.value)
        assert self.SECRET not in str(exc.value)

    @pytest.mark.parametrize(
        "hosts",
        [
            "user:TOP,SECRET@h:6379",
            "h:1,user:TOP,SECRET@h:2",
            "TOP,SECRET@h:6379",
            "redis://user:TOP,SECRET@h:6379",
            "h:1,rediss://TOP,SECRET:6379",
            ["n1:7000", "user:TOP,SECRET@n2:7001"],
            ["n1:7000", "user:TOP", "SECRET@n2:7001"],
        ],
    )
    def test_password_with_a_comma_is_not_echoed(self, hosts):
        # the whole value is checked for credentials before it is split into entries
        error = config_error({"clusters": [mode_cluster("cluster", hosts=hosts)]})
        assert error == (
            "Invalid config:\n - clusters[0].hosts: hosts takes host:port entries without a scheme or"
            " credentials (user:password@); passwords come from the passwords directory"
        )
        assert "TOP" not in error
        assert "SECRET" not in error

    def test_traceback_does_not_show_the_password(self):
        hosts = f"user:{self.SECRET}@h:6379"
        data = {"clusters": [cluster(hosts=hosts)]}
        with pytest.raises(ConfigError) as exc:
            parse_config(data)
        assert self.SECRET not in "".join(traceback.format_exception(exc.value))


class TestSentinelPasswordClash:
    """<name>_sentinel_pass is both the Sentinel password of a sentinel cluster <name> and the password of a
    cluster named <name>_sentinel."""

    MESSAGE = (
        " - clusters: cluster name s_sentinel clashes with the Sentinel password of cluster s"
        " (s_sentinel_pass); rename one of them"
    )

    @pytest.mark.parametrize("other_mode", MODES)
    def test_clash_with_any_mode(self, other_mode):
        error = config_error(
            {
                "clusters": [
                    mode_cluster("sentinel", name="s"),
                    mode_cluster(other_mode, name="s_sentinel"),
                ]
            }
        )
        assert self.MESSAGE in error

    def test_clash_in_either_order(self):
        error = config_error(
            {
                "clusters": [
                    cluster(name="s_sentinel"),
                    cluster(name="other", hosts="h:2"),
                    mode_cluster("sentinel", name="s"),
                ]
            }
        )
        assert self.MESSAGE in error

    @pytest.mark.parametrize("mode", ["standalone", "cluster"])
    def test_no_clash_without_sentinel_mode(self, mode):
        # only a sentinel cluster reads <name>_sentinel_pass
        config = parse_config(
            {"clusters": [mode_cluster(mode, name="s"), cluster(name="s_sentinel")]}
        )
        assert config.cluster_names() == ["s", "s_sentinel"]

    @pytest.mark.parametrize(
        "other", ["s_sentinel2", "s-sentinel", "s_sentinel_pass", "xs_sentinel"]
    )
    def test_similar_names_do_not_clash(self, other):
        config = parse_config(
            {"clusters": [mode_cluster("sentinel", name="s"), cluster(name=other)]}
        )
        assert config.cluster_names() == ["s", other]

    def test_clash_from_a_drop_in(self, tmp_path):
        main = write(
            tmp_path / "main.yaml",
            """
            clusters:
              - name: s
                mode: sentinel
                hosts: s1:26379
                masterName: mymaster
            """,
        )
        conf_d = tmp_path / "conf.d"
        conf_d.mkdir()
        write(conf_d / "10-new.yaml", "clusters: [{name: s_sentinel, hosts: 'h:1'}]\n")
        with pytest.raises(ConfigError) as exc:
            load_config(main, conf_d)
        assert self.MESSAGE in str(exc.value)


class TestApiKeyError:
    @pytest.mark.parametrize(
        "api_key", [None, "", "0123456789abcdef0123", "a", "key-with_symbols.!@#"]
    )
    def test_acceptable(self, api_key):
        assert api_key_error(api_key) is None

    @pytest.mark.parametrize(
        "api_key, fragment",
        [
            (" ", "blank"),
            ("abc ", "leading or trailing whitespace"),
            ("<changeme>", "placeholder"),
            ("abc>", "placeholder"),
            ("${KEY}", "placeholder"),
        ],
    )
    def test_rejected(self, api_key, fragment):
        assert fragment in api_key_error(api_key)


class TestParseHosts:
    def test_single(self):
        assert parse_hosts("localhost:6379") == [("localhost", 6379)]

    def test_many_with_spaces(self):
        assert parse_hosts(" h1:1 , h2:65535 ") == [("h1", 1), ("h2", 65535)]

    def test_ipv6_brackets_are_removed(self):
        assert parse_hosts("[::1]:6379,[fe80::1]:6380") == [
            ("::1", 6379),
            ("fe80::1", 6380),
        ]

    def test_surrounding_whitespace_is_ignored(self):
        assert parse_hosts("\th1:1 ,\n h2:2\r\n") == [("h1", 1), ("h2", 2)]

    @pytest.mark.parametrize(
        "hosts, number",
        [
            ("h", 1),
            ("h:", 1),
            (":1", 1),
            ("h:x", 1),
            ("h:+1", 1),
            ("h:-1", 1),
            ("h: 1", 1),
            ("", 1),
            ("  ", 1),
            (",", 1),
            ("[::1]", 1),
            ("h:1,,h:2", 2),
            ("h:1,", 2),
            ("h:1, h:2 ,x:y", 3),
            ("a:1,b:2,c:3,d", 4),
        ],
    )
    def test_invalid_entry_is_numbered(self, hosts, number):
        with pytest.raises(ValueError) as exc:
            parse_hosts(hosts)
        assert str(exc.value) == (
            f"hosts entry {number} is not host:port (or [IPv6 address]:port)"
        )

    @pytest.mark.parametrize(
        "hosts, number",
        [
            ("my host:6379", 1),
            ("h :6379", 1),
            ("h:1,h\t2:2", 2),
            ("h:1,h\n2:2", 2),
            ("h:1,h\u00a02:2", 2),
            ("[::1 ]:6379", 1),
        ],
    )
    def test_whitespace_inside_a_host_is_an_error(self, hosts, number):
        with pytest.raises(ValueError) as exc:
            parse_hosts(hosts)
        assert str(exc.value) == (
            f"hosts entry {number} is not host:port (or [IPv6 address]:port)"
        )

    @pytest.mark.parametrize(
        "hosts, number",
        [
            ("h:0", 1),
            ("h:65536", 1),
            ("h:99999999", 1),
            ("h:1,h:00000", 2),
            ("a:1,b:2,[::1]:70000", 3),
        ],
    )
    def test_port_out_of_range(self, hosts, number):
        with pytest.raises(ValueError) as exc:
            parse_hosts(hosts)
        assert str(exc.value) == (
            f"hosts entry {number}: the port must be between 1 and 65535"
        )

    @pytest.mark.parametrize(
        "hosts",
        [
            "TOPSECRET",
            "h:1,TOPSECRET",
            "TOPSECRET:abc",
            "TOPSECRET value:1",
            "h:1, TOPSECRET:0",
            "[TOPSECRET]:65536",
            "TOPSECRET:",
        ],
    )
    def test_entry_is_not_echoed(self, hosts):
        # a password pasted into hosts must not reach the log
        with pytest.raises(ValueError) as exc:
            parse_hosts(hosts)
        assert "TOPSECRET" not in str(exc.value)
        error = config_error({"clusters": [mode_cluster("cluster", hosts=hosts)]})
        assert " - clusters[0].hosts: hosts entry " in error
        assert "TOPSECRET" not in error

    @pytest.mark.parametrize("hosts", ["h@x:1", "h:1,@", "h:1,x://y", "://"])
    def test_any_at_sign_or_scheme_is_a_credentials_error(self, hosts):
        with pytest.raises(ValueError, match="without a scheme or credentials"):
            parse_hosts(hosts)

    @pytest.mark.xfail(
        strict=True,
        reason="bug: parse_hosts checks the port with str.isdigit(), which is also true for digits int() "
        "cannot parse (e.g. superscript '²'); int() then raises its own ValueError \"invalid literal for "
        "int() with base 10: '²'\", which echoes the entry and has no entry number",
    )
    def test_port_with_a_digit_int_cannot_parse(self):
        with pytest.raises(ValueError) as exc:
            parse_hosts("h:1,h:\u00b2")
        assert str(exc.value) == (
            "hosts entry 2 is not host:port (or [IPv6 address]:port)"
        )


class TestTlsConfig:
    def test_defaults(self):
        assert TlsConfig().client_kwargs() == {
            "ssl": True,
            "ssl_cert_reqs": "required",
        }

    def test_all_fields(self):
        tls = TlsConfig.model_validate(
            {
                "caFile": "/ca.crt",
                "caPath": "/ca",
                "certFile": "/tls.crt",
                "keyFile": "/tls.key",
                "certReqs": "none",
                "checkHostname": True,
            }
        )
        assert tls.client_kwargs() == {
            "ssl": True,
            "ssl_cert_reqs": "none",
            "ssl_ca_certs": "/ca.crt",
            "ssl_ca_path": "/ca",
            "ssl_certfile": "/tls.crt",
            "ssl_keyfile": "/tls.key",
            "ssl_check_hostname": True,
        }

    def test_check_hostname_false_is_passed(self):
        kwargs = TlsConfig.model_validate({"checkHostname": False}).client_kwargs()
        assert kwargs["ssl_check_hostname"] is False

    def test_unset_fields_are_left_out(self):
        kwargs = TlsConfig.model_validate({"caFile": "/ca.crt"}).client_kwargs()
        assert kwargs == {
            "ssl": True,
            "ssl_cert_reqs": "required",
            "ssl_ca_certs": "/ca.crt",
        }


class TestIsReadOnly:
    @pytest.mark.parametrize(
        "global_read_only, cluster_read_only, expected",
        [
            (False, None, False),
            (True, None, True),
            (True, False, False),
            (False, True, True),
            (True, True, True),
            (False, False, False),
        ],
    )
    def test_cluster_value_overrides_global(
        self, global_read_only, cluster_read_only, expected
    ):
        fields = {} if cluster_read_only is None else {"readOnly": cluster_read_only}
        config = parse_config(
            {"readOnly": global_read_only, "clusters": [cluster(**fields)]}
        )
        assert config.is_read_only(config.clusters[0]) is expected

    def test_each_cluster_separately(self):
        config = parse_config(
            {
                "readOnly": True,
                "clusters": [
                    cluster(name="ro"),
                    cluster(name="rw", readOnly=False),
                ],
            }
        )
        assert config.is_read_only(config.cluster("ro")) is True
        assert config.is_read_only(config.cluster("rw")) is False


class TestMerge:
    def test_maps_are_deep_merged(self):
        target = {"a": {"b": 1, "c": {"d": 2, "e": 3}}, "keep": "x"}
        merge(target, {"a": {"c": {"d": 20, "f": 4}}, "new": "y"})
        assert target == {
            "a": {"b": 1, "c": {"d": 20, "e": 3, "f": 4}},
            "keep": "x",
            "new": "y",
        }

    def test_lists_and_scalars_are_replaced(self):
        target = {"toolsets": ["core", "data"], "port": "1", "m": {"x": "1"}}
        merge(target, {"toolsets": ["json"], "port": "2", "m": "scalar"})
        assert target == {"toolsets": ["json"], "port": "2", "m": "scalar"}

    def test_source_is_not_modified(self):
        base_options = {"socket_timeout": "5"}
        target = {"clusters": [{"name": "a", "options": base_options}]}
        source = {"clusters": [{"name": "a", "options": {"client_name": "x"}}]}
        merge(target, source)
        assert base_options == {"socket_timeout": "5"}
        assert source == {"clusters": [{"name": "a", "options": {"client_name": "x"}}]}

    def test_clusters_by_name(self):
        target = {
            "clusters": [
                {"name": "a", "hosts": "h:1", "options": {"x": "1", "y": "2"}},
                {"name": "b", "hosts": "h:2"},
            ]
        }
        merge(
            target,
            {
                "clusters": [
                    {"name": "b", "db": "4"},
                    {"name": "c", "hosts": "h:3"},
                    {"name": "a", "options": {"y": "20"}, "modules": ["json"]},
                ]
            },
        )
        assert target["clusters"] == [
            {
                "name": "a",
                "hosts": "h:1",
                "options": {"x": "1", "y": "20"},
                "modules": ["json"],
            },
            {"name": "b", "hosts": "h:2", "db": "4"},
            {"name": "c", "hosts": "h:3"},
        ]

    def test_cluster_entries_without_name_are_appended(self):
        target = {"clusters": [{"name": "a", "hosts": "h:1"}]}
        merge(target, {"clusters": [{"hosts": "h:2"}, "text"]})
        assert target["clusters"] == [
            {"name": "a", "hosts": "h:1"},
            {"hosts": "h:2"},
            "text",
        ]

    def test_clusters_replaced_when_not_both_lists(self):
        target = {"clusters": {"not": "a list"}}
        merge(target, {"clusters": [{"name": "a"}]})
        assert target == {"clusters": [{"name": "a"}]}


MAIN_CONFIG = """
port: 8080
readOnly: true
toolsets: [core, data]
clusters:
  - name: a
    hosts: h1:6379
    db: 1
    modules: [json, search]
    tls:
      caFile: /ca.crt
    options:
      socket_timeout: 5
      socket_connect_timeout: 3
  - name: b
    mode: cluster
    hosts: [n1:7000, n2:7001]
"""


class TestDropIns:
    @pytest.fixture
    def main_file(self, tmp_path) -> Path:
        return write(tmp_path / "redis-mcp.yaml", MAIN_CONFIG)

    @pytest.fixture
    def conf_d(self, tmp_path) -> Path:
        path = tmp_path / "conf.d"
        path.mkdir()
        return path

    def test_files_are_sorted_by_name_and_filtered(self, conf_d):
        # created in reverse order, so the result cannot follow creation order
        write(conf_d / "30-c.yml", "port: 3\n")
        write(conf_d / "20-b.yaml", "port: 2\n")
        write(conf_d / "10-a.yaml", "port: 1\n")
        write(conf_d / ".hidden.yaml", "bogus: 1\n")
        write(conf_d / "notes.txt", "bogus: 1\n")
        write(conf_d / "README.md", "bogus: 1\n")
        write(conf_d / "10-a.yaml.bak", "bogus: 1\n")
        write(conf_d / "upper.YAML", "bogus: 1\n")
        (conf_d / "sub.yaml").mkdir()
        write(conf_d / "sub.yaml" / "inner.yaml", "bogus: 1\n")
        assert [p.name for p in drop_ins(conf_d)] == [
            "10-a.yaml",
            "20-b.yaml",
            "30-c.yml",
        ]

    def test_later_files_win(self, main_file, conf_d):
        write(conf_d / "20-b.yaml", "port: 2\n")
        write(conf_d / "10-a.yaml", "port: 1\nreadOnly: false\n")
        config = load_config(main_file, conf_d)
        assert config.port == 2
        assert config.read_only is False

    def test_skipped_files_are_not_read(self, main_file, conf_d):
        write(conf_d / ".hidden.yaml", "bogus: 1\n")
        write(conf_d / "notes.txt", "bogus: 1\n")
        write(conf_d / "old.yaml.bak", "bogus: 1\n")
        config = load_config(main_file, conf_d)
        assert config.port == 8080

    def test_cluster_fields_are_merged_by_name(self, main_file, conf_d):
        write(
            conf_d / "10-a.yaml",
            """
            clusters:
              - name: a
                db: 2
                username: mcp
                tls:
                  certReqs: none
                options:
                  socket_timeout: 10
                  client_name: mcp
            """,
        )
        config = load_config(main_file, conf_d)
        a = config.cluster("a")
        assert a.hosts == "h1:6379"
        assert a.db == 2
        assert a.username == "mcp"
        assert a.modules == ["json", "search"]
        assert a.tls == TlsConfig.model_validate(
            {"caFile": "/ca.crt", "certReqs": "none"}
        )
        assert a.options == {
            "socket_timeout": 10,
            "socket_connect_timeout": 3,
            "client_name": "mcp",
        }
        assert config.cluster_names() == ["a", "b"]

    def test_new_cluster_is_appended(self, main_file, conf_d):
        write(
            conf_d / "10-new.yaml",
            """
            clusters:
              - name: c
                mode: sentinel
                hosts: s1:26379,s2:26379
                masterName: mymaster
            """,
        )
        config = load_config(main_file, conf_d)
        assert config.cluster_names() == ["a", "b", "c"]
        assert config.cluster("c").master_name == "mymaster"
        assert config.cluster("b").addresses() == [("n1", 7000), ("n2", 7001)]

    def test_lists_are_replaced(self, main_file, conf_d):
        write(
            conf_d / "10-lists.yaml",
            """
            toolsets: [json]
            clusters:
              - name: a
                modules: [search]
              - name: b
                hosts: [n9:7009]
            """,
        )
        config = load_config(main_file, conf_d)
        assert config.toolsets == ["json"]
        assert config.cluster("a").modules == ["search"]
        assert config.cluster("b").hosts == "n9:7009"

    def test_drop_ins_merge_over_each_other(self, main_file, conf_d):
        write(
            conf_d / "10-a.yaml",
            "clusters: [{name: a, options: {client_name: mcp, socket_timeout: 7}}]\n",
        )
        write(
            conf_d / "20-a.yaml",
            "clusters: [{name: a, options: {health_check_interval: 2, socket_timeout: 8}}]\n",
        )
        config = load_config(main_file, conf_d)
        assert config.cluster("a").options == {
            "socket_timeout": 8,
            "socket_connect_timeout": 3,
            "client_name": "mcp",
            "health_check_interval": 2,
        }

    def test_options_are_checked_after_the_merge(self, main_file, conf_d):
        # b is a Redis Cluster, where health_check_interval would be dropped by redis-py
        write(
            conf_d / "10-b.yaml",
            "clusters: [{name: b, options: {health_check_interval: 2}}]\n",
        )
        with pytest.raises(ConfigError) as exc:
            load_config(main_file, conf_d)
        assert (
            "clusters[1]: options health_check_interval not supported in cluster mode"
            in str(exc.value)
        )

    def test_cluster_options_from_drop_in(self, main_file, conf_d):
        write(
            conf_d / "10-b.yaml",
            "clusters: [{name: b, options: {read_from_replicas: true, reinitialize_steps: 3}}]\n",
        )
        config = load_config(main_file, conf_d)
        assert config.cluster("b").options == {
            "read_from_replicas": True,
            "reinitialize_steps": 3,
        }

    def test_empty_drop_in_changes_nothing(self, main_file, conf_d):
        write(conf_d / "10-empty.yaml", "# only a comment\n")
        assert load_config(main_file, conf_d) == load_config(main_file)

    def test_config_dir_alone(self, conf_d):
        write(conf_d / "00-base.yaml", MAIN_CONFIG)
        write(conf_d / "10-port.yaml", "port: 9090\n")
        config = load_config(None, conf_d)
        assert config.port == 9090
        assert config.cluster_names() == ["a", "b"]

    def test_empty_config_dir_alone_has_no_clusters(self, conf_d):
        with pytest.raises(ConfigError, match=r"clusters: Field required"):
            load_config(None, conf_d)

    def test_missing_config_dir(self, main_file, tmp_path):
        missing = tmp_path / "missing"
        with pytest.raises(ConfigError) as exc:
            load_config(main_file, missing)
        assert str(exc.value) == f"--config-dir is not a directory: {missing}"

    def test_config_dir_is_a_file(self, main_file):
        with pytest.raises(ConfigError, match="--config-dir is not a directory"):
            load_config(None, main_file)

    def test_errors_use_merged_indexes(self, main_file, conf_d):
        write(conf_d / "10-b.yaml", "clusters: [{name: b, mode: sentinel}]\n")
        with pytest.raises(ConfigError) as exc:
            load_config(main_file, conf_d)
        assert "clusters[1]: masterName is required when mode is sentinel" in str(
            exc.value
        )

    def test_incomplete_new_cluster(self, main_file, conf_d):
        write(conf_d / "10-typo.yaml", "clusters: [{name: aa, db: 2}]\n")
        with pytest.raises(ConfigError) as exc:
            load_config(main_file, conf_d)
        assert "clusters[2].hosts: Field required" in str(exc.value)

    def test_unknown_key_in_drop_in(self, main_file, conf_d):
        write(conf_d / "10-a.yaml", "clusters: [{name: a, hostz: 'h:1'}]\n")
        with pytest.raises(ConfigError, match=r"clusters\[0\]\.hostz: Extra inputs"):
            load_config(main_file, conf_d)

    def test_duplicate_toolset_from_drop_in(self, main_file, conf_d):
        write(conf_d / "10-t.yaml", "toolsets: [core, core]\n")
        with pytest.raises(ConfigError, match="toolsets: duplicate toolset: core"):
            load_config(main_file, conf_d)


class TestFileErrors:
    def test_missing_file(self, tmp_path):
        missing = tmp_path / "missing.yaml"
        with pytest.raises(ConfigError) as exc:
            load_config(missing)
        assert str(exc.value) == (
            f"Could not read config file: {missing} (No such file or directory)"
        )

    def test_directory_as_file(self, tmp_path):
        with pytest.raises(ConfigError, match="Could not read config file"):
            load_config(tmp_path)

    def test_os_error(self, tmp_path):
        path = write(tmp_path / "c.yaml", MAIN_CONFIG)
        with patch.object(
            Path, "read_bytes", side_effect=PermissionError(13, "Permission denied")
        ):
            with pytest.raises(ConfigError) as exc:
                load_config(path)
        assert str(exc.value) == (
            f"Could not read config file: {path} (Permission denied)"
        )
        assert isinstance(exc.value.__cause__, PermissionError)

    @pytest.mark.parametrize(
        "text",
        [
            "clusters: [{name: a\n",
            "a: b: c\n",
            "a: 1\n---\nb: 2\n",
            "a:\n\t- tab\n",
            "a: *unknown\n",
        ],
    )
    def test_invalid_yaml(self, tmp_path, text):
        path = write(tmp_path / "c.yaml", text)
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        assert str(exc.value).startswith(f"Could not parse config file: {path} (")
        assert isinstance(exc.value.__cause__, yaml.YAMLError)

    @pytest.mark.parametrize("text", ["- a\n- b\n", "just text\n", "42\n", "true\n"])
    def test_not_a_map(self, tmp_path, text):
        path = write(tmp_path / "c.yaml", text)
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        assert str(exc.value) == f"Config file must be a YAML map: {path}"

    def test_empty_file_is_an_empty_map(self, tmp_path):
        path = write(tmp_path / "c.yaml", "")
        with pytest.raises(ConfigError, match=r"clusters: Field required"):
            load_config(path)

    def test_invalid_drop_in_names_the_file(self, tmp_path):
        main = write(tmp_path / "main.yaml", MAIN_CONFIG)
        conf_d = tmp_path / "conf.d"
        conf_d.mkdir()
        bad = write(conf_d / "10-bad.yaml", "port: [\n")
        with pytest.raises(ConfigError) as exc:
            load_config(main, conf_d)
        assert f"Could not parse config file: {bad}" in str(exc.value)

    def test_non_map_drop_in(self, tmp_path):
        main = write(tmp_path / "main.yaml", MAIN_CONFIG)
        conf_d = tmp_path / "conf.d"
        conf_d.mkdir()
        bad = write(conf_d / "10-list.yaml", "- port: 1\n")
        with pytest.raises(ConfigError) as exc:
            load_config(main, conf_d)
        assert str(exc.value) == f"Config file must be a YAML map: {bad}"

    def test_invalid_utf8(self, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_bytes(b"apiKey: \xff\xfe\n")
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        assert str(exc.value) == (
            f"Could not read config file: {path} (not UTF-8: invalid byte on line 1)"
        )
        assert isinstance(exc.value.__cause__, UnicodeDecodeError)

    def test_latin1_config_file(self, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_bytes(
            "clusters: [{name: a, hosts: 'h:1', username: café}]\n".encode("latin-1")
        )
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        assert str(exc.value) == (
            f"Could not read config file: {path} (not UTF-8: invalid byte on line 1)"
        )

    @pytest.mark.parametrize(
        "raw, line",
        [
            (b"\xff", 1),
            (b"a: \xe2\x82", 1),
            (b"a: \xe2\nb: 1\n", 1),
            (b"a: 1\n\xff: 2\n", 2),
            (b"a: 1\r\nb: \xc3\x28\r\n", 2),
            (b"a: 1\n\n\n# caf\xe9\n", 4),
            (b"a: 1\nb: 2\n#\n#\n#\n#\nc: \x80\n", 7),
            ("a: café\nb: ok\n".encode() + b"c: \xc0\xaf\n", 3),
        ],
    )
    def test_invalid_utf8_names_the_line(self, tmp_path, raw, line):
        path = tmp_path / "c.yaml"
        path.write_bytes(raw)
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        assert str(exc.value) == (
            f"Could not read config file: {path} (not UTF-8: invalid byte on line {line})"
        )

    def test_invalid_utf8_does_not_echo_the_content(self, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_bytes(b"apiKey: TOPSECRET\xff\n")
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        assert "TOPSECRET" not in str(exc.value)

    def test_invalid_utf8_drop_in_names_the_file(self, tmp_path):
        main = write(tmp_path / "main.yaml", MAIN_CONFIG)
        conf_d = tmp_path / "conf.d"
        conf_d.mkdir()
        bad = conf_d / "10-bad.yaml"
        bad.write_bytes(b"port: 1\n# \xc3\x28\n")
        with pytest.raises(ConfigError) as exc:
            load_config(main, conf_d)
        assert str(exc.value) == (
            f"Could not read config file: {bad} (not UTF-8: invalid byte on line 2)"
        )

    def test_invalid_utf8_line_after_a_long_line(self, tmp_path):
        # larger than the 8 KiB chunks a text stream decodes in, where a position would be chunk-relative
        path = tmp_path / "c.yaml"
        raw = b"# " + b"x" * 300_000 + b"\napiKey: \xff\n"
        assert len(raw) > 8 * 1024
        path.write_bytes(raw)
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        assert str(exc.value) == (
            f"Could not read config file: {path} (not UTF-8: invalid byte on line 2)"
        )

    def test_invalid_utf8_line_in_a_large_file(self, tmp_path):
        path = tmp_path / "c.yaml"
        raw = b"port: 8080\n" + b"# padding comment line\n" * 5000 + b"apiKey: \xfe\n"
        assert len(raw) > 8 * 1024
        path.write_bytes(raw)
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        assert str(exc.value) == (
            f"Could not read config file: {path} (not UTF-8: invalid byte on line 5002)"
        )

    def test_invalid_utf8_after_a_byte_order_mark(self, tmp_path):
        path = tmp_path / "c.yaml"
        path.write_bytes(b"\xef\xbb\xbfport: 1\napiKey: abcdef\xff\n")
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        assert str(exc.value) == (
            f"Could not read config file: {path} (not UTF-8: invalid byte on line 2)"
        )

    @pytest.mark.xfail(
        strict=True,
        reason="bug: utf-8-sig strips the byte order mark before decoding, so UnicodeDecodeError.start counts "
        "from after the 3 BOM bytes, but _read counts the newlines in raw[:start] (BOM included): a bad "
        "byte within the first 3 bytes of a line is reported on the line before",
    )
    @pytest.mark.parametrize(
        "raw, line",
        [
            (b"\xef\xbb\xbfport: 1\n\xff: x\n", 2),
            (b"\xef\xbb\xbfport: 1\n# x\n# \xff\n", 3),
        ],
    )
    def test_invalid_utf8_line_at_a_line_start_after_a_byte_order_mark(
        self, tmp_path, raw, line
    ):
        path = tmp_path / "c.yaml"
        path.write_bytes(raw)
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        assert str(exc.value) == (
            f"Could not read config file: {path} (not UTF-8: invalid byte on line {line})"
        )

    @pytest.mark.xfail(
        strict=True,
        reason="regression: _read now passes the decoded text to yaml.load, so the YAMLError marks carry the "
        "buffer and the message quotes the offending line (with a stream, as before, it gives only "
        "line/column); a YAML typo on the apiKey line puts the key into the startup error and its traceback",
    )
    @pytest.mark.parametrize(
        "text", ['apiKey: "TOPSECRET\nclusters: []\n', "apiKey: TOPSECRET: x\n"]
    )
    def test_parse_error_does_not_quote_the_line(self, tmp_path, text):
        path = tmp_path / "c.yaml"
        path.write_text(text, encoding="utf-8")
        with pytest.raises(ConfigError) as exc:
            load_config(path)
        assert str(exc.value).startswith(f"Could not parse config file: {path} (")
        assert "TOPSECRET" not in "".join(traceback.format_exception(exc.value))

    def test_utf8_with_bom(self, tmp_path):
        # editors on Windows may save UTF-8 with a byte order mark
        path = tmp_path / "c.yaml"
        path.write_bytes(
            "port: 8080\nclusters: [{name: a, hosts: 'h:1', username: café}]\n".encode(
                "utf-8-sig"
            )
        )
        config = load_config(path)
        assert config.port == 8080
        assert config.clusters[0].username == "café"


class TestResolvePasswords:
    @pytest.fixture
    def passwords_dir(self, tmp_path) -> Path:
        path = tmp_path / "passwords"
        path.mkdir()
        return path

    def config(self, passwords_dir, *clusters) -> ServerConfig:
        return parse_config(
            {"passwordsDir": str(passwords_dir), "clusters": list(clusters)}
        )

    def test_password_from_file(self, passwords_dir, clean_password_env):
        (passwords_dir / "pw1_pass").write_text("s3cret\n", encoding="utf-8")
        (passwords_dir / "pw2_pass").write_text("other\r\n", encoding="utf-8")
        (passwords_dir / "pw3_pass").write_text(" spaced ", encoding="utf-8")
        config = self.config(
            passwords_dir,
            cluster(name="pw1"),
            cluster(name="pw2"),
            cluster(name="pw3"),
        )
        resolve_passwords(config)
        assert config.cluster("pw1").password == "s3cret"
        assert config.cluster("pw2").password == "other"
        assert config.cluster("pw3").password == " spaced "

    def test_password_from_env(self, passwords_dir, clean_password_env):
        clean_password_env.setenv("pw1_pass", "from-env")
        config = self.config(passwords_dir, cluster(name="pw1"))
        resolve_passwords(config)
        assert config.clusters[0].password == "from-env"

    def test_env_when_passwords_dir_is_missing(self, tmp_path, clean_password_env):
        clean_password_env.setenv("pw1_pass", "from-env")
        config = self.config(tmp_path / "missing", cluster(name="pw1"))
        resolve_passwords(config)
        assert config.clusters[0].password == "from-env"

    def test_file_wins_over_env(self, passwords_dir, clean_password_env):
        clean_password_env.setenv("pw1_pass", "from-env")
        (passwords_dir / "pw1_pass").write_text("from-file\n", encoding="utf-8")
        config = self.config(passwords_dir, cluster(name="pw1"))
        resolve_passwords(config)
        assert config.clusters[0].password == "from-file"

    def test_sentinel_password(self, passwords_dir, clean_password_env):
        (passwords_dir / "snt_pass").write_text("redis-pw\n", encoding="utf-8")
        (passwords_dir / "snt_sentinel_pass").write_text(
            "sentinel-pw\n", encoding="utf-8"
        )
        config = self.config(
            passwords_dir,
            cluster(name="snt", mode="sentinel", hosts="s:26379", masterName="m"),
        )
        resolve_passwords(config)
        assert config.clusters[0].password == "redis-pw"
        assert config.clusters[0].sentinel_password == "sentinel-pw"

    def test_sentinel_password_from_env(self, passwords_dir, clean_password_env):
        clean_password_env.setenv("snt_sentinel_pass", "sentinel-env")
        config = self.config(
            passwords_dir,
            cluster(name="snt", mode="sentinel", hosts="s:26379", masterName="m"),
        )
        resolve_passwords(config)
        assert config.clusters[0].sentinel_password == "sentinel-env"

    def test_sentinel_password_only_for_sentinel_mode(
        self, passwords_dir, clean_password_env
    ):
        (passwords_dir / "pw1_pass").write_text("pw\n", encoding="utf-8")
        (passwords_dir / "pw1_sentinel_pass").write_text("unused\n", encoding="utf-8")
        config = self.config(passwords_dir, cluster(name="pw1"))
        resolve_passwords(config)
        assert config.clusters[0].sentinel_password is None

    def test_missing_password_logs_a_warning(
        self, passwords_dir, clean_password_env, caplog
    ):
        config = self.config(passwords_dir, cluster(name="nopw"))
        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            resolve_passwords(config)
        assert config.clusters[0].password is None
        warnings = [r for r in caplog.records if r.name == LOGGER_NAME]
        assert len(warnings) == 1
        assert warnings[0].levelno == logging.WARNING
        message = warnings[0].getMessage()
        assert "cluster nopw: no password" in message
        assert f"{passwords_dir}/nopw_pass" in message
        assert "env nopw_pass" in message
        assert "connecting without AUTH" in message

    def test_no_warning_when_password_is_set(
        self, passwords_dir, clean_password_env, caplog
    ):
        (passwords_dir / "pw1_pass").write_text("pw\n", encoding="utf-8")
        config = self.config(passwords_dir, cluster(name="pw1"))
        with caplog.at_level(logging.WARNING, logger=LOGGER_NAME):
            resolve_passwords(config)
        assert [r for r in caplog.records if r.name == LOGGER_NAME] == []

    def test_no_warning_for_entraid(self, passwords_dir, clean_password_env, caplog):
        (passwords_dir / "pw1_pass").write_text("pw\n", encoding="utf-8")
        config = self.config(
            passwords_dir, cluster(name="ent", entraid=True), cluster(name="pw1")
        )
        with (
            patch("src.common.config.is_entraid_auth_enabled", return_value=True),
            caplog.at_level(logging.WARNING, logger=LOGGER_NAME),
        ):
            resolve_passwords(config)
        assert config.cluster("ent").password is None
        assert config.cluster("pw1").password == "pw"
        assert config.passwords_resolved is True
        assert [r for r in caplog.records if r.name == LOGGER_NAME] == []

    def test_entraid_without_auth_flow_is_an_error(
        self, passwords_dir, clean_password_env
    ):
        # without a flow the client would connect without any authentication
        (passwords_dir / "pw1_pass").write_text("pw\n", encoding="utf-8")
        config = self.config(
            passwords_dir,
            cluster(name="ent", entraid=True),
            cluster(name="pw1"),
            cluster(name="ent2", mode="cluster", entraid=True),
        )
        with patch("src.common.config.is_entraid_auth_enabled", return_value=False):
            with pytest.raises(ConfigError) as exc:
                resolve_passwords(config)
        assert str(exc.value) == (
            "clusters ent, ent2 set entraid: true, but no Entra ID auth flow is configured"
            " (REDIS_ENTRAID_AUTH_FLOW or --entraid-auth-flow)"
        )
        assert config.passwords_resolved is False
        assert config.cluster("pw1").password is None

    def test_entraid_without_auth_flow_unpatched(
        self, passwords_dir, clean_password_env
    ):
        # the real check reads ENTRAID_CFG (REDIS_ENTRAID_AUTH_FLOW / --entraid-auth-flow)
        config = self.config(passwords_dir, cluster(name="ent", entraid=True))
        with patch.dict("src.common.config.ENTRAID_CFG", {"auth_flow": None}):
            with pytest.raises(
                ConfigError, match="no Entra ID auth flow is configured"
            ):
                resolve_passwords(config)

    def test_entraid_with_auth_flow_unpatched(self, passwords_dir, clean_password_env):
        config = self.config(passwords_dir, cluster(name="ent", entraid=True))
        with patch.dict(
            "src.common.config.ENTRAID_CFG", {"auth_flow": "service_principal"}
        ):
            resolve_passwords(config)
        assert config.passwords_resolved is True
        assert config.clusters[0].password is None

    def test_auth_flow_is_not_needed_without_entraid(
        self, passwords_dir, clean_password_env
    ):
        config = self.config(passwords_dir, cluster(name="pw1"))
        with patch(
            "src.common.config.is_entraid_auth_enabled", return_value=False
        ) as enabled:
            resolve_passwords(config)
        enabled.assert_not_called()
        assert config.passwords_resolved is True

    def test_passwords_resolved_flag(self, passwords_dir, clean_password_env):
        config = self.config(passwords_dir, cluster(name="pw1"))
        assert config.passwords_resolved is False
        resolve_passwords(config)
        assert config.passwords_resolved is True

    def test_password_file_directory_falls_back_to_env(
        self, passwords_dir, clean_password_env
    ):
        (passwords_dir / "pw1_pass").mkdir()
        clean_password_env.setenv("pw1_pass", "from-env")
        config = self.config(passwords_dir, cluster(name="pw1"))
        resolve_passwords(config)
        assert config.clusters[0].password == "from-env"

    def test_unreadable_password_file(self, passwords_dir, clean_password_env):
        (passwords_dir / "pw1_pass").write_text("pw\n", encoding="utf-8")
        config = self.config(passwords_dir, cluster(name="pw1"))
        with patch.object(
            Path, "read_text", side_effect=PermissionError(13, "Permission denied")
        ):
            with pytest.raises(ConfigError) as exc:
                resolve_passwords(config)
        assert str(exc.value) == (
            f"Could not read password file {passwords_dir / 'pw1_pass'} (Permission denied)"
        )

    def test_invalid_utf8_password_file(self, passwords_dir, clean_password_env):
        (passwords_dir / "bad_pass").write_bytes(b"p\xe4ss-TOPSECRET\n")
        config = self.config(passwords_dir, cluster(name="bad"))
        with pytest.raises(ConfigError) as exc:
            resolve_passwords(config)
        assert str(exc.value) == (
            f"Could not read password file {passwords_dir / 'bad_pass'} (not UTF-8)"
        )
        assert "TOPSECRET" not in str(exc.value)
        assert isinstance(exc.value.__cause__, UnicodeDecodeError)

    def test_invalid_utf8_sentinel_password_file(
        self, passwords_dir, clean_password_env
    ):
        (passwords_dir / "snt_pass").write_text("redis-pw\n", encoding="utf-8")
        (passwords_dir / "snt_sentinel_pass").write_bytes(b"\xff\xfe\n")
        config = self.config(
            passwords_dir,
            cluster(name="snt", mode="sentinel", hosts="s:26379", masterName="m"),
        )
        with pytest.raises(ConfigError) as exc:
            resolve_passwords(config)
        assert str(exc.value) == (
            f"Could not read password file {passwords_dir / 'snt_sentinel_pass'} (not UTF-8)"
        )

    def test_invalid_utf8_file_does_not_fall_back_to_env(
        self, passwords_dir, clean_password_env
    ):
        clean_password_env.setenv("bad_pass", "from-env")
        (passwords_dir / "bad_pass").write_bytes(b"\xff\n")
        config = self.config(passwords_dir, cluster(name="bad"))
        with pytest.raises(ConfigError, match="not UTF-8"):
            resolve_passwords(config)

    def test_non_ascii_utf8_password(self, passwords_dir, clean_password_env):
        (passwords_dir / "pw1_pass").write_bytes("pässwörd\n".encode())
        config = self.config(passwords_dir, cluster(name="pw1"))
        resolve_passwords(config)
        assert config.clusters[0].password == "pässwörd"


class TestFromLegacy:
    def test_standalone(self, redis_config):
        config = from_legacy(redis_config)
        assert config.cluster_names() == [LEGACY_CLUSTER_NAME]
        c = config.clusters[0]
        assert c.mode == "standalone"
        assert c.hosts == "localhost:6379"
        assert c.addresses() == [("localhost", 6379)]
        assert c.db == 0
        assert c.username is None
        assert c.password is None
        assert c.tls is None
        assert c.entraid is False
        assert c.modules == ["json", "search"]
        assert config.all_toolsets is True
        assert config.passwords_resolved is True
        assert config.port is None
        assert config.api_key is None
        assert config.read_only is False
        assert config.is_read_only(c) is False

    def test_db(self, redis_config):
        redis_config["db"] = 5
        assert from_legacy(redis_config).clusters[0].db == 5

    def test_cluster_mode_drops_db(self, redis_config):
        redis_config.update(cluster_mode=True, db=5, port=7000)
        c = from_legacy(redis_config).clusters[0]
        assert c.mode == "cluster"
        assert c.db == 0
        assert c.hosts == "localhost:7000"

    def test_username_and_password(self, redis_config):
        redis_config.update(username="mcp", password="s3cret")
        c = from_legacy(redis_config).clusters[0]
        assert c.username == "mcp"
        assert c.password == "s3cret"

    def test_ssl_maps_to_tls(self, redis_config):
        redis_config.update(
            ssl=True,
            ssl_ca_certs="/ca.crt",
            ssl_ca_path="/ca",
            ssl_certfile="/tls.crt",
            ssl_keyfile="/tls.key",
            ssl_cert_reqs="optional",
        )
        tls = from_legacy(redis_config).clusters[0].tls
        assert tls == TlsConfig.model_validate(
            {
                "caFile": "/ca.crt",
                "caPath": "/ca",
                "certFile": "/tls.crt",
                "keyFile": "/tls.key",
                "certReqs": "optional",
            }
        )
        assert tls.client_kwargs() == {
            "ssl": True,
            "ssl_cert_reqs": "optional",
            "ssl_ca_certs": "/ca.crt",
            "ssl_ca_path": "/ca",
            "ssl_certfile": "/tls.crt",
            "ssl_keyfile": "/tls.key",
        }

    def test_ssl_with_defaults(self, redis_config):
        redis_config["ssl"] = True
        assert from_legacy(redis_config).clusters[0].tls == TlsConfig()

    def test_ssl_off_ignores_ssl_settings(self, redis_config):
        redis_config.update(ssl=False, ssl_ca_certs="/ca.crt")
        assert from_legacy(redis_config).clusters[0].tls is None

    def test_entraid_drops_username_and_password(self, redis_config):
        redis_config.update(username="mcp", password="s3cret")
        c = from_legacy(redis_config, entraid=True).clusters[0]
        assert c.entraid is True
        assert c.username is None
        assert c.password is None

    @pytest.mark.parametrize(
        "host, hosts, address",
        [
            ("::1", "[::1]:6379", ("::1", 6379)),
            ("fe80::1", "[fe80::1]:6379", ("fe80::1", 6379)),
            ("10.0.0.1", "10.0.0.1:6379", ("10.0.0.1", 6379)),
            (None, "127.0.0.1:6379", ("127.0.0.1", 6379)),
            ("", "127.0.0.1:6379", ("127.0.0.1", 6379)),
        ],
    )
    def test_host(self, redis_config, host, hosts, address):
        redis_config["host"] = host
        c = from_legacy(redis_config).clusters[0]
        assert c.hosts == hosts
        assert c.addresses() == [address]

    def test_invalid_settings_raise_config_error(self, redis_config):
        redis_config.update(ssl=True, ssl_cert_reqs="sometimes")
        with pytest.raises(ConfigError, match=r"clusters\[0\]\.tls\.certReqs"):
            from_legacy(redis_config)

    @pytest.mark.parametrize(
        "extra",
        [
            {},
            {"ssl_ca_certs": "/ca.crt"},
            {"ssl_cert_reqs": "none"},
            {"ssl_certfile": "/tls.crt", "ssl_keyfile": "/tls.key"},
        ],
    )
    def test_cluster_mode_with_ssl_ca_path_is_an_error(self, redis_config, extra):
        # RedisCluster would drop ssl_ca_path; failing is better than verifying against the system CAs.
        # The message names the flag / variable the user set, not the config key.
        redis_config.update(cluster_mode=True, ssl=True, ssl_ca_path="/ca", **extra)
        with pytest.raises(ConfigError) as exc:
            from_legacy(redis_config)
        assert str(exc.value) == (
            "--ssl-ca-path / REDIS_SSL_CA_PATH is not supported with --cluster-mode (redis-py's RedisCluster"
            " ignores it); use --ssl-ca-certs / REDIS_SSL_CA_CERTS"
        )

    def test_cluster_mode_without_ssl_ignores_ssl_ca_path(self, redis_config):
        redis_config.update(cluster_mode=True, ssl=False, ssl_ca_path="/ca")
        c = from_legacy(redis_config).clusters[0]
        assert c.mode == "cluster"
        assert c.tls is None

    def test_ssl_ca_path_without_cluster_mode(self, redis_config):
        redis_config.update(ssl=True, ssl_ca_path="/ca")
        tls = from_legacy(redis_config).clusters[0].tls
        assert tls.client_kwargs()["ssl_ca_path"] == "/ca"

    def test_cluster_mode_with_ssl_ca_certs(self, redis_config):
        redis_config.update(cluster_mode=True, ssl=True, ssl_ca_certs="/ca.crt")
        tls = from_legacy(redis_config).clusters[0].tls
        assert tls.ca_file == "/ca.crt"

    def test_host_with_credentials_is_rejected(self, redis_config):
        redis_config["host"] = "user:TOPSECRET@h"
        with pytest.raises(ConfigError) as exc:
            from_legacy(redis_config)
        assert "hosts takes host:port entries" in str(exc.value)
        assert "TOPSECRET" not in str(exc.value)


class TestGetConfig:
    LEGACY = {
        "host": "legacy.example.com",
        "port": 6390,
        "db": 2,
        "username": "legacy-user",
        "password": "legacy-pw",
        "ssl": False,
        "cluster_mode": False,
    }

    def test_builds_the_legacy_config_lazily(self):
        with (
            patch.dict("src.common.config.REDIS_CFG", self.LEGACY),
            patch("src.common.config.is_entraid_auth_enabled", return_value=False),
            patch(
                "src.common.config_loader.from_legacy", wraps=from_legacy
            ) as mock_from_legacy,
        ):
            assert mock_from_legacy.call_count == 0
            config = get_config()
            assert get_config() is config
        assert mock_from_legacy.call_count == 1
        assert mock_from_legacy.call_args.kwargs == {"entraid": False}
        c = config.clusters[0]
        assert c.name == LEGACY_CLUSTER_NAME
        assert c.hosts == "legacy.example.com:6390"
        assert c.db == 2
        assert c.username == "legacy-user"
        assert c.password == "legacy-pw"
        assert config.all_toolsets is True

    def test_entraid(self):
        with (
            patch.dict("src.common.config.REDIS_CFG", self.LEGACY),
            patch("src.common.config.is_entraid_auth_enabled", return_value=True),
        ):
            c = get_config().clusters[0]
        assert c.entraid is True
        assert c.username is None
        assert c.password is None

    def test_set_config(self):
        config = parse_config({"clusters": [cluster(name="mine")]})
        set_config(config)
        assert get_config() is config

    def test_set_config_none_resets(self):
        set_config(parse_config({"clusters": [cluster(name="mine")]}))
        set_config(None)
        with patch.dict("src.common.config.REDIS_CFG", self.LEGACY):
            assert get_config().cluster_names() == [LEGACY_CLUSTER_NAME]


class TestClusterConfigModel:
    def test_validates_directly(self):
        c = ClusterConfig.model_validate(
            {"name": "a", "mode": "cluster", "hosts": ["n1:1", "n2:2"]}
        )
        assert c.addresses() == [("n1", 1), ("n2", 2)]

    def test_snake_case_keys_are_rejected(self):
        with pytest.raises(ValueError):
            ClusterConfig.model_validate(
                {"name": "a", "hosts": "h:1", "read_only": True}
            )
