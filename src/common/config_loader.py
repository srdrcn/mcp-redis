"""Multi-cluster configuration.

The server reads one YAML file (``--config``) and/or the drop-in ``*.yaml``/``*.yml`` files of a directory
(``--config-dir``, merged over the main file in file name order). Maps are deep-merged, the ``clusters`` list is
merged by ``name`` (a drop-in can add a cluster or override fields of an existing one) and every other list
replaces the earlier value. Unknown keys are errors.

Passwords are never part of the YAML: for a cluster named ``<name>`` the password is read from the file
``<passwordsDir>/<name>_pass`` (a mounted Secret), or from the environment variable ``<name>_pass``.

Without a config file the server keeps the original behaviour: the REDIS_* environment variables and CLI flags
describe a single cluster named ``default``.
"""

import logging
import os
import re
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, Union

import yaml
from pydantic import (
    BaseModel,
    ConfigDict,
    PrivateAttr,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic.alias_generators import to_camel

_logger = logging.getLogger(__name__)

CLUSTER_NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,62}")
DEFAULT_PASSWORDS_DIR = "/etc/redis-mcp/passwords"
# file / env variable name suffixes, not passwords
PASSWORD_SUFFIX = "_pass"  # nosec B105
SENTINEL_PASSWORD_SUFFIX = "_sentinel_pass"  # nosec B105
LEGACY_CLUSTER_NAME = "default"

# Recommended minimum length of a non-empty apiKey; shorter keys only produce a startup warning.
RECOMMENDED_API_KEY_LENGTH = 16

# redis-py arguments a cluster may tune through `options`. Everything else is either set from the cluster keys
# (hosts, db, username, tls), comes from the passwords directory, or is managed by the server (decode_responses,
# retry), so `options` cannot carry a secret, point the client elsewhere or be silently dropped by redis-py.
COMMON_OPTIONS = (
    "socket_timeout",
    "socket_connect_timeout",
    "socket_keepalive",
    "max_connections",
    "client_name",
    "protocol",
)
MODE_OPTIONS = {
    "standalone": COMMON_OPTIONS + ("health_check_interval",),
    "sentinel": COMMON_OPTIONS + ("health_check_interval",),
    "cluster": COMMON_OPTIONS
    + (
        "read_from_replicas",
        "require_full_coverage",
        "reinitialize_steps",
        "dynamic_startup_nodes",
    ),
}

_INT = re.compile(r"[-+]?[0-9]+")
_FLOAT = re.compile(r"[-+]?([0-9]+\.[0-9]*|\.[0-9]+|[0-9]+)([eE][-+]?[0-9]+)?")


class ConfigError(Exception):
    """Invalid configuration or command line."""


class _Model(BaseModel):
    model_config = ConfigDict(extra="forbid", alias_generator=to_camel)


class TlsConfig(_Model):
    """TLS settings of a cluster. Each key maps to the redis-py ssl_* argument named in the comment."""

    ca_file: Optional[str] = None  # ssl_ca_certs
    ca_path: Optional[str] = None  # ssl_ca_path
    cert_file: Optional[str] = None  # ssl_certfile
    key_file: Optional[str] = None  # ssl_keyfile
    cert_reqs: Literal["required", "optional", "none"] = "required"  # ssl_cert_reqs
    check_hostname: Optional[bool] = None  # ssl_check_hostname

    def client_kwargs(self) -> Dict[str, Any]:
        kwargs: Dict[str, Any] = {"ssl": True, "ssl_cert_reqs": self.cert_reqs}
        for key, value in (
            ("ssl_ca_certs", self.ca_file),
            ("ssl_ca_path", self.ca_path),
            ("ssl_certfile", self.cert_file),
            ("ssl_keyfile", self.key_file),
            ("ssl_check_hostname", self.check_hostname),
        ):
            if value is not None:
                kwargs[key] = value
        return kwargs


class ClusterConfig(_Model):
    name: str
    mode: Literal["standalone", "cluster", "sentinel"] = "standalone"
    # standalone: host:port; cluster: startup nodes; sentinel: the sentinels
    hosts: str
    db: int = 0
    username: Optional[str] = None
    master_name: Optional[str] = None
    # `tls: true` turns TLS on with the defaults; a map sets the details
    tls: Optional[TlsConfig] = None
    # if None, the global readOnly applies
    read_only: Optional[bool] = None
    # modules the json/search tools need; those tools are listed only on clusters that name them
    modules: List[Literal["json", "search"]] = []
    # use the Entra ID credential provider (REDIS_ENTRAID_* settings) instead of a password
    entraid: bool = False
    # redis-py client argument overrides, applied after the defaults
    options: Dict[str, Any] = {}

    _password: Optional[str] = PrivateAttr(default=None)
    _sentinel_password: Optional[str] = PrivateAttr(default=None)

    @field_validator("name")
    @classmethod
    def _check_name(cls, value: str) -> str:
        if not CLUSTER_NAME.fullmatch(value):
            raise ValueError(
                "letters, digits, _ . - ; starting with a letter or digit; at most 63 characters"
            )
        return value

    @field_validator("hosts", mode="before")
    @classmethod
    def _join_hosts(cls, value: Any) -> Any:
        if isinstance(value, list):
            return ",".join(str(v) for v in value)
        return value

    @field_validator("hosts")
    @classmethod
    def _check_hosts(cls, value: str) -> str:
        parse_hosts(value)
        return value

    @field_validator("tls", mode="before")
    @classmethod
    def _tls_flag(cls, value: Any) -> Any:
        if value is True:
            return {}
        if value is False:
            return None
        return value

    @field_validator("modules")
    @classmethod
    def _unique_modules(cls, value: List[str]) -> List[str]:
        if len(set(value)) != len(value):
            raise ValueError("duplicate module")
        return value

    @field_validator("options")
    @classmethod
    def _convert_options(cls, value: Dict[str, Any]) -> Dict[str, Any]:
        return {key: _option_value(v) for key, v in value.items()}

    @model_validator(mode="after")
    def _check_mode(self) -> "ClusterConfig":
        addresses = self.addresses()
        if self.mode == "standalone" and len(addresses) != 1:
            raise ValueError(
                "a standalone cluster takes exactly one host:port in hosts"
            )
        if self.mode == "sentinel" and not self.master_name:
            raise ValueError("masterName is required when mode is sentinel")
        if self.mode != "sentinel" and self.master_name:
            raise ValueError("masterName is only used when mode is sentinel")
        if self.mode == "cluster" and self.db != 0:
            raise ValueError("Redis Cluster only has database 0; remove db")
        if self.db < 0:
            raise ValueError("db must be 0 or greater")
        if self.entraid and self.username:
            raise ValueError("username cannot be combined with entraid")
        if self.mode == "cluster" and self.tls is not None and self.tls.ca_path:
            # redis-py's RedisCluster drops ssl_ca_path, so the nodes would be verified against the system CAs
            raise ValueError("tls.caPath is not supported in cluster mode; use caFile")
        allowed = MODE_OPTIONS[self.mode]
        unsupported = sorted(key for key in self.options if key not in allowed)
        if unsupported:
            raise ValueError(
                f"options {', '.join(unsupported)} not supported in {self.mode} mode"
                f" (supported: {', '.join(allowed)}; addresses, db, username and tls have their own keys,"
                " passwords come from the passwords directory)"
            )
        return self

    def addresses(self) -> List[Tuple[str, int]]:
        return parse_hosts(self.hosts)

    @property
    def password(self) -> Optional[str]:
        return self._password

    @property
    def sentinel_password(self) -> Optional[str]:
        return self._sentinel_password


class ServerConfig(_Model):
    """Root of redis-mcp.yaml. Field aliases match the YAML keys (camelCase)."""

    # HTTP port: when set (> 0) and --transport is not given, Streamable HTTP runs, otherwise stdio
    port: Optional[int] = None
    # if not empty, HTTP requests must send this value in the X-Authorization header
    api_key: Optional[str] = None
    read_only: bool = False
    disable_destructive: bool = False
    toolsets: List[str] = ["core"]
    passwords_dir: str = DEFAULT_PASSWORDS_DIR
    clusters: List[ClusterConfig]

    # the legacy single-cluster mode lists every toolset, as the server always did
    _all_toolsets: bool = PrivateAttr(default=False)
    # set once the cluster passwords are known (resolve_passwords, or from_legacy)
    _passwords_resolved: bool = PrivateAttr(default=False)

    @field_validator("port")
    @classmethod
    def _check_port(cls, value: Optional[int]) -> Optional[int]:
        if value is not None and not 0 <= value <= 65535:
            raise ValueError("port must be between 0 and 65535")
        return value

    @field_validator("api_key")
    @classmethod
    def _check_api_key(cls, value: Optional[str]) -> Optional[str]:
        error = api_key_error(value)
        if error:
            raise ValueError(error)
        return value

    @field_validator("toolsets")
    @classmethod
    def _check_toolsets(cls, value: List[str]) -> List[str]:
        if any(not t or not t.strip() for t in value):
            raise ValueError("the toolsets list contains an empty value")
        duplicates = sorted({t for t in value if value.count(t) > 1})
        if duplicates:
            raise ValueError(f"duplicate toolset: {', '.join(duplicates)}")
        return value

    @field_validator("clusters")
    @classmethod
    def _check_clusters(cls, value: List[ClusterConfig]) -> List[ClusterConfig]:
        if not value:
            raise ValueError("at least one cluster must be defined")
        names = [c.name for c in value]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise ValueError(f"duplicate cluster name: {', '.join(duplicates)}")
        for c in value:
            # <name>_sentinel_pass would also be the password file of a cluster named <name>_sentinel
            if c.mode == "sentinel" and c.name + "_sentinel" in names:
                raise ValueError(
                    f"cluster name {c.name}_sentinel clashes with the Sentinel password of cluster {c.name}"
                    f" ({c.name}{SENTINEL_PASSWORD_SUFFIX}); rename one of them"
                )
        return value

    @property
    def api_key_enabled(self) -> bool:
        return bool(self.api_key)

    @property
    def all_toolsets(self) -> bool:
        return self._all_toolsets

    @property
    def passwords_resolved(self) -> bool:
        return self._passwords_resolved

    def is_read_only(self, cluster: ClusterConfig) -> bool:
        """Effective readOnly of the cluster: its own value if set, otherwise the global one."""
        return cluster.read_only if cluster.read_only is not None else self.read_only

    def cluster(self, name: str) -> Optional[ClusterConfig]:
        for c in self.clusters:
            if c.name == name:
                return c
        return None

    def cluster_names(self) -> List[str]:
        return [c.name for c in self.clusters]


def api_key_error(api_key: Optional[str]) -> Optional[str]:
    """None if the apiKey is acceptable: not set or empty (no authentication), or a value without
    surrounding whitespace that is not a template placeholder."""
    if not api_key:
        return None
    if not api_key.strip():
        return (
            "apiKey is blank; set a key, or leave it empty to turn authentication off"
        )
    if api_key.strip() != api_key:
        return "apiKey has leading or trailing whitespace (HTTP header values are trimmed, so it could never match)"
    if "<" in api_key or ">" in api_key or "${" in api_key:
        return "apiKey still holds a placeholder (<...> or ${...}); set a real key, e.g. from `openssl rand -hex 32`"
    return None


def parse_hosts(hosts: str) -> List[Tuple[str, int]]:
    """Parses ``host:port,host:port`` (IPv6 as ``[addr]:port``)."""
    addresses = []
    for item in hosts.split(","):
        item = item.strip()
        if "@" in item or "://" in item:
            # the item is not echoed: it may hold a password
            raise ValueError(
                "hosts takes host:port entries without a scheme or credentials (user:password@);"
                " passwords come from the passwords directory"
            )
        host, sep, port = item.rpartition(":")
        if not item or not sep or not host or not port.isdigit():
            raise ValueError(f"'{item}' is not host:port")
        if host.startswith("[") and host.endswith("]"):
            host = host[1:-1]
        port_number = int(port)
        if not 0 < port_number <= 65535:
            raise ValueError(f"'{item}': port must be between 1 and 65535")
        addresses.append((host, port_number))
    return addresses


def _option_value(value: Any) -> Any:
    # Numbers stay text in the YAML tree (see _TextLoader); options go to redis-py as they would in Python.
    if isinstance(value, str):
        if _INT.fullmatch(value):
            return int(value)
        if _FLOAT.fullmatch(value):
            return float(value)
    return value


class _TextLoader(yaml.SafeLoader):
    """SafeLoader without the int, float and timestamp resolvers.

    YAML 1.1 reads ``012345`` as octal 5349, ``0x1F`` as 31, ``1_000`` as 1000 and ``1:20`` as 80, so a value
    such as an apiKey or a username would silently change. Numbers therefore stay text and are converted where a
    number is expected (port, db). Booleans and nulls keep their type.
    """


_TextLoader.yaml_implicit_resolvers = {
    first: [
        (tag, regexp)
        for tag, regexp in resolvers
        if tag
        not in (
            "tag:yaml.org,2002:int",
            "tag:yaml.org,2002:float",
            "tag:yaml.org,2002:timestamp",
        )
    ]
    for first, resolvers in yaml.SafeLoader.yaml_implicit_resolvers.items()
}


def _read(path: Path) -> Dict[str, Any]:
    try:
        with open(path, encoding="utf-8") as f:
            # _TextLoader is a SafeLoader
            data = yaml.load(f, Loader=_TextLoader)  # nosec B506
    except OSError as e:
        raise ConfigError(f"Could not read config file: {path} ({e.strerror})") from e
    except UnicodeDecodeError as e:
        raise ConfigError(
            f"Could not read config file: {path} (not UTF-8: invalid byte at position {e.start})"
        ) from e
    except yaml.YAMLError as e:
        raise ConfigError(f"Could not parse config file: {path} ({e})") from e
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise ConfigError(f"Config file must be a YAML map: {path}")
    return data


def drop_ins(config_dir: Path) -> List[Path]:
    if not config_dir.is_dir():
        raise ConfigError(f"--config-dir is not a directory: {config_dir}")
    return sorted(
        p
        for p in config_dir.iterdir()
        if p.is_file() and not p.name.startswith(".") and p.suffix in (".yaml", ".yml")
    )


def merge(target: Dict[str, Any], source: Dict[str, Any]) -> None:
    """Deep-merges source into target: maps recursively, ``clusters`` by name, everything else replaced."""
    for key, value in source.items():
        existing = target.get(key)
        if key == "clusters" and isinstance(value, list) and isinstance(existing, list):
            target[key] = _merge_clusters(existing, value)
        elif isinstance(value, dict) and isinstance(existing, dict):
            copy = dict(existing)
            merge(copy, value)
            target[key] = copy
        else:
            target[key] = value


def _merge_clusters(base: List[Any], overlay: List[Any]) -> List[Any]:
    result = [dict(c) if isinstance(c, dict) else c for c in base]
    for item in overlay:
        match = None
        if isinstance(item, dict) and "name" in item:
            match = next(
                (
                    c
                    for c in result
                    if isinstance(c, dict) and c.get("name") == item["name"]
                ),
                None,
            )
        if match is not None:
            merge(match, item)
        else:
            result.append(item)
    return result


def _format_errors(error: ValidationError) -> str:
    lines = []
    for e in error.errors():
        path = ""
        for part in e["loc"]:
            path += (
                f"[{part}]"
                if isinstance(part, int)
                else (f".{part}" if path else str(part))
            )
        message = e["msg"].removeprefix("Value error, ")
        lines.append(f"{path}: {message}" if path else message)
    return "Invalid config:\n - " + "\n - ".join(lines)


def parse_config(data: Dict[str, Any]) -> ServerConfig:
    try:
        return ServerConfig.model_validate(data)
    except ValidationError as e:
        raise ConfigError(_format_errors(e)) from e


def load_config(
    config_file: Optional[Path] = None, config_dir: Optional[Path] = None
) -> ServerConfig:
    """Reads, merges and validates the config. The passwords are read later by :func:`resolve_passwords`
    (the server does that once logging is set up, so its warnings reach the configured log)."""
    root: Dict[str, Any] = {}
    if config_file is not None:
        merge(root, _read(config_file))
    if config_dir is not None:
        for path in drop_ins(config_dir):
            merge(root, _read(path))
    return parse_config(root)


def _read_secret(passwords_dir: str, key: str) -> Optional[str]:
    path = Path(passwords_dir) / key
    if path.is_file():
        try:
            return path.read_text(encoding="utf-8").rstrip("\r\n")
        except OSError as e:
            raise ConfigError(
                f"Could not read password file {path} ({e.strerror})"
            ) from e
        except UnicodeDecodeError as e:
            raise ConfigError(f"Could not read password file {path} (not UTF-8)") from e
    return os.environ.get(key)


def resolve_passwords(config: ServerConfig) -> None:
    """Sets each cluster's password from ``<passwordsDir>/<name>_pass`` or the ``<name>_pass`` env variable
    (and a sentinel's own password from ``<name>_sentinel_pass``). Clusters with ``entraid: true`` need an
    Entra ID auth flow (REDIS_ENTRAID_AUTH_FLOW / --entraid-auth-flow); without one they would connect without
    any authentication, so that is an error."""
    entraid_clusters = [c.name for c in config.clusters if c.entraid]
    if entraid_clusters:
        from src.common.config import is_entraid_auth_enabled

        if not is_entraid_auth_enabled():
            raise ConfigError(
                f"clusters {', '.join(entraid_clusters)} set entraid: true, but no Entra ID auth flow is"
                " configured (REDIS_ENTRAID_AUTH_FLOW or --entraid-auth-flow)"
            )
    for c in config.clusters:
        c._password = _read_secret(config.passwords_dir, c.name + PASSWORD_SUFFIX)
        if c.mode == "sentinel":
            c._sentinel_password = _read_secret(
                config.passwords_dir, c.name + SENTINEL_PASSWORD_SUFFIX
            )
        if c.password is None and not c.entraid:
            _logger.warning(
                "cluster %s: no password (%s/%s%s or env %s%s); connecting without AUTH",
                c.name,
                config.passwords_dir,
                c.name,
                PASSWORD_SUFFIX,
                c.name,
                PASSWORD_SUFFIX,
            )
    config._passwords_resolved = True


def from_legacy(redis_cfg: Dict[str, Any], entraid: bool = False) -> ServerConfig:
    """The single ``default`` cluster described by the REDIS_* environment variables / CLI flags."""
    cluster: Dict[str, Any] = {
        "name": LEGACY_CLUSTER_NAME,
        "mode": "cluster" if redis_cfg.get("cluster_mode") else "standalone",
        "hosts": f"{_host(redis_cfg.get('host'))}:{redis_cfg.get('port')}",
        "modules": ["json", "search"],
        "entraid": entraid,
    }
    if not redis_cfg.get("cluster_mode"):
        cluster["db"] = redis_cfg.get("db", 0)
    if redis_cfg.get("username") and not entraid:
        cluster["username"] = redis_cfg["username"]
    if redis_cfg.get("ssl"):
        cluster["tls"] = {
            key: value
            for key, value in (
                ("caFile", redis_cfg.get("ssl_ca_certs")),
                ("caPath", redis_cfg.get("ssl_ca_path")),
                ("certFile", redis_cfg.get("ssl_certfile")),
                ("keyFile", redis_cfg.get("ssl_keyfile")),
                ("certReqs", redis_cfg.get("ssl_cert_reqs")),
            )
            if value
        }
    config = parse_config({"clusters": [cluster]})
    config._all_toolsets = True
    config._passwords_resolved = True
    if not entraid:
        config.clusters[0]._password = redis_cfg.get("password") or None
    return config


def _host(host: Union[str, None]) -> str:
    host = host or "127.0.0.1"
    return f"[{host}]" if ":" in host else host


_config: Optional[ServerConfig] = None


def set_config(config: Optional[ServerConfig]) -> None:
    global _config
    _config = config


def get_config() -> ServerConfig:
    """The active config; without one, the legacy single cluster from REDIS_CFG / ENTRAID_CFG."""
    global _config
    if _config is None:
        from src.common.config import REDIS_CFG, is_entraid_auth_enabled

        _config = from_legacy(REDIS_CFG, entraid=is_entraid_auth_enabled())
    return _config
