import logging
import os
import sys
from pathlib import Path
from typing import Optional

import click
from click.core import ParameterSource

from src.common.config import (
    REDIS_CFG,
    is_entraid_auth_enabled,
    parse_redis_uri,
    set_redis_config_from_cli,
    set_entraid_config_from_cli,
)
from src.common.config_loader import (
    ConfigError,
    ServerConfig,
    api_key_error,
    from_legacy,
    get_config,
    load_config,
    resolve_passwords,
    set_config,
)
from src.common import registry
from src.common.logging_utils import configure_json_logging, configure_logging
from src.common.server import build_server

# Redis connection flags of the single-cluster mode; with --config the clusters come from the config file.
LEGACY_CONNECTION_OPTIONS = (
    "url",
    "host",
    "port",
    "db",
    "username",
    "password",
    "ssl",
    "ssl_ca_path",
    "ssl_keyfile",
    "ssl_certfile",
    "ssl_cert_reqs",
    "ssl_ca_certs",
    "cluster_mode",
)


class RedisMCPServer:
    def __init__(
        self, config: Optional[ServerConfig] = None, transport: Optional[str] = None
    ):
        self.config = config or get_config()
        set_config(self.config)
        if transport is None:
            transport = "http" if (self.config.port or 0) > 0 else "stdio"
        self.transport = transport
        # In stdio mode stdout carries JSON-RPC, so logs stay on stderr; over HTTP they are JSON on stdout.
        if self.transport == "http":
            configure_json_logging()
        else:
            configure_logging()
        self._logger = logging.getLogger(__name__)
        self._logger.info("Starting the Redis MCP Server")
        if not self.config.passwords_resolved:
            resolve_passwords(self.config)
        self.mcp = build_server(self.config)
        self._logger.info(
            "%d tools listed for clusters %s (readOnly=%s, disableDestructive=%s)",
            len(registry.select(self.config)),
            self.config.cluster_names(),
            self.config.read_only,
            self.config.disable_destructive,
        )

    def run(self):
        if self.transport == "http":
            from src.common.http import run_http

            run_http(self.mcp, self.config)
        else:
            self.mcp.run()


def _fail(message: str):
    click.echo(f"redis-mcp: {message}", err=True)
    sys.exit(2)


def _url_given(url: Optional[str]) -> bool:
    # gemini-cli passes the raw "${REDIS_URL}" string when the env var is not set
    return bool(url and url.strip() and url.strip() != "${REDIS_URL}")


def apply_api_key(config: ServerConfig, api_key: Optional[str]) -> None:
    """Sets the HTTP API key from --api-key / REDIS_MCP_API_KEY (e.g. from a Secret), over the config's apiKey.
    An empty value changes nothing, so an empty variable cannot switch authentication off."""
    if not api_key:
        return
    error = api_key_error(api_key)
    if error:
        raise ConfigError(f"--api-key / REDIS_MCP_API_KEY: {error}")
    config.api_key = api_key


@click.command()
@click.option(
    "--config",
    "config_file",
    type=click.Path(path_type=Path, dir_okay=False),
    envvar="REDIS_MCP_CONFIG",
    help="Multi-cluster config file (YAML). Env: REDIS_MCP_CONFIG",
)
@click.option(
    "--config-dir",
    type=click.Path(path_type=Path, file_okay=False),
    envvar="REDIS_MCP_CONFIG_DIR",
    help="Drop-in .yaml/.yml files merged over --config in file name order. Env: REDIS_MCP_CONFIG_DIR",
)
@click.option(
    "--transport",
    type=click.Choice(["stdio", "http"]),
    help="Transport (default: http if an HTTP port is set, otherwise stdio)",
)
@click.option(
    "--api-key",
    envvar="REDIS_MCP_API_KEY",
    help="API key HTTP clients must send in X-Authorization (overrides apiKey in the config). Env: REDIS_MCP_API_KEY",
)
@click.option(
    "--http-port",
    type=click.IntRange(1, 65535),
    help="HTTP port for Streamable HTTP on /mcp (overrides port in the config)",
)
@click.option(
    "--url",
    help="Redis connection URI (redis://user:pass@host:port/db or rediss:// for SSL)",
)
@click.option("--host", default="127.0.0.1", help="Redis host")
@click.option("--port", default=6379, type=int, help="Redis port")
@click.option("--db", default=0, type=int, help="Redis database number")
@click.option("--username", help="Redis username")
@click.option("--password", help="Redis password")
@click.option("--ssl", is_flag=True, help="Use SSL connection")
@click.option("--ssl-ca-path", help="Path to CA certificate file")
@click.option("--ssl-keyfile", help="Path to SSL key file")
@click.option("--ssl-certfile", help="Path to SSL certificate file")
@click.option(
    "--ssl-cert-reqs", default="required", help="SSL certificate requirements"
)
@click.option("--ssl-ca-certs", help="Path to CA certificates file")
@click.option("--cluster-mode", is_flag=True, help="Enable Redis cluster mode")
# Entra ID Authentication Options
@click.option(
    "--entraid-auth-flow",
    type=click.Choice(["service_principal", "managed_identity", "default_credential"]),
    help="Entra ID authentication flow",
)
@click.option(
    "--entraid-client-id",
    help="Entra ID client ID (for service principal or user-assigned managed identity)",
)
@click.option(
    "--entraid-client-secret", help="Entra ID client secret (for service principal)"
)
@click.option("--entraid-tenant-id", help="Entra ID tenant ID (for service principal)")
@click.option(
    "--entraid-identity-type",
    type=click.Choice(["system_assigned", "user_assigned"]),
    default="system_assigned",
    help="Managed identity type",
)
@click.option(
    "--entraid-scopes",
    default="https://redis.azure.com/.default",
    help="Entra ID scopes (comma-separated)",
)
@click.option(
    "--entraid-resource", default="https://redis.azure.com/", help="Entra ID resource"
)
@click.option(
    "--entraid-token-refresh-ratio",
    type=float,
    default=0.9,
    help="Token expiration refresh ratio",
)
@click.option(
    "--entraid-retry-max-attempts",
    type=int,
    default=3,
    help="Maximum retry attempts for token requests",
)
@click.option(
    "--entraid-retry-delay-ms",
    type=int,
    default=100,
    help="Retry delay in milliseconds",
)
@click.pass_context
def cli(
    ctx,
    config_file,
    config_dir,
    transport,
    api_key,
    http_port,
    url,
    host,
    port,
    db,
    username,
    password,
    ssl,
    ssl_ca_path,
    ssl_keyfile,
    ssl_certfile,
    ssl_cert_reqs,
    ssl_ca_certs,
    cluster_mode,
    entraid_auth_flow,
    entraid_client_id,
    entraid_client_secret,
    entraid_tenant_id,
    entraid_identity_type,
    entraid_scopes,
    entraid_resource,
    entraid_token_refresh_ratio,
    entraid_retry_max_attempts,
    entraid_retry_delay_ms,
):
    """Redis MCP Server - Model Context Protocol server for Redis."""

    multi_cluster = config_file is not None or config_dir is not None
    if multi_cluster:
        given = [
            "--" + name.replace("_", "-")
            for name in LEGACY_CONNECTION_OPTIONS
            if ctx.get_parameter_source(name) is ParameterSource.COMMANDLINE
            and (name != "url" or _url_given(url))
        ]
        if given:
            _fail(
                f"{', '.join(given)} cannot be combined with --config/--config-dir;"
                " define the clusters in the config file"
            )

    # Handle Redis URI if provided (and not empty)

    if multi_cluster:
        pass
    elif _url_given(url):
        try:
            uri_config = parse_redis_uri(url)
            set_redis_config_from_cli(uri_config)
        except ValueError as e:
            click.echo(f"Error parsing Redis URI: {e}", err=True)
            sys.exit(1)
    else:
        # Set individual Redis parameters
        config = {
            "host": host,
            "port": port,
            "db": db,
            "ssl": ssl,
            "cluster_mode": cluster_mode,
        }

        if username:
            config["username"] = username
        if password:
            config["password"] = password
        if ssl_ca_path:
            config["ssl_ca_path"] = ssl_ca_path
        if ssl_keyfile:
            config["ssl_keyfile"] = ssl_keyfile
        if ssl_certfile:
            config["ssl_certfile"] = ssl_certfile
        if ssl_cert_reqs:
            config["ssl_cert_reqs"] = ssl_cert_reqs
        if ssl_ca_certs:
            config["ssl_ca_certs"] = ssl_ca_certs

        set_redis_config_from_cli(config)

    # Handle Entra ID authentication configuration
    entraid_config = {}
    if entraid_auth_flow:
        entraid_config["auth_flow"] = entraid_auth_flow
    if entraid_client_id:
        entraid_config["client_id"] = entraid_client_id
    if entraid_client_secret:
        entraid_config["client_secret"] = entraid_client_secret
    if entraid_tenant_id:
        entraid_config["tenant_id"] = entraid_tenant_id
    if entraid_identity_type:
        entraid_config["identity_type"] = entraid_identity_type
    if entraid_scopes:
        entraid_config["scopes"] = entraid_scopes
    if entraid_resource:
        entraid_config["resource"] = entraid_resource
    if entraid_token_refresh_ratio is not None:
        entraid_config["token_expiration_refresh_ratio"] = entraid_token_refresh_ratio
    if entraid_retry_max_attempts is not None:
        entraid_config["retry_max_attempts"] = entraid_retry_max_attempts
    if entraid_retry_delay_ms is not None:
        entraid_config["retry_delay_ms"] = entraid_retry_delay_ms

    # For user-assigned managed identity, use client_id as user_assigned_identity_client_id
    if (
        entraid_auth_flow == "managed_identity"
        and entraid_identity_type == "user_assigned"
        and entraid_client_id
    ):
        entraid_config["user_assigned_identity_client_id"] = entraid_client_id

    if entraid_config:
        set_entraid_config_from_cli(entraid_config)

    try:
        if multi_cluster:
            config = load_config(config_file, config_dir)
        else:
            config = from_legacy(REDIS_CFG, entraid=is_entraid_auth_enabled())
        apply_api_key(config, api_key)
        if http_port is not None:
            config.port = http_port
        if transport == "http" and not config.port:
            config.port = 8080
        server = RedisMCPServer(config=config, transport=transport)
    except ConfigError as e:
        _fail(str(e))
    server.run()


def main():
    """Entry point without CLI parsing: REDIS_MCP_CONFIG / REDIS_MCP_CONFIG_DIR select the multi-cluster
    config, otherwise the REDIS_* environment variables describe a single cluster. REDIS_MCP_API_KEY sets the
    HTTP API key."""
    config_file = os.getenv("REDIS_MCP_CONFIG")
    config_dir = os.getenv("REDIS_MCP_CONFIG_DIR")
    try:
        config = None
        if config_file or config_dir:
            config = load_config(
                Path(config_file) if config_file else None,
                Path(config_dir) if config_dir else None,
            )
        else:
            config = get_config()
        apply_api_key(config, os.getenv("REDIS_MCP_API_KEY"))
        server = RedisMCPServer(config=config)
    except ConfigError as e:
        _fail(str(e))
    server.run()


if __name__ == "__main__":
    main()
