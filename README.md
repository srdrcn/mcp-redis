# Redis MCP Server

<!-- mcp-name: io.github.redis/mcp-redis -->

[![Integration](https://github.com/redis/mcp-redis/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/redis/mcp-redis/actions/workflows/ci.yml)
[![PyPI - Version](https://img.shields.io/pypi/v/redis-mcp-server)](https://pypi.org/project/redis-mcp-server/)
[![Python Version](https://img.shields.io/badge/python-3.14%2B-blue&logo=redis)](https://www.python.org/downloads/)
[![MIT licensed](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE.txt)
[![Verified on MseeP](https://mseep.ai/badge.svg)](https://mseep.ai/app/70102150-efe0-4705-9f7d-87980109a279)
[![Docker Image Version](https://img.shields.io/docker/v/mcp/redis?sort=semver&logo=docker&label=Docker)](https://hub.docker.com/r/mcp/redis)
[![codecov](https://codecov.io/gh/redis/mcp-redis/branch/master/graph/badge.svg?token=yenl5fzxxr)](https://codecov.io/gh/redis/mcp-redis)


[![Discord](https://img.shields.io/discord/697882427875393627.svg?style=social&logo=discord)](https://discord.gg/redis)
[![Twitch](https://img.shields.io/twitch/status/redisinc?style=social)](https://www.twitch.tv/redisinc)
[![YouTube](https://img.shields.io/youtube/channel/views/UCD78lHSwYqMlyetR0_P4Vig?style=social)](https://www.youtube.com/redisinc)
[![Twitter](https://img.shields.io/twitter/follow/redisinc?style=social)](https://twitter.com/redisinc)
[![Stack Exchange questions](https://img.shields.io/stackexchange/stackoverflow/t/mcp-redis?style=social&logo=stackoverflow&label=Stackoverflow)](https://stackoverflow.com/questions/tagged/mcp-redis)

## Overview
The Redis MCP Server is a **natural language interface** designed for agentic applications to efficiently manage and search data in Redis. It integrates seamlessly with **MCP (Model Content Protocol) clients**, enabling AI-driven workflows to interact with structured and unstructured data in Redis. Using this MCP Server, you can ask questions like:

- "Store the entire conversation in a stream"
- "Cache this item"
- "Store the session with an expiration time"
- "Index and search this vector"

## Multi-cluster mode (fork)

This fork turns the server into an MCP server for a whole fleet of Redis deployments. It follows the operating model
of [openshift-mcp-server](https://github.com/openshift/openshift-mcp-server) and of its sibling kafka-mcp: one YAML
config file, tools grouped into toolsets, a read-only switch, and Streamable HTTP or stdio as transport.

**Highlights**

- **Many clusters, one server**: standalone servers, Redis Cluster and Sentinel-managed primaries side by side.
  Every tool that works on Redis takes a `cluster` parameter whose enum lists exactly the clusters the tool may run
  on. When only one cluster is configured, the parameter is optional.
- **54 tools in 6 toolsets**: `core`, `data`, `json`, `search`, `pubsub`, `docs`.
- **Read-only in the example config**: `readOnly: true` hides every write tool (one sandbox cluster opts back in),
  and `disableDestructive: true` hides the delete/pop/remove tools.
- **Passwords stay out of the config**: each cluster's password comes from a mounted Secret file
  (`<passwordsDir>/<name>_pass`) or an environment variable (`<name>_pass`). `hosts` and `options` cannot carry
  one either.
- **Bounded calls**: every tool call has a 15 s limit. The Redis clients use 5 s connect and 10 s socket timeouts
  and send every command once: there are no client retries, so the server never sends a write twice. A
  standalone server, a Sentinel-managed primary or a Redis Cluster node that stops answering therefore reports its
  own error (`Timeout reading from socket`) after 10 s. A Sentinel that does not answer costs 2 s. Redis
  Cluster startup nodes that do not answer while the client reads the slot map cost one timeout each and can still
  add up to the 15 s limit (see [Time limits, retries and concurrency](#time-limits-retries-and-concurrency)).
- **Clusters are isolated**: each cluster has its own 8 workers for tool calls and its own connection pools (16
  connections for the tools, 50 for pub/sub subscriptions), so a cluster that hangs does not slow down calls to
  the others, and open subscriptions do not take the connections the other tools need.
- **Simple to operate**: a single static API key (in the config, or from a Secret through `REDIS_MCP_API_KEY`),
  JSON logs with one line per tool call, a `/healthz` endpoint, a non-root image that runs on a read-only root
  filesystem, and kustomize manifests for OpenShift (`restricted-v2` SCC).
- **Legacy mode kept**: without `--config`, the `REDIS_*` environment variables and the connection flags
  describe a single cluster named `default` with every toolset, as described in the rest of this README (from
  [Table of Contents](#table-of-contents) on). Because it is the only cluster, the tools' `cluster` parameter is
  optional, so calls without it keep working. One combination is now refused at startup: `--cluster-mode` (or
  `REDIS_CLUSTER_MODE`) together with `--ssl` (`REDIS_SSL`) and `--ssl-ca-path` (`REDIS_SSL_CA_PATH`) stops with
  `redis-mcp: --ssl-ca-path / REDIS_SSL_CA_PATH is not supported with --cluster-mode (redis-py's RedisCluster
  ignores it); use --ssl-ca-certs / REDIS_SSL_CA_CERTS` (exit code 2), because redis-py's `RedisCluster` ignores
  the CA directory and would verify the nodes against the system CAs only. Use `--ssl-ca-certs`
  (`REDIS_SSL_CA_CERTS`) with a CA bundle file instead.

Contents: [Quick start](#quick-start) · [Command line](#command-line) ·
[Configuration file](#configuration-file) · [Passwords](#passwords) ·
[Redis ACL user per cluster](#redis-acl-user-per-cluster) · [Drop-in files](#drop-in-files---config-dir) ·
[Toolsets](#toolsets) · [Which tools are listed](#which-tools-are-listed) ·
[Time limits, retries and concurrency](#time-limits-retries-and-concurrency) ·
[Transports and security](#transports-and-security) · [JSON logs](#json-logs) ·
[Container image](#container-image) · [OpenShift deployment](#openshift-deployment) ·
[Claude Code](#claude-code) · [Troubleshooting](#troubleshooting)

### Quick start

```bash
uv sync                                          # creates .venv with the redis-mcp-server command
cp redis-mcp.example.yaml redis-mcp.local.yaml   # git-ignored; set apiKey and the clusters
export cbdevrds_pass='<password>'                # one <cluster>_pass variable per cluster (see Passwords)
uv run redis-mcp-server --config redis-mcp.local.yaml
```

The server refuses to start while `apiKey` still holds the `<api-key>` placeholder
(`redis-mcp: Invalid config: - apiKey: apiKey still holds a placeholder ...`, exit code 2): set a real key, for
example from `openssl rand -hex 32`, or an empty value (`apiKey: ""`) to turn authentication off for a local run.
To keep the key out of the file, delete the `apiKey` line and export `REDIS_MCP_API_KEY` instead (see
[Command line](#command-line)). The example config sets `port: 8080`, so the server listens on
`http://localhost:8080/mcp`:

```bash
curl -s localhost:8080/healthz                   # {"status":"UP"}
curl -s localhost:8080/mcp -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -H "X-Authorization: $REDIS_MCP_KEY" -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'
```

To run over stdio instead (for a local MCP client that starts the process itself), add `--transport stdio`.

**With Docker.** Use the published image `chosenwar/redis-mcp` (see [Container image](#container-image)) or build
your own with `docker build -t redis-mcp:latest .`, and mount the config and a directory with one `<cluster>_pass`
file per cluster (keep it outside the repository):

```bash
docker run --rm -p 8080:8080 --read-only --tmpfs /tmp \
  -v "$PWD/redis-mcp.local.yaml:/etc/redis-mcp/config/redis-mcp.yaml:ro" \
  -v "$HOME/redis-mcp-passwords:/etc/redis-mcp/passwords:ro" \
  chosenwar/redis-mcp:latest /app/.venv/bin/redis-mcp-server --config /etc/redis-mcp/config/redis-mcp.yaml
```

- The image's default command (`/app/.venv/bin/python src/main.py`) also works: pass
  `-e REDIS_MCP_CONFIG=/etc/redis-mcp/config/redis-mcp.yaml` instead of the command. Without that variable it runs
  the legacy single-cluster mode from the `REDIS_*` variables.
- To keep the API key out of the mounted file, delete `apiKey` there and add `-e REDIS_MCP_API_KEY` (which passes
  the variable from your shell).
- The process runs as UID 1001 in group 0 and must be able to read the mounted files. Files with mode `600` owned
  by your user are not readable by UID 1001; run the container as your UID with `--user "$(id -u):0"`.
- Inside the container `localhost` is the container itself. To reach a Redis on the host, use `--network host`
  (Linux; then `-p` is not needed) or the host's address.

### Command line

| Option | Environment variable | Meaning |
|---|---|---|
| `--config <file>` | `REDIS_MCP_CONFIG` | Main config file (YAML). |
| `--config-dir <dir>` | `REDIS_MCP_CONFIG_DIR` | Directory of drop-in `.yaml`/`.yml` files merged over the main config (see [Drop-in files](#drop-in-files---config-dir)). Can be used without `--config`. |
| `--transport stdio\|http` | | Transport. Defaults to `http` when an HTTP port is set (> 0) and to `stdio` otherwise. `--transport http` without a port uses 8080. |
| `--api-key <key>` | `REDIS_MCP_API_KEY` | API key that HTTP clients must send in `X-Authorization`. Overrides `apiKey` from the config, also in the legacy mode. An empty value is ignored, so an empty variable cannot turn authentication off. A value of only whitespace stops the server (`redis-mcp: --api-key / REDIS_MCP_API_KEY is blank; set a key, or leave it empty or unset to keep apiKey from the config`); any other value is checked like `apiKey` (`redis-mcp: --api-key / REDIS_MCP_API_KEY: apiKey has leading or trailing whitespace ...`). Prefer the variable: a command line can be read by other users (`ps`) and anyone who can read the Deployment. |
| `--http-port <port>` | | HTTP port (1–65535). Overrides `port` from the config. (`--port` is the Redis port of the legacy mode.) |

With `--config` or `--config-dir`, the clusters come from the config only: the legacy connection flags (`--url`,
`--host`, `--port`, `--db`, `--username`, `--password`, `--ssl*`, `--cluster-mode`) are rejected, for example
`redis-mcp: --host cannot be combined with --config/--config-dir; define the clusters in the config file`, and the
`REDIS_HOST`/`REDIS_PORT`/... variables are not used. An empty `--url` and the literal `--url '${REDIS_URL}'`
(which gemini-cli passes when `REDIS_URL` is not set) count as not given. The `--entraid-*` flags and
`REDIS_ENTRAID_*` variables still apply to clusters with `entraid: true`.

The process writes `redis-mcp: ...` to stderr and exits with code 2 for: an invalid or unreadable config (also
one that is not UTF-8: `Could not read config file: <path> (not UTF-8: invalid byte on line <n>)`; a UTF-8 byte
order mark is accepted) or password file, a blank or invalid `--api-key`, a legacy flag combined with `--config`,
an unknown toolset, and a cluster with `entraid: true` but no Entra ID auth flow. A malformed option value gets
click's usage error, also with exit code 2. `python src/main.py` (the image's default command) takes no options
and reads `REDIS_MCP_CONFIG`/`REDIS_MCP_CONFIG_DIR` and `REDIS_MCP_API_KEY` from the environment.

### Configuration file

This is the content of `redis-mcp.example.yaml` without its header comment
(`deploy/openshift/redis-mcp.yaml.example` is identical). Never commit real values: keep them in
`redis-mcp.local.yaml` or `redis-mcp.yaml` (both git-ignored) or in the OpenShift ConfigMap.

```yaml
port: 8080
# X-Authorization value: a long random key (openssl rand -hex 32), or "" for no auth. Better, keep the key out of
# this file: delete the line and set REDIS_MCP_API_KEY (on OpenShift from a Secret, see deployment.yaml).
apiKey: <api-key>
readOnly: true
disableDestructive: true
toolsets: [core, data]
passwordsDir: /etc/redis-mcp/passwords   # <name>_pass files (the redis-mcp-passwords Secret); else env <name>_pass

clusters:
  - name: cbdevrds
    mode: standalone
    hosts: cbdevrds.example.com:6379   # host:port only: no redis:// scheme, no user:password@
    db: 0
    username: mcp-reader
  - name: cbtstrds
    mode: standalone
    hosts: cbtstrds.example.com:6379
    username: mcp-reader
  - name: cbprdrds
    mode: cluster                  # Redis Cluster: startup nodes, the client discovers the rest
    hosts: [cbprdrds-1.example.com:6379, cbprdrds-2.example.com:6379, cbprdrds-3.example.com:6379]
    username: mcp-reader
  - name: cbsbxrds
    mode: standalone
    hosts: cbsbxrds.example.com:6379
    username: mcp-writer           # an ACL user that may write; the sandbox is the only writable cluster
    readOnly: false
    # modules: [json, search]      # lists the json/search tools for this cluster (Redis 8, Redis Stack)
    # tls:                         # or `tls: true` for TLS with the system CAs
    #   caFile: /etc/redis-mcp/tls/ca.crt   # caPath is not supported with mode: cluster
    # options:                     # client tuning; keys and values depend on the mode (README, "Cluster options")
    #   socket_timeout: 5          # seconds, > 0 and <= 3600
  # A primary managed by Redis Sentinel. A Sentinel that does not answer costs 2 s. The Sentinels' own
  # password is <name>_sentinel_pass, so no other cluster may be named cbuatrds_sentinel:
  # - name: cbuatrds
  #   mode: sentinel
  #   hosts: [cbuatrds-s1.example.com:26379, cbuatrds-s2.example.com:26379, cbuatrds-s3.example.com:26379]
  #   masterName: mymaster
  #   username: mcp-reader
```

#### Top-level keys

| Key | Type | Default | Meaning |
|---|---|---|---|
| `port` | integer (0–65535) | not set | HTTP port. When it is set (> 0) and `--transport` is not given, the server runs Streamable HTTP, otherwise stdio. `--http-port` overrides it. |
| `apiKey` | string | not set | Static API key. HTTP requests to `/mcp` must send it in the `X-Authorization` header. Not set or empty (`""`) turns the check off. A non-empty key must not be blank, must not start or end with whitespace and must not be a `<...>` or `${...}` placeholder, otherwise the server does not start; a key shorter than 16 characters only logs a warning. A non-empty `--api-key` / `REDIS_MCP_API_KEY` overrides it (the config is still checked first, so a placeholder left here stops the server even then). |
| `readOnly` | boolean | `false` | `true` hides every write tool on clusters that do not override it with their own `readOnly`. |
| `disableDestructive` | boolean | `false` | `true` hides the destructive tools (`delete`, `hdel`, `lpop`, `rpop`, `lrem`, `srem`, `zrem`, `xdel`, `xgroup_destroy`, `json_del`) on every cluster. It is global only and does not affect the other write tools. |
| `toolsets` | list of strings | `[core]` | Toolsets to enable, which also sets the order of `tools/list`. An unknown name stops the server at startup; an empty or duplicate entry is an invalid config. |
| `passwordsDir` | path | `/etc/redis-mcp/passwords` | Directory of the `<name>_pass` password files (see [Passwords](#passwords)). |
| `clusters` | list | (required) | The Redis deployments. At least one is required, and names must be unique. |

The defaults in the code are permissive (`readOnly: false`, `disableDestructive: false`). The example config turns
both on.

YAML numbers keep the text they are written with wherever a string is expected, so `apiKey: 0123456701234567` or
`username: 0123` are read exactly as written (plain YAML 1.1 would turn them into octal numbers); `port` and `db`
are converted to numbers. In `options`, values that look like integers or decimals are passed to redis-py as
numbers, quoted or not, except `client_name`, which stays text (`client_name: 0123` is the name `0123`); every
option value is then checked (see [Cluster options](#cluster-options)). Unknown keys are errors, and the error
message names the field (`clusters[0].password: Extra inputs are not permitted`). These messages do not repeat
the rejected value, so a password typed into the wrong field does not reach the log.

#### Cluster keys

| Key | Type | Default | Meaning |
|---|---|---|---|
| `name` | string | (required) | Unique name of 1–63 characters from `A-Z a-z 0-9 _ . -`, starting with a letter or digit. It becomes a value of the `cluster` enum, and it names the password file or variable (`<name>_pass`). A cluster must not be named `<x>_sentinel` while a `mode: sentinel` cluster `<x>` exists, because `<x>_sentinel_pass` would be both its password and the Sentinel password of `<x>` (`cluster name <x>_sentinel clashes with the Sentinel password of cluster <x> (<x>_sentinel_pass); rename one of them`). |
| `mode` | `standalone`, `cluster` or `sentinel` | `standalone` | `standalone`: `redis.Redis`. `cluster`: `redis.cluster.RedisCluster` (Redis Cluster / OSS cluster API). `sentinel`: `redis.sentinel.Sentinel(...).master_for(masterName)`, always the current primary. |
| `hosts` | string or list | (required) | `host:port` entries, as a comma-separated string or a YAML list. IPv6 addresses are written `[addr]:port` (quote them in YAML). `standalone` takes exactly one; `cluster` takes the startup nodes (the client reads the slot map from them and discovers the other nodes); `sentinel` takes the Sentinels. A value that contains a scheme (`://`) or credentials (`@`) anywhere is rejected with `hosts takes host:port entries without a scheme or credentials (user:password@); passwords come from the passwords directory`. Other errors name the entry by its position (1 for the first) and never repeat it, since it may hold a password: `hosts entry 2 is not host:port (or [IPv6 address]:port)` (also for an empty entry or a space inside the host) and `hosts entry 2: the port must be between 1 and 65535`. Prefer fully qualified host names (see [DNS lookups](#dns-lookups)). |
| `db` | integer (≥ 0) | `0` | Database number (`standalone`, `sentinel`). A Redis Cluster only has database 0, so `db` other than 0 is rejected with `mode: cluster`. |
| `username` | string | not set | ACL user (redis-py `username`). Without it, the password authenticates the `default` user (`requirepass`). Cannot be combined with `entraid`. |
| `masterName` | string | not set | Name of the monitored primary. Required with `mode: sentinel`, rejected with the other modes. |
| `tls` | `true` or map | not set (no TLS) | TLS for the connections to Redis (redis-py `ssl=True` plus the settings below). `true` uses the defaults: the server certificate is verified against the system CAs, including the host name. With `mode: sentinel` the same settings also apply to the Sentinel connections. With `mode: cluster`, `caPath` is not supported. |
| `readOnly` | boolean | global `readOnly` | Per-cluster override of `readOnly`, in either direction. |
| `modules` | list of `json`, `search` | `[]` | Modules the cluster has. The `json` and `search` toolsets offer their tools only on clusters that list the module. |
| `entraid` | boolean | `false` | Authenticate with the Entra ID credential provider (settings from the `--entraid-*` flags / `REDIS_ENTRAID_*` variables, see [EntraID Authentication](#entraid-authentication-for-azure-managed-redis)) instead of a password. It needs an Entra ID auth flow (`--entraid-auth-flow` / `REDIS_ENTRAID_AUTH_FLOW`): without one the server does not start (`redis-mcp: clusters <names> set entraid: true, but no Entra ID auth flow is configured (REDIS_ENTRAID_AUTH_FLOW or --entraid-auth-flow)`) instead of connecting without authentication. |
| `options` | map | `{}` | Client tuning for redis-py, limited to the keys of [Cluster options](#cluster-options) for the cluster's mode. Applied after the defaults, the credentials and the TLS settings. |

#### Cluster options

`options` accepts only the redis-py arguments below. Addresses, `db`, `username` and `tls` have their own keys,
passwords come from the passwords directory, and the server sets `decode_responses` and `retry` itself. Any other
key, or a key of another mode, stops the server at startup:

```
redis-mcp: Invalid config:
 - clusters[0]: options password, retry_on_timeout not supported in standalone mode (supported: socket_timeout, socket_connect_timeout, socket_keepalive, max_connections, client_name, protocol, health_check_interval; addresses, db, username and tls have their own keys, passwords come from the passwords directory)
```

The values are checked as well. A value outside the rule in the "Value" column, `null` included (a `null`
timeout would remove the time bound), stops the server with `options.<key> must be <rule>`, for example
`clusters[0]: options.socket_timeout must be a number of seconds > 0 and <= 3600`.

| Option | Modes | Default | Value | Meaning |
|---|---|---|---|---|
| `socket_timeout` | all | `10` | a number of seconds > 0 and <= 3600 | Seconds to wait for a reply. With `mode: sentinel` the Sentinel connections use it too, but at most 2 s. |
| `socket_connect_timeout` | all | `5` | a number of seconds > 0 and <= 3600 | Seconds to wait for a connection. With `mode: sentinel` the Sentinel connections use it too, but at most 2 s. The host name lookup is not part of it (see [DNS lookups](#dns-lookups)). |
| `socket_keepalive` | all | off | `true` or `false` | TCP keepalive on the connections to Redis. |
| `max_connections` | all | `16` | an integer >= 1 | Size of the connection pool of the tools' client (with `mode: cluster`, per node). Keep it above the 8 calls a cluster runs at a time, or calls fail with `Too many connections`. Pub/sub subscriptions have a client of their own with 50 connections, which this option does not change. |
| `client_name` | all | not set | a name without spaces (not empty) | Connection name set with `CLIENT SETNAME`, shown by `CLIENT LIST`. Kept as text, also when it looks like a number. |
| `protocol` | all | `2` | `2` or `3` | RESP protocol version. |
| `health_check_interval` | `standalone`, `sentinel` | `0` (off) | a number of seconds >= 0 | A connection that was idle for longer than this many seconds is checked with `PING` before it is used. |
| `read_from_replicas` | `cluster` | `false` | `true` or `false` | Send read commands to the replicas as well (round robin); replicas can return slightly older data. redis-py 6 marks the argument as deprecated. |
| `require_full_coverage` | `cluster` | `true` | `true` or `false` | `false` also accepts a cluster whose hash slots are not all covered. |
| `reinitialize_steps` | `cluster` | `5` | an integer >= 0 | Number of `MOVED` redirects after which the whole slot map is read again (`1`: after every one, `0`: never). |
| `dynamic_startup_nodes` | `cluster` | `true` | `true` or `false` | After connecting, use the discovered nodes as startup nodes for later slot map refreshes. Set `false` when `hosts` is a DNS name or load balancer address that `CLUSTER SLOTS` does not list. |

`health_check_interval` is not accepted with `mode: cluster`, and the last four keys only with `mode: cluster`,
because redis-py would drop or reject them there.

#### TLS keys

| Key | Type | Default | redis-py argument | Meaning |
|---|---|---|---|---|
| `caFile` | path | not set | `ssl_ca_certs` | CA bundle (PEM) trusted in addition to the system CAs. |
| `caPath` | path | not set | `ssl_ca_path` | Directory of CA certificates. Not supported with `mode: cluster`, because redis-py's `RedisCluster` ignores it and would verify the nodes against the system CAs only: the server does not start (`tls.caPath is not supported in cluster mode; use caFile`). |
| `certFile` | path | not set | `ssl_certfile` | Client certificate (mutual TLS). |
| `keyFile` | path | not set | `ssl_keyfile` | Key of the client certificate. |
| `certReqs` | `required`, `optional` or `none` | `required` | `ssl_cert_reqs` | Verification of the server certificate. `none` also turns the host name check off. |
| `checkHostname` | boolean | not set (redis-py default: on) | `ssl_check_hostname` | Match the server certificate against the host name in `hosts`. |

#### Redis clients

The client arguments of each cluster are built in this order (later entries win):

```python
# 1. defaults
decode_responses=True or False          # chosen by the tool
lib_name="redis-py(mcp-server_v<version>)"
socket_connect_timeout=5
socket_timeout=10
# 2. credentials: username and the <name>_pass password, or credential_provider with entraid: true
# 3. tls: ssl=True, ssl_cert_reqs, ssl_ca_certs, ssl_ca_path, ssl_certfile, ssl_keyfile, ssl_check_hostname
# 4. the cluster's options: map (only the keys of "Cluster options")
max_connections=16                      # tools' client, only if options does not set it
max_connections=50                      # pub/sub client, always
# 5. set by the server (options cannot change it)
retry=Retry(NoBackoff(), 0)             # one attempt per command, in every mode (also for the Sentinels)
```

- A cluster has up to three clients: one for the tools, one with `decode_responses=False` for
  `get_vector_from_hash` (which reads raw bytes), and one for pub/sub subscriptions. Each is created on first use
  and then reused. Each client has its own lock, so a cluster that is slow to connect does not hold up calls for
  the others.
- A failed creation leaves no client behind, but its error is remembered for 5 s: calls in that time, including
  those that were waiting for the creation, get the same error at once, and the first call after it tries again.
  Only a Redis Cluster client connects while it is created (it reads the slot map), so this mostly spares a burst
  of calls to an unreachable Redis Cluster from repeating the slow connection attempt one after another.
- The tools' client has a pool of up to 16 connections (`max_connections`): room for the 8 calls a cluster runs
  at a time and the `clusters_list` checks. Pub/sub subscriptions hold their connection until `unsubscribe`, so
  they use a client of their own with up to 50 connections (with `mode: cluster`, per node), the most
  subscriptions the server allows in total. Open subscriptions therefore never take the connections the other
  tools need.
- **Sentinel**: the Sentinel connections use the cluster's socket timeouts (including overrides from `options`),
  but at most 2 s each, its `tls` settings, one attempt per command and the password `<name>_sentinel_pass` (no
  username). The other `options` apply only to the connections to the primary. When no Sentinel names the
  primary, the error lists each Sentinel that could not be asked as `<host>:<port> - <error type>: <message>`,
  for example `No master found for 'mymaster' : 10.0.0.5:26379 - TimeoutError: Timeout connecting to server`.
  (redis-py's own message would show the Sentinel clients' connection arguments, the Sentinel password included.)
- What a single attempt per command means for restarts, failovers and slow or unreachable clusters is described
  in [Time limits, retries and concurrency](#time-limits-retries-and-concurrency).

### Passwords

Passwords are never part of the YAML: there is no password key, `options` does not accept `password`, and `hosts`
rejects `user:password@`. At startup, for each cluster `<name>`, the server reads:

1. the file `<passwordsDir>/<name>_pass` (for example `/etc/redis-mcp/passwords/cbdevrds_pass`) if it exists,
2. otherwise the environment variable `<name>_pass`.

Trailing line breaks are removed from a file's content; everything else, including spaces, is part of the password.
A file that is not UTF-8 stops the server (`Could not read password file <path> (not UTF-8)`). With
`mode: sentinel`, the password of the Sentinels themselves is read the same way from `<name>_sentinel_pass` (this
is why no other cluster may be named `<name>_sentinel`). A cluster without a password (and without
`entraid: true`) connects without AUTH, and the server logs a warning at startup:

```
cluster cbdevrds: no password (/etc/redis-mcp/passwords/cbdevrds_pass or env cbdevrds_pass); connecting without AUTH
```

The passwords are read only at startup. The `auth` field of `clusters_list` shows `password`, `none` or `entraid`
instead of the password.

On OpenShift, the Secret `redis-mcp-passwords` is created from a `passwords.env` file
(template: `deploy/openshift/passwords.env.example`) and mounted at `/etc/redis-mcp/passwords`, one file per key:

```bash
cp deploy/openshift/passwords.env.example passwords.env    # git-ignored; one <cluster>_pass=<password> line per cluster
oc create secret generic redis-mcp-passwords --from-env-file=passwords.env
```

In an env file the value is everything after the first `=`: quotes are kept as part of the value, and so is a
`# comment` at the end of a line. Lines that start with `#` are ignored. A key must not start with a digit; for
such a cluster name use `--from-literal=<name>_pass=<password>` instead. For a local run, `export
cbdevrds_pass=...` works in bash (names with `-` or `.` cannot be exported from a shell; use files then).

### Redis ACL user per cluster

`readOnly` only decides which tools are listed; it does not restrict what the server's Redis user may do. Give the
server its own ACL user on every cluster, with only the rights it needs. A read-only user for the `core` and `data`
toolsets:

```
ACL SETUSER mcp-reader on >REPLACE_ME ~* &* +@read +@connection -@dangerous +info +client|list +cluster|slots
```

and, on a writable cluster such as the sandbox, a user that may also write but still cannot flush, reconfigure or
shut down the server:

```
ACL SETUSER mcp-writer on >REPLACE_ME ~* &* +@read +@write +@connection -@dangerous +info +client|list +cluster|slots
```

- The rules are applied from left to right. `-@dangerous` must come **before** `+info +client|list`: `INFO` and
  `CLIENT LIST` (used by the `info`, `client_list` and `clusters_list` tools) are in the `@dangerous` category, so a
  `-@dangerous` at the end would remove them again. `-@dangerous` also removes `KEYS`, `FLUSHALL`, `FLUSHDB`,
  `CONFIG`, `SHUTDOWN` and `CLIENT KILL`.
- `+cluster|slots` is needed on a Redis Cluster: redis-py reads the slot map when it connects, and without this
  command the connection fails with `Cluster mode is not enabled on this node`. It does no harm elsewhere.
- Checked on Redis 7.0 with the server's tools and `ACL DRYRUN`: `mcp-reader` can run `get`, `hgetall`, `scan_keys`,
  `scan_all_keys`, `type`, `dbsize`, `info`, `client_list` and `clusters_list`, and `set`/`delete` fail with
  `this user has no permissions to run the 'set' command`; `mcp-writer` can also run `set`, `delete`, `expire`,
  `rename` and the stream group commands, but not `FLUSHALL`, `FLUSHDB`, `KEYS`, `CONFIG` or `SHUTDOWN`.
- The `pubsub` toolset needs `+subscribe +psubscribe +unsubscribe +punsubscribe` (and `+publish` for the write
  tool `publish`): pub/sub commands are not in `@read` or `@write`.
- The `json` and `search` tools use module commands (`JSON.GET`, `JSON.SET`, `JSON.DEL`, `FT._LIST`, `FT.INFO`,
  `FT.SEARCH`, `FT.CREATE`). Their ACL categories depend on the Redis/module version, so check them with
  `ACL DRYRUN mcp-reader JSON.GET somekey` and add the commands explicitly if needed.
- ACL users are per node: on a Redis Cluster or a Sentinel setup, create the user on every node, replicas
  included (or use an ACL file), so that it still exists after a failover. Persist it with `ACL SAVE` (with an
  `aclfile`) or `CONFIG REWRITE`.
- Set `username: mcp-reader` (or `mcp-writer`) on the cluster in the config, and put the password in
  `<name>_pass`.

### Drop-in files (`--config-dir`)

- The server reads the regular `*.yaml` and `*.yml` files directly inside the directory. Hidden files are skipped
  and subdirectories are not read, so the directory can be a mounted ConfigMap. The files are sorted by name and
  merged over `--config` one after another.
- Maps are merged deeply. `clusters` is merged **by `name`**: an entry with an existing name overrides only the
  fields it sets (its `tls` and `options` maps are merged key by key), and an entry with a new name is appended.
  Every other list, such as `toolsets`, `modules` or a `hosts` list, replaces the earlier value entirely. Scalars
  replace the earlier value.
- Validation runs after the merge.

```yaml
# conf.d/10-sandbox.yaml: the sandbox runs Redis 8 with the JSON and search modules
clusters:
  - name: cbsbxrds
    modules: [json, search]
```

```yaml
# conf.d/20-toolsets.yaml: replaces the whole toolsets list
toolsets: [core, data, json, search]
```

The config is read once at startup, so restart the server to apply changes.

### Toolsets

| Toolset | Tools | Read | Write | Destructive | Available on |
|---|---:|---|---|---|---|
| `core` (enabled by default) | 10 | `clusters_list`, `type`, `scan_keys`, `scan_all_keys`, `dbsize`, `info`, `client_list` | `expire`, `rename` | `delete` | Every cluster. `clusters_list` takes an optional `cluster` (all clusters when omitted) and `check` (default `true`: connect and report status, latency, Redis version, role and, for a Redis Cluster, the number of primaries). |
| `data` | 29 | `get`, `hget`, `hgetall`, `hexists`, `get_vector_from_hash`, `lrange`, `llen`, `smembers`, `zrange`, `xrange` | `set`, `hset`, `set_vector_in_hash`, `lpush`, `rpush`, `sadd`, `zadd`, `xadd`, `xgroup_create`, `xreadgroup`, `xack` | `hdel`, `lpop`, `rpop`, `lrem`, `srem`, `zrem`, `xdel`, `xgroup_destroy` | Every cluster |
| `json` | 3 | `json_get` | `json_set` | `json_del` | Clusters with `modules: [json]` |
| `search` | 6 | `get_indexes`, `get_index_info`, `get_indexed_keys_number`, `vector_search_hash`, `hybrid_search` | `create_vector_index_hash` | | Clusters with `modules: [search]` |
| `pubsub` | 5 | `subscribe`, `psubscribe`, `read_messages`, `unsubscribe` | `publish` | | Every cluster. `read_messages` and `unsubscribe` take the `subscription_id` and no `cluster`. |
| `docs` | 1 | `search_redis_documents` | | | Not tied to a cluster (no `cluster` parameter). Calls `MCP_DOCS_SEARCH_URL` (default `https://redis.io/convai/api/docs/search`), so the pod needs egress to it. |
| **Total** | **54** | **28** | **16** | **10** | |

Pub/sub subscriptions live in the server process: at most 50 at a time over all clusters (one more gets `Too many
active subscriptions. Close unused subscriptions and try again.`), and a subscription that was not read for 5
minutes is closed when the next one is created. They use the cluster's pub/sub client, not the connections of the
other tools (see [Redis clients](#redis-clients)). `read_messages` waits up to `timeout_ms` (default 1000, at most
5000; `0` does not wait) and returns once `max_messages` messages (default 10, at most 100) have arrived or the
time is up; a call right after `subscribe` waits like any other. Subscriptions are lost when the pod restarts, and
`read_messages` then answers `Subscription '<id>' was not found`; this is why the Deployment runs a single replica.

On a Redis Cluster, `dbsize` adds up the keys of every primary, and `scan_all_keys` scans every primary until each
one is done. `scan_keys` runs one `SCAN` step on every primary at once (`count` applies to each of them): `keys`
holds what all of them returned, `cursor` is a map from `<host>:<port>` to that primary's next cursor, and
`scan_complete` is always `false`, even when every cursor is 0. Its `cursor` argument is a single number that is
sent to every primary, so `scan_keys` cannot page through a Redis Cluster: use `scan_all_keys` with a narrow
`pattern` there (it still has to finish within the 15 s limit).

### Which tools are listed

For every tool, the server works out which clusters it may run on:

1. Its toolset must be in `toolsets`.
2. `json` and `search` tools need a cluster that lists the module in `modules`.
3. **Write** tools (descriptions starting with `WRITE:`) need the cluster's effective `readOnly` to be `false`. The
   cluster's own `readOnly` applies if it is set, otherwise the global one does.
4. **Destructive** tools also need `disableDestructive: false`.

A tool with no eligible cluster is **not listed at all** (calling it anyway returns `Unknown tool: <name>`). Every
other tool is listed with its `cluster` enum **narrowed to the eligible clusters**, and a call that names any other
cluster is rejected by schema validation (`Input should be 'cbsbxrds'`). With two or more clusters configured,
`cluster` is required and comes first, so a call without it is rejected as well (`Field required`). With exactly
one cluster (a one-cluster config file or the legacy mode), `cluster` is optional and comes last, described as
`Cluster name (default: the only configured cluster)`: a call without it runs on that cluster, so calls written
for the original single-cluster server keep working. `clusters_list` always takes an optional `cluster` (all
clusters when omitted), and `read_messages`, `unsubscribe` and `search_redis_documents` take none. With the
example config, 30 of the 54 tools are listed. For example:

| Tool | `cluster` enum |
|---|---|
| `get`, `scan_keys`, `info` (read) | `cbdevrds`, `cbtstrds`, `cbprdrds`, `cbsbxrds` |
| `set`, `hset`, `expire` (write) | `cbsbxrds` |
| `delete`, `hdel`, `lpop` (destructive) | not listed |
| `json_get`, `get_indexes` | not listed (toolsets `json`/`search` not enabled) |

The `WRITE:` prefix is added to the description of every write and destructive tool. Tools also carry MCP
annotations: `readOnlyHint: true` on read tools and `destructiveHint: true` on destructive tools (both are `false`
on the other write tools).

### Time limits, retries and concurrency

Every tool call runs for at most **15 s**. A call that runs longer returns:

```
Tool did not finish within 15 s (timeout). The cluster may be slow or unreachable (check clusters_list), or the request too large (narrow the pattern or count).
```

The call's worker keeps running until the Redis client gives up, so a write that hit the limit may still be
applied. `clusters_list` checks all clusters in parallel and stops waiting after 13 s; a cluster whose check has
not finished by then shows `status: timeout` (`The connection check did not finish within the time limit`).

**Client timeouts, no retries.** The Redis clients wait 5 s for a connection (`socket_connect_timeout`) and 10 s
for a reply (`socket_timeout`); both can be changed per cluster in [`options`](#cluster-options). Every command is
sent once, in every mode and also to the Sentinels: redis-py's default retry policy (3 retries with backoff,
timeouts included) is replaced by a single attempt. A command whose connection broke or timed out may already
have run, so sending it again could apply a write twice, and retries could outlast the 15 s limit.

- A call whose connection breaks while its command is in flight fails once with the client's error, for example
  `Error while reading from <host>:<port> : (104, 'Connection reset by peer')` when the server was killed, or
  `Timeout reading from socket` when the server or its host went away without closing the connection. So the first
  call after a restart or a failover can fail once; the next call opens a new connection (on Sentinel, to the
  primary the Sentinels name then).
- Connections that broke while they sat idle in the pool are still replaced: the pool checks a connection before
  it hands it out and reconnects when the server has closed it. Measured: after a restart of a standalone server,
  and after `SENTINEL FAILOVER` (the Sentinels close the clients of the old primary), the next call succeeded.
- **Redis Cluster**: after a connection error or a timeout, the client removes that node from its startup nodes
  and reads the slot map again from the others before it reports the error, so the next call uses the new
  topology. `MOVED` and `ASK` redirects are still followed.
- **Sentinel**: each new connection to the primary first asks the Sentinels, in turn, which node is the primary.
  Each Sentinel connection uses connect and reply timeouts of at most 2 s (`socket_connect_timeout` and
  `socket_timeout` of the cluster, capped at 2 s), so a Sentinel that does not answer costs 2 s. A Sentinel that
  answers slowly can take longer: the connection waits up to 2 s for each reply (AUTH, client info, the master
  lookup).

**What a cluster that fails returns.** Measured with the default timeouts, redis-py 6.4 and Redis 7.0, with a
closed port, with a listener whose backlog is full (connections time out, as with a firewall that drops packets),
with a listener that accepts connections but never answers, and with `DEBUG SLEEP` on a running server:

| Failure | `standalone` | `sentinel` | `cluster` |
|---|---|---|---|
| Connection refused | `Error 111 connecting to <host>:<port>. Connection refused.` at once | `No master found for '<masterName>' : <host>:<port> - ConnectionError: Error 111 connecting to <host>:<port>. Connection refused.` (one entry per Sentinel) at once | `Redis Cluster cannot be connected. Please provide at least one reachable node: Error 111 ...` at once |
| Connection times out | `Timeout connecting to server` after 5 s | 2 s per Sentinel: `No master found for '<masterName>' : <host>:<port> - TimeoutError: Timeout connecting to server, ...` after 2 s with one Sentinel, 6 s with three | 5 s per startup node: with one or two nodes `Redis Cluster cannot be connected ...: Timeout connecting to server` after 5 or 10 s, with three or more the 15 s limit |
| Connection accepted, no answer | `Timeout reading from socket` after 10 s | 2 s per Sentinel: `No master found for '<masterName>' : <host>:<port> - TimeoutError: Timeout reading from socket, ...` after 2 s with one Sentinel, 6 s with three | 10 s per startup node: with one node `Redis Cluster cannot be connected ...: Timeout reading from socket` after 10 s, with two or more the 15 s limit |
| Server hangs after the client connected | `Timeout reading from socket` after 10 s | the primary: `Timeout reading from socket` after 10 s | a node: `Timeout reading from socket` after 10 s for keys on that node; keys on the other nodes are not affected |

The Sentinel and startup node rows apply when none of them answers. One that answers ends the search: with a
hanging Sentinel listed before a working one, the first call took 2 s and succeeded, and later calls asked the
working Sentinel first; with a hanging first startup node, the first call to a Redis Cluster took 10 s and
succeeded. A Redis Cluster that cannot be connected is remembered for 5 s (see [Redis clients](#redis-clients)):
calls in that time get the same error at once, and the first call after it tries again.

If startup nodes can become unreachable without refusing connections (a firewall that drops packets), lower the
timeouts of that cluster in `options` so that number of startup nodes × timeout stays below 15 s: with
`socket_timeout: 3` and `socket_connect_timeout: 2`, three hanging startup nodes failed with their own error after
9 s, and a hanging first startup node delayed the first call by 3 s. `socket_timeout` also limits every command on
that cluster, so keep it above the slowest command you expect. Sentinels need no tuning for this: with the 2 s
cap, even five Sentinels that do not answer fail after about 10 s.

**Concurrency.** Tool calls run in worker threads, 8 per cluster. Calls to a cluster beyond 8 at a time wait for
a free worker; the wait counts toward their 15 s, and a call still waiting at the limit is dropped without
running. When only one cluster is configured, a call that leaves out `cluster` runs on that cluster's workers,
like a call that names it. Calls without a cluster (`clusters_list` without `cluster`, `read_messages`,
`unsubscribe`, `search_redis_documents`) share another 8 workers; a `read_messages` call holds its worker for up
to `timeout_ms` when fewer than `max_messages` arrive, so keep `timeout_ms` short when polling many subscriptions.
A cluster that hangs ties up only its own
workers. With 12 simultaneous `get` calls to a standalone cluster that accepted connections but never answered,
8 returned `Timeout reading from socket` after 10 s and the other 4 (which got a worker only after 10 s) the 15 s
timeout message, while `get` calls to another cluster made at the same time answered within about 10 ms.

#### DNS lookups

Host names are resolved before the connection attempt starts, so the lookup is not covered by
`socket_connect_timeout`. It runs for every new connection (the pool keeps and reuses them), for the names in
`hosts` and for names that Sentinel or `CLUSTER SLOTS` return (usually IP addresses). A name that does not
resolve fails quickly (`Error -2 connecting to <host>:<port>. Name or service not known.`), but a slow or
unreachable DNS server can make a lookup take longer than the whole 15 s limit. In a pod, the default `ndots:5`
makes the resolver try a name with fewer than five dots, such as `redis-1.example.com`, with every search domain
of the pod first (`<namespace>.svc.cluster.local`, `svc.cluster.local`, `cluster.local`, ...), each one a DNS
query that has to fail before the real name is asked. For Redis hosts outside the cluster, write fully qualified
names in `hosts` and lower `ndots`: `deploy/openshift/deployment.yaml` has a commented `dnsConfig` with
`ndots: "2"`, which makes names with two or more dots go to DNS as written first; short names such as a Service
name still go through the search domains.

### Transports and security

- **Streamable HTTP** on `/mcp` (stateless, JSON responses). The server handles each request on its own and keeps no
  MCP session, so a pod restart does not break connected clients (pub/sub subscriptions excepted). The health
  endpoint `GET /healthz` returns `{"status":"UP"}`; it does not depend on Redis, so a cluster outage does not
  restart the pod. Use `clusters_list` to check the clusters.
- **stdio**: JSON-RPC on stdin/stdout, with logs on stderr. There is no HTTP listener and no API key check (the
  config is still validated, so `apiKey` must not be the placeholder; use `""` or delete the line).

Security:

- **API key (HTTP only)**: when an API key is set (`apiKey`, or `--api-key` / `REDIS_MCP_API_KEY`, which override
  it), every request to `/mcp` (and anything below it) must carry the header `X-Authorization: <api-key>` with
  exactly that value (compared in constant time). A missing or different value gets **HTTP 401** with
  `{"error": "unauthorized", "message": "X-Authorization header is missing or the API key does not match"}`.
  The header is `X-Authorization`, not `Authorization`, and takes the raw key (no `Bearer` prefix). An empty or
  missing `apiKey` without a `REDIS_MCP_API_KEY` turns the check off. The startup log says `API key authentication
  enabled for /mcp (header X-Authorization)`, or warns `API key authentication DISABLED (apiKey is empty): anyone
  who can reach /mcp can call the tools`. On OpenShift, keep the key in a Secret (see
  [OpenShift deployment](#openshift-deployment)).
- **Browser requests are refused**: a request to `/mcp` with an `Origin` header (that is, one sent by a web page)
  gets **HTTP 403**
  `{"error": "forbidden", "message": "Requests with an Origin header (from a web page) are not accepted"}`, even
  with the right key. This stops a web page from driving the tools through the user's browser (CSRF, DNS
  rebinding). MCP clients such as Claude Code send no `Origin` header; browser-based clients cannot connect.
- `/healthz` is not behind the key, so probes work. It shows no details.
- Run HTTP behind TLS. The OpenShift Route terminates TLS at the edge.
- On the Redis side, the server can do what its ACL user can do (see
  [Redis ACL user per cluster](#redis-acl-user-per-cluster)). `readOnly` controls which tools are listed; it does
  not replace ACLs.
- Keep secrets out of the repository. `redis-mcp.yaml` in the repository root, `*.local.yaml`, `passwords.env`,
  `conf.d/`, and `deploy/openshift/redis-mcp.yaml`/`passwords.env` are git-ignored, and the image build context
  contains only `src/`, `pyproject.toml`, `uv.lock` and `README.md`. Examples use `<...>` placeholders.
- The pod does not mount a service account token (`automountServiceAccountToken: false`), because the server never
  calls the Kubernetes API.

### JSON logs

Over HTTP, logs are JSON on **stdout**, one object per line with `@timestamp`, `level`, `logger`, `message` and any
extra fields, at `INFO` (`MCP_REDIS_LOG_LEVEL` overrides it). The MCP SDK's own loggers are kept at `WARNING` and
uvicorn's access log is off. Every tool call that passes the input schema validation produces one line with `tool`,
`cluster`, `durationMs`, `outcome` (`ok`, `error` or `cancelled`) and `error`, for example:

```json
{"@timestamp": "2026-10-20T06:25:24.482Z", "level": "INFO", "logger": "src.common.registry", "message": "tool call tool=set cluster=cbsbxrds durationMs=4 outcome=ok", "tool": "set", "cluster": "cbsbxrds", "durationMs": 4, "outcome": "ok", "error": null}
```

- `outcome` is `error` when the tool raises or hits the 15 s limit. Most data tools report Redis errors (for
  example a missing ACL permission or `Timeout reading from socket`) in their result text instead, which is logged
  as `ok`. It is `cancelled`, with `error: "cancelled by the client"`, when the client cancels the request before
  it finishes.
- `cluster` is `null` for tools without a cluster, for `clusters_list` without one, and for calls that leave it
  out when only one cluster is configured.
- Calls rejected before they reach the tool (a `cluster` outside the enum, a missing or mistyped parameter, an
  unknown tool) produce no `tool call` line.
- A rejected HTTP request logs `unauthorized request` at `WARNING` with `remoteAddr` and `headerPresent`, never
  the key itself.
- At startup the server logs `<n> tools listed for clusters [...] (readOnly=..., disableDestructive=...)` and a
  warning for each cluster without a password.

Over stdio, stdout carries the JSON-RPC stream, so logs go to stderr in the plain format of the legacy mode, at
`WARNING` by default (see [Logging](#logging)); set `MCP_REDIS_LOG_LEVEL=INFO` to see the `tool call` lines.

### Container image

`Dockerfile` builds on `python:3.14-slim` and installs the locked dependencies into `/app/.venv` with
`uv sync --locked --no-dev`. It is built to run under the OpenShift **restricted-v2** SCC:

- It runs as non-root user 1001. `/app` and `/etc/redis-mcp` are group 0 with `g=u`, so a random UID in group 0
  works.
- It writes nothing at runtime: the bytecode is compiled during the build and `uv` is not used to start the
  server, so the root filesystem can be read-only, with an `emptyDir` mounted at `/tmp`. (`uv run` inside the
  container would fail as a non-root user: `Failed to initialize cache at /.cache/uv`.)
- `CMD ["/app/.venv/bin/python", "src/main.py"]`: with `REDIS_MCP_CONFIG` (or `REDIS_MCP_CONFIG_DIR`) set it runs
  the multi-cluster mode, otherwise the legacy single-cluster mode from the `REDIS_*` variables over stdio, as
  before. The OpenShift Deployment runs `/app/.venv/bin/redis-mcp-server --config
  /etc/redis-mcp/config/redis-mcp.yaml` instead. `/app/.venv/bin` is on `PATH`, so `redis-mcp-server` also works
  as a command. Port 8080 is exposed.
- Mount points: `/etc/redis-mcp/config` (redis-mcp.yaml), `/etc/redis-mcp/passwords` (`<cluster>_pass` files),
  `/etc/redis-mcp/tls` (CA certificates).

The image runs with `--user 12345:0 --read-only --tmpfs /tmp` (a random UID in group 0 and a read-only root
filesystem, as on OpenShift), in both the multi-cluster (HTTP) and the legacy (stdio) mode.

**Published image.** `.github/workflows/docker-push.yml` builds the image and pushes it to Docker Hub as
[`chosenwar/redis-mcp`](https://hub.docker.com/r/chosenwar/redis-mcp):

| Trigger | Tags |
|---|---|
| push to `main` | `latest`, `sha-<short commit>` |
| tag `vX.Y.Z` | `X.Y.Z`, `sha-<short commit>` |
| manual run (Actions → Docker push → Run workflow) | `sha-<short commit>` (plus `latest` on `main`) |

Before pushing, the workflow smoke-tests the image the way OpenShift runs it (`--user 12345:0 --read-only --tmpfs
/tmp`): `--help`, the import, `/healthz`, 401 without the API key and `tools/list` with it. It needs the repository
secrets `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` (a Docker Hub access token with Read & Write scope). To release a
version: `git tag v0.6.0 && git push origin v0.6.0`.

### OpenShift deployment

`deploy/openshift/` is a kustomize directory:

| File | Content |
|---|---|
| `deployment.yaml` | 1 replica (`Recreate`: pub/sub subscriptions and the client caches live in the process). Startup, liveness and readiness probes on `/healthz`. A restricted-v2 security context (non-root, no privilege escalation, all capabilities dropped, `RuntimeDefault` seccomp, read-only root filesystem). No service account token is mounted. A `/tmp` `emptyDir`. Requests of 50m CPU and 128Mi memory, with a 256Mi memory limit (the server uses under 100 MB at idle). The config is mounted at `/etc/redis-mcp/config`, the passwords at `/etc/redis-mcp/passwords` (mode `0440`); an optional CA Secret mount at `/etc/redis-mcp/tls`, the `REDIS_MCP_API_KEY` variable from the Secret `redis-mcp-api-key` and a `dnsConfig` with `ndots: "2"` (see [DNS lookups](#dns-lookups)) are commented out. |
| `service.yaml` | `redis-mcp` Service on port 8080 (`http`). |
| `route.yaml` | Edge TLS (HTTP redirects to HTTPS), `haproxy.router.openshift.io/timeout: 60s`. |
| `kustomization.yaml` | The resources above, labels, and the `images:` entry that sets the image. |
| `redis-mcp.yaml.example`, `passwords.env.example` | Templates for the ConfigMap and the Secret. |

The ConfigMap `redis-mcp-config` (key `redis-mcp.yaml`), the Secret `redis-mcp-passwords` (one `<cluster>_pass`
key per cluster) and the recommended Secret `redis-mcp-api-key` (key `api-key`) hold environment-specific data, so
`oc apply -k` does not create them. You create them with `oc create`.

**0. Select the project** the server runs in. The commands below use the current project:

```bash
oc new-project redis-mcp      # or switch to an existing one: oc project <namespace>
```

**1. Pick the image.** By default `kustomization.yaml` uses the published `docker.io/chosenwar/redis-mcp:latest`
(see [Container image](#container-image)); pin a release tag (`X.Y.Z`) for production. To build your own instead,
push it to a registry the cluster can pull from:

```bash
docker build -t <registry>/<namespace>/redis-mcp:<tag> .     # or podman build
docker push <registry>/<namespace>/redis-mcp:<tag>
```

or build it inside the cluster, into the internal registry (the build pod needs access to Docker Hub and PyPI):

```bash
oc new-build --name redis-mcp --binary --strategy docker
oc start-build redis-mcp --from-repo . --follow   # -> image-registry.openshift-image-registry.svc:5000/<namespace>/redis-mcp:latest
```

`--from-repo` uploads the committed files of the clone, so local config and password files are not sent to the
cluster.

**2. Set the image** in `deploy/openshift/kustomization.yaml` (only needed for a release tag or your own image):

```yaml
images:
  - name: redis-mcp
    newName: docker.io/chosenwar/redis-mcp   # or image-registry.openshift-image-registry.svc:5000/<namespace>/redis-mcp
    newTag: latest                           # or a release, e.g. 0.6.0
```

**3. Prepare the config and the passwords** from the templates. Keep `port: 8080` and
`passwordsDir: /etc/redis-mcp/passwords`, which match the Deployment. Add one `<cluster>_pass` line per cluster to
`passwords.env`.

```bash
cp deploy/openshift/redis-mcp.yaml.example redis-mcp.yaml   # set the clusters (git-ignored)
cp deploy/openshift/passwords.env.example passwords.env     # set the passwords (git-ignored)
```

**The API key** belongs in a Secret of its own (recommended): delete the `apiKey` line from `redis-mcp.yaml`,
uncomment the `env` entry `REDIS_MCP_API_KEY` in `deployment.yaml`, and create the Secret:

```bash
REDIS_MCP_KEY=$(openssl rand -hex 32)
oc create secret generic redis-mcp-api-key --from-literal=api-key="$REDIS_MCP_KEY"
oc extract secret/redis-mcp-api-key --keys=api-key --to=-   # shows the key again later
```

- `REDIS_MCP_API_KEY` overrides `apiKey`, but the config is checked first: a leftover `apiKey: <api-key>`
  placeholder still stops the pod.
- Use `--from-literal` (or a file without a final line break): a key with a trailing line break stops the pod with
  `redis-mcp: --api-key / REDIS_MCP_API_KEY: apiKey has leading or trailing whitespace ...`.
- While the `env` entry is uncommented and the Secret does not exist, the pod does not start
  (`CreateContainerConfigError`).
- Without the Secret, set `apiKey` in `redis-mcp.yaml` to the generated key instead. It then sits in the
  ConfigMap, which anyone who can read ConfigMaps in the namespace can read. With neither, authentication is off
  (`API key authentication DISABLED` in the log).

**4. Create the ConfigMap and the Secret, then apply the manifests:**

```bash
oc create configmap redis-mcp-config --from-file=redis-mcp.yaml
oc create secret generic redis-mcp-passwords --from-env-file=passwords.env
oc apply -k deploy/openshift
oc rollout status deployment/redis-mcp
oc get route redis-mcp
```

For clusters with TLS and a private CA, create a Secret with the CA, uncomment the `tls-ca` volume and its mount in
`deployment.yaml`, and set `tls: {caFile: /etc/redis-mcp/tls/ca.crt}` on those clusters:

```bash
oc create secret generic redis-mcp-tls --from-file=ca.crt=/path/to/ca.crt
```

The Route has no fixed host, so OpenShift generates `redis-mcp-<namespace>.<apps-domain>`. To use
`redis-mcp.<apps-domain>`, uncomment `spec.host` in `route.yaml`. The 60 s router timeout leaves room above the
15 s tool limit.

**Smoke test:**

```bash
curl -s https://redis-mcp.<apps-domain>/mcp \
  -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -H "X-Authorization: $REDIS_MCP_KEY" \
  -d '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"clusters_list","arguments":{}}}'
```

**Updating the config, the passwords or the API key.** They are read only at startup, so replace the object and
restart the pod:

```bash
oc create configmap redis-mcp-config --from-file=redis-mcp.yaml --dry-run=client -o yaml | oc replace -f -
oc create secret generic redis-mcp-passwords --from-env-file=passwords.env --dry-run=client -o yaml | oc replace -f -
REDIS_MCP_KEY=$(openssl rand -hex 32)   # only to change the API key; clients then need the new key
oc create secret generic redis-mcp-api-key --from-literal=api-key="$REDIS_MCP_KEY" \
  --dry-run=client -o yaml | oc replace -f -
oc rollout restart deployment/redis-mcp
```

With `Recreate`, the old pod stops before the new one starts, so there is a short gap, and open pub/sub
subscriptions are lost.

### Claude Code

Add the server to `.mcp.json` (project scope). The key stays in an environment variable and never in the file:

```json
{
  "mcpServers": {
    "redis": {
      "type": "http",
      "url": "https://redis-mcp.<apps-domain>/mcp",
      "headers": {
        "X-Authorization": "${REDIS_MCP_KEY}"
      }
    }
  }
}
```

```bash
export REDIS_MCP_KEY=<api-key>
claude
```

If the Route certificate is signed by a corporate CA, Node.js (which Claude Code runs on) must trust that CA. Point
`NODE_EXTRA_CA_CERTS` to the CA bundle in PEM format before you start `claude`:

```bash
export NODE_EXTRA_CA_CERTS=/path/to/corporate-ca.pem
```

Run `/mcp` inside Claude Code to check the connection. The tools appear as `mcp__redis__<tool>`.

**stdio alternative.** Claude Code can also start the server locally. The clusters must then be reachable from your
machine, and the passwords come from the environment. Pass `--transport stdio` explicitly, because a config with
`port` set would otherwise start HTTP:

```json
{
  "mcpServers": {
    "redis": {
      "type": "stdio",
      "command": "/path/to/mcp-redis/.venv/bin/redis-mcp-server",
      "args": ["--config", "/path/to/redis-mcp.local.yaml", "--transport", "stdio"],
      "env": {
        "cbsbxrds_pass": "${CBSBXRDS_PASS}"
      }
    }
  }
}
```

### Troubleshooting

Start with `clusters_list`: it shows every cluster's mode, hosts, `readOnly`, modules and auth, and connects to each
one to report `status` (`ok`, `error` or `timeout`), `latencyMs`, `redisVersion`, `role` and, for a Redis Cluster,
`primaries`, or the `error`.

| Symptom | Cause and fix |
|---|---|
| `redis-mcp: Invalid config: - apiKey: apiKey still holds a placeholder ...` | `apiKey` was not filled in. Set a real key (`openssl rand -hex 32`), `""` to turn authentication off, or delete the line when the key comes from `REDIS_MCP_API_KEY` (the config is checked even then). |
| `redis-mcp: --api-key / REDIS_MCP_API_KEY is blank; ...` | The key from `--api-key` or `REDIS_MCP_API_KEY` (on OpenShift: the Secret `redis-mcp-api-key`) holds only whitespace. Set a key, or leave the value empty or unset to keep `apiKey` from the config. |
| `redis-mcp: --api-key / REDIS_MCP_API_KEY: apiKey ...` | The key from `--api-key` or `REDIS_MCP_API_KEY` is a `<...>`/`${...}` placeholder, or starts or ends with whitespace, typically the line break of a Secret created with `--from-file`. Recreate the Secret with `--from-literal`. |
| `redis-mcp: Invalid config: - clusters[0]...` | Fix the named key, for example `clusters[0]: a standalone cluster takes exactly one host:port in hosts`, `clusters[0]: masterName is required when mode is sentinel`, `clusters[0].hosts: hosts entry 1 is not host:port (or [IPv6 address]:port)` or `clusters[0].password: Extra inputs are not permitted` (passwords go into `<name>_pass`). |
| `clusters[0]: options <keys> not supported in <mode> mode (supported: ...)` | `options` only takes the keys of [Cluster options](#cluster-options) for the cluster's mode. Addresses, `db`, `username` and `tls` have their own keys; passwords go into `<name>_pass`. |
| `clusters[0]: options.<key> must be <rule>` | The option's value breaks the rule in the "Value" column of [Cluster options](#cluster-options), for example `options.socket_timeout must be a number of seconds > 0 and <= 3600` for `0`, `null` or `1e10`, or `options.client_name must be a name without spaces`. |
| `clusters[0]: tls.caPath is not supported in cluster mode; use caFile` | Put the CA certificates into one PEM bundle and set `caFile`. |
| `redis-mcp: --ssl-ca-path / REDIS_SSL_CA_PATH is not supported with --cluster-mode ...` | The legacy mode's equivalent of the row above: use `--ssl-ca-certs` / `REDIS_SSL_CA_CERTS` with a CA bundle file instead of the CA directory. |
| `clusters[0].hosts: hosts takes host:port entries without a scheme or credentials ...` | `hosts` contains `://` or `@`, that is a URL or `user:password@`. Write plain `host:port` entries, put the user into `username` and the password into `<name>_pass`. |
| `clusters[0].hosts: hosts entry <n> is not host:port (or [IPv6 address]:port)` or `hosts entry <n>: the port must be between 1 and 65535` | Entry `<n>` of `hosts` (1 for the first) is empty (for example after a trailing comma), has no port (or one not written with the digits 0-9), has a space inside the host, or its port is outside 1–65535. Write IPv6 addresses in brackets and quote them in YAML (`"[2001:db8::1]:6379"`). |
| `clusters: cluster name <x>_sentinel clashes with the Sentinel password of cluster <x> ...` | Both clusters would read the password file `<x>_sentinel_pass`. Rename one of them. |
| `redis-mcp: clusters <names> set entraid: true, but no Entra ID auth flow is configured ...` | Set `--entraid-auth-flow` / `REDIS_ENTRAID_AUTH_FLOW` and its settings (see [EntraID Authentication](#entraid-authentication-for-azure-managed-redis)), or remove `entraid: true`. |
| `redis-mcp: Could not read config file: <path> (not UTF-8: invalid byte on line <n>)` or `Could not read password file <path> (not UTF-8)` | Save the file as UTF-8 (a byte order mark is accepted in config files). The config message names the line of the first byte that is not UTF-8. |
| `redis-mcp: Could not parse config file: <path> (... line <l>, column <c> ...)` | The file is not valid YAML; fix it at the line and column named. The message does not quote the line. |
| `redis-mcp: Unknown toolset: [...]` | Valid toolsets are `core`, `data`, `json`, `search`, `pubsub` and `docs`. |
| `redis-mcp: --host cannot be combined with --config/--config-dir; ...` | The legacy connection flags do not apply to the multi-cluster mode; define the clusters in the config file. |
| HTTP 401 `X-Authorization header is missing or the API key does not match` | The key does not match the server's key exactly. Check that `REDIS_MCP_KEY` is exported in the shell that starts Claude Code and that it equals the key in the Secret `redis-mcp-api-key` (or `apiKey` in the ConfigMap when there is no Secret): no extra whitespace, no `Bearer` prefix, header name `X-Authorization`. The server log shows `unauthorized request` with `headerPresent`. |
| HTTP 403 `Requests with an Origin header (from a web page) are not accepted` | The request came from a web page or a browser-based client. Use a non-browser MCP client such as Claude Code. |
| Startup warning `cluster <name>: no password (...); connecting without AUTH`, then `Authentication required.` | The Secret has no `<name>_pass` key for this cluster (the name is case-sensitive and must match `name` exactly), or the Secret is not mounted at `passwordsDir`. |
| `invalid username-password pair or user is disabled.` | Wrong password or `username`, or the ACL user is `off`. Check for quotes or a trailing comment that `--from-env-file` kept in the value. |
| `this user has no permissions to run the '<command>' command` | The cluster's ACL user lacks the command. Expected for writes with `mcp-reader`; otherwise add the rule (see [Redis ACL user per cluster](#redis-acl-user-per-cluster)). |
| `Redis Cluster cannot be connected. Please provide at least one reachable node: Cluster mode is not enabled on this node` | Either the cluster is not a Redis Cluster (use `mode: standalone` or `sentinel`), or its ACL user lacks `+cluster\|slots`. |
| Results like `Error retrieving key a: 15495 127.0.0.1:17002` (a slot and an address: a `MOVED` redirect) | The cluster is a Redis Cluster but configured with `mode: standalone`. Set `mode: cluster`. |
| `Error 111 connecting to <host>:<port>. Connection refused.` or `Timeout connecting to server` | Check `hosts` and the network path from the pod (egress, firewall). The connect timeout is 5 s; raise it with `options: {socket_connect_timeout: ...}` if needed. |
| `Error -2 connecting to <host>:<port>. Name or service not known.` (or `Error -3 ... Temporary failure in name resolution`) | The host name does not resolve from the pod. Check the name in `hosts` and the pod's DNS (see [DNS lookups](#dns-lookups)). |
| `No master found for '<masterName>'` | Without anything after it: the Sentinels answered, but none of them knows a healthy primary named `masterName`; check `masterName`. With ` : <host>:<port> - <error type>: <message>, ...` after it: those Sentinels could not be asked. `ConnectionError` or `TimeoutError`: check the Sentinel addresses (usually port 26379) and the network path (each Sentinel gets at most 2 s). `AuthenticationError: Authentication required.` or `invalid username-password pair ...`: set or fix `<name>_sentinel_pass`. |
| `Timeout connecting to server` after about 10 s on a cluster with `tls`, or TLS errors such as `CERTIFICATE_VERIFY_FAILED` | `tls` set on a port without TLS: the TLS handshake gets no answer. For certificate errors, set `caFile` to the CA that signed the server certificate (mounted at `/etc/redis-mcp/tls`), and use the host name the certificate is issued for in `hosts`. |
| `Tool did not finish within 15 s (timeout). The cluster may be slow or unreachable (check clusters_list), or the request too large (narrow the pattern or count).` | Run `clusters_list`. If the cluster shows `status: timeout` or an error, the cluster or the path to it is the cause: Redis Cluster startup nodes that do not answer cost one timeout each and can add up to more than 15 s, and so can more than 8 simultaneous calls to a cluster that hangs, or a slow DNS lookup (see [Time limits, retries and concurrency](#time-limits-retries-and-concurrency)); fix `hosts`, the network or DNS, or lower the timeouts in `options`. If the cluster is fine, the request is too large: `scan_all_keys` walks the whole keyspace in one call, whatever the `pattern`. On large standalone or Sentinel databases use `scan_keys` and page through the keys with the `cursor` it returns, or use a more specific tool; on a Redis Cluster `scan_keys` cannot page (see [Toolsets](#toolsets)), so narrow the `pattern`. |
| `status: timeout` in `clusters_list` | The cluster's check did not finish within 13 s, for example two or more Redis Cluster startup nodes that accept connections but do not answer, or a slow DNS lookup. See the row above. |
| `Redis Cluster cannot be connected ...` again at once, right after the cluster was fixed | A failed Redis Cluster connection is remembered for 5 s (see [Redis clients](#redis-clients)). The first call after that tries again. |
| A call fails once with `Error while reading from <host>:<port> : (104, 'Connection reset by peer')`, `Connection closed by server.` or `Timeout reading from socket`, and the next call works | The connection broke while the command was in flight, typically during a restart or a failover. The server sends no command twice, so a write in such a call may or may not have been applied: check before you repeat it. |
| `Too many connections` | More calls ran on the cluster at once than its tools' client has connections, usually because `options` sets `max_connections` below the 8 calls a cluster runs at a time. Remove the option (default 16) or raise it. Pub/sub subscriptions do not count: they have a client of their own. |
| A tool or cluster is missing from the list, or `Unknown tool: <name>` | Its toolset is not in `toolsets`, the cluster is read-only (write tools), `disableDestructive` is on (delete/pop/remove tools), or the cluster does not list the module (`json`/`search` tools). |
| `Input should be '<cluster>'` | The tool is not available on the named cluster (for example a write tool on a read-only cluster); the error lists the allowed values. |
| `Subscription '<id>' was not found` | The pod restarted (subscriptions live in the process) or the subscription was closed. Subscribe again. |

## Table of Contents
- [Overview](#overview)
- [Multi-cluster mode (fork)](#multi-cluster-mode-fork)
- [Features](#features)
- [Tools](#tools)
- [Installation](#installation)
  - [From PyPI (recommended)](#from-pypi-recommended)
  - [Testing the PyPI package](#testing-the-pypi-package)
  - [From GitHub](#from-github)
  - [Development Installation](#development-installation)
  - [With Docker](#with-docker)
- [Configuration](#configuration)
  - [Redis ACL](#redis-acl)
  - [Configuration via command line arguments](#configuration-via-command-line-arguments)
  - [Configuration via Environment Variables](#configuration-via-environment-variables)
  - [EntraID Authentication for Azure Managed Redis](#entraid-authentication-for-azure-managed-redis)
  - [Logging](#logging)
- [Integrations](#integrations)
  - [OpenAI Agents SDK](#openai-agents-sdk)
  - [Augment](#augment)
  - [Claude Desktop](#claude-desktop)
  - [VS Code with GitHub Copilot](#vs-code-with-github-copilot)
- [Testing](#testing)
- [Example Use Cases](#example-use-cases)
- [Contributing](#contributing)
- [License](#license)
- [Badges](#badges)
- [Contact](#contact)


## Features
- **Natural Language Queries**: Enables AI agents to query and update Redis using natural language.
- **Seamless MCP Integration**: Works with any **MCP client** for smooth communication.
- **Full Redis Support**: Handles **hashes, lists, sets, sorted sets, streams**, and more.
- **Search & Filtering**: Supports efficient data retrieval and searching in Redis.
- **Scalable & Lightweight**: Designed for **high-performance** data operations.
- **EntraID Authentication**: Native support for Azure Active Directory authentication with Azure Managed Redis.
- The Redis MCP Server supports the `stdio` [transport](https://modelcontextprotocol.io/docs/concepts/transports#standard-input%2Foutput-stdio). Support to the `stremable-http` transport will be added in the future. (This fork also serves Streamable HTTP; see [Multi-cluster mode (fork)](#multi-cluster-mode-fork).)

## Tools

This MCP Server provides tools to manage the data stored in Redis.

- `string` tools to set, get strings with expiration. Useful for storing simple configuration values, session data, or caching responses.
- `hash` tools to store field-value pairs within a single key. The hash can store vector embeddings. Useful for representing objects with multiple attributes, user profiles, or product information where fields can be accessed individually.
- `list` tools with common operations to append and pop items. Useful for queues, message brokers, or maintaining a list of most recent actions.
- `set` tools to add, remove and list set members. Useful for tracking unique values like user IDs or tags, and for performing set operations like intersection.
- `sorted set` tools to manage data for e.g. leaderboards, priority queues, or time-based analytics with score-based ordering.
- `pub/sub` functionality to publish messages, create stateful channel or pattern subscriptions, and read queued messages using returned subscription handles. Useful for real-time notifications, chat applications, or distributing updates to multiple clients.
- `streams` tools to add, read, delete, create and destroy consumer groups, and acknowledge processed entries in data streams. Useful for event sourcing, activity feeds, and worker-based event processing with Redis Streams consumer groups.
- `JSON` tools to store, retrieve, and manipulate JSON documents in Redis. Useful for complex nested data structures, document databases, or configuration management with path-based access.

Additional tools.

- `docs` tool to search Redis documentation, tutorials, and best practices using natural language questions (backed by the `MCP_DOCS_SEARCH_URL` HTTP API).
- `query engine` tools to manage vector indexes and perform vector search
- `server management` tool to retrieve information about the database

## Installation

The Redis MCP Server is available as a PyPI package and as direct installation from the GitHub repository.

### From PyPI (recommended)
Configuring the latest Redis MCP Server version from PyPI, as an example, can be done importing the following JSON configuration in the desired framework or tool.
The `uvx` command will download the server on the fly (if not cached already), create a temporary environment, and then run it.

```commandline
{
  "mcpServers": {
    "RedisMCPServer": {
      "command": "uvx",
      "args": [
        "--from",
        "redis-mcp-server@latest",
        "redis-mcp-server",
        "--url",
        "\"redis://localhost:6379/0\""
      ]
    }
  }
}
```

#### URL specification

The format to specify the `--url` argument follows the [redis](https://www.iana.org/assignments/uri-schemes/prov/redis) and [rediss](https://www.iana.org/assignments/uri-schemes/prov/rediss) schemes:

```commandline
redis://user:secret@localhost:6379/0?foo=bar&qux=baz
```

As an example, you can easily connect to a localhost server with:

```commandline
redis://localhost:6379/0
```

Where `0` is the [logical database](https://redis.io/docs/latest/commands/select/) you'd like to connect to.

For an encrypted connection to the database (e.g., connecting to a [Redis Cloud](https://redis.io/cloud/) database), you'd use the `rediss` scheme.

```commandline
rediss://user:secret@localhost:6379/0?foo=bar&qux=baz
```

To verify the server's identity, specify `ssl_ca_certs`.

```commandline
rediss://user:secret@hostname:port?ssl_cert_reqs=required&ssl_ca_certs=path_to_the_certificate
```

For an unverified connection, set `ssl_cert_reqs` to `none`

```commandline
rediss://user:secret@hostname:port?ssl_cert_reqs=none
```

Configure your connection using the available options in the section "Available CLI Options".

### Testing the PyPI package

You can install the package as follows:

```sh
pip install redis-mcp-server
```

And start it using `uv` the package in your environment.

```sh
uv python install 3.14
uv sync
uv run redis-mcp-server --url redis://localhost:6379/0
```

However, starting the MCP Server is most useful when delegate to the framework or tool where this MCP Server is configured.

### From GitHub

You can configure the desired Redis MCP Server version with `uvx`, which allows you to run it directly from GitHub (from a branch, or use a tagged release).

> It is recommended to use a tagged release, the `main` branch is under active development and may contain breaking changes.

As an example, you can execute the following command to run the `0.2.0` release:

```commandline
uvx --from git+https://github.com/redis/mcp-redis.git@0.2.0 redis-mcp-server --url redis://localhost:6379/0
```

Check the release notes for the latest version in the [Releases](https://github.com/redis/mcp-redis/releases) section.
Additional examples are provided below.

```sh
# Run with Redis URI
uvx --from git+https://github.com/redis/mcp-redis.git redis-mcp-server --url redis://localhost:6379/0

# Run with Redis URI and SSL
uvx --from git+https://github.com/redis/mcp-redis.git redis-mcp-server --url "rediss://<USERNAME>:<PASSWORD>@<HOST>:<PORT>?ssl_cert_reqs=required&ssl_ca_certs=<PATH_TO_CERT>"

# Run with individual parameters
uvx --from git+https://github.com/redis/mcp-redis.git redis-mcp-server --host localhost --port 6379 --password mypassword

# See all options
uvx --from git+https://github.com/redis/mcp-redis.git redis-mcp-server --help
```

### Development Installation

For development or if you prefer to clone the repository:

```sh
# Clone the repository
git clone https://github.com/redis/mcp-redis.git
cd mcp-redis

# Install dependencies using uv
uv venv
source .venv/bin/activate
uv sync

# Run with CLI interface
uv run redis-mcp-server --help

# Or run the main file directly (uses environment variables)
uv run src/main.py
```

Once you cloned the repository, installed the dependencies and verified you can run the server, you can configure Claude Desktop or any other MCP Client to use this MCP Server running the main file directly (it uses environment variables). This is usually preferred for development.
The following example is for Claude Desktop, but the same applies to any other MCP Client.

1. Specify your Redis credentials and TLS configuration
2. Retrieve your `uv` command full path (e.g. `which uv`)
3. Edit the `claude_desktop_config.json` configuration file
   - on a MacOS, at `~/Library/Application\ Support/Claude/`

```json
{
    "mcpServers": {
        "redis": {
            "command": "<full_path_uv_command>",
            "args": [
                "--directory",
                "<your_mcp_server_directory>",
                "run",
                "src/main.py"
            ],
            "env": {
                "REDIS_HOST": "<your_redis_database_hostname>",
                "REDIS_PORT": "<your_redis_database_port>",
                "REDIS_PWD": "<your_redis_database_password>",
                "REDIS_SSL": True|False,
                "REDIS_SSL_CA_PATH": "<your_redis_ca_path>",
                "REDIS_CLUSTER_MODE": True|False
            }
        }
    }
}
```

You can troubleshoot problems by tailing the log file.

```commandline
tail -f ~/Library/Logs/Claude/mcp-server-redis.log
```

### With Docker

You can use a dockerized deployment of this server. You can either build your own image or use the official [Redis MCP Docker](https://hub.docker.com/r/mcp/redis) image.

If you'd like to build your own image, the Redis MCP Server provides a Dockerfile. Build this server's image with:

```commandline
docker build -t mcp-redis .
```

Finally, configure the client to create the container at start-up. An example for Claude Desktop is provided below. Edit the `claude_desktop_config.json` and add:

```json
{
  "mcpServers": {
    "redis": {
      "command": "docker",
      "args": ["run",
                "--rm",
                "--name",
                "redis-mcp-server",
                "-i",
                "-e", "REDIS_HOST=<redis_hostname>",
                "-e", "REDIS_PORT=<redis_port>",
                "-e", "REDIS_USERNAME=<redis_username>",
                "-e", "REDIS_PWD=<redis_password>",
                "mcp-redis"]
    }
  }
}
```

To use the official [Redis MCP Docker](https://hub.docker.com/r/mcp/redis) image, just replace your image name (`mcp-redis` in the example above) with `mcp/redis`.

## Configuration

The Redis MCP Server can be configured in two ways: via command line arguments or via environment variables.
The precedence is: command line arguments > environment variables > default values.

### Redis ACL

You can configure Redis ACL to restrict the access to the Redis database. For example, to create a read-only user:

```
127.0.0.1:6379> ACL SETUSER readonlyuser on >mypassword ~* +@read -@write
```

Configure the user via command line arguments or environment variables.

### Configuration via command line arguments

When using the CLI interface, you can configure the server with command line arguments:

```sh
# Basic Redis connection
uvx --from redis-mcp-server@latest redis-mcp-server \
  --host localhost \
  --port 6379 \
  --password mypassword

# Using Redis URI (simpler)
uvx --from redis-mcp-server@latest redis-mcp-server \
  --url redis://user:pass@localhost:6379/0

# SSL connection
uvx --from redis-mcp-server@latest redis-mcp-server \
  --url rediss://user:pass@redis.example.com:6379/0

# See all available options
uvx --from redis-mcp-server@latest redis-mcp-server --help
```

**Available CLI Options:**
- `--url` - Redis connection URI (redis://user:pass@host:port/db)
- `--host` - Redis hostname (default: 127.0.0.1)
- `--port` - Redis port (default: 6379)
- `--db` - Redis database number (default: 0)
- `--username` - Redis username
- `--password` - Redis password
- `--ssl` - Enable SSL connection
- `--ssl-ca-path` - Path to CA certificate file
- `--ssl-keyfile` - Path to SSL key file
- `--ssl-certfile` - Path to SSL certificate file
- `--ssl-cert-reqs` - SSL certificate requirements (default: required)
- `--ssl-ca-certs` - Path to CA certificates file
- `--cluster-mode` - Enable Redis cluster mode

### Configuration via Environment Variables

If desired, you can use environment variables. Defaults are provided for all variables.

| Name                 | Description                                               | Default Value |
|----------------------|-----------------------------------------------------------|---------------|
| `REDIS_HOST`         | Redis IP or hostname                                      | `"127.0.0.1"` |
| `REDIS_PORT`         | Redis port                                                | `6379`        |
| `REDIS_DB`           | Database                                                  | 0             |
| `REDIS_USERNAME`     | Default database username                                 | `"default"`   |
| `REDIS_PWD`          | Default database password                                 | ""            |
| `REDIS_SSL`          | Enables or disables SSL/TLS                               | `False`       |
| `REDIS_SSL_CA_PATH`  | CA certificate for verifying server                       | None          |
| `REDIS_SSL_KEYFILE`  | Client's private key file for client authentication       | None          |
| `REDIS_SSL_CERTFILE` | Client's certificate file for client authentication       | None          |
| `REDIS_SSL_CERT_REQS`| Whether the client should verify the server's certificate | `"required"`  |
| `REDIS_SSL_CA_CERTS` | Path to the trusted CA certificates file                  | None          |
| `REDIS_CLUSTER_MODE` | Enable Redis Cluster mode                                 | `False`       |


### EntraID Authentication for Azure Managed Redis

The Redis MCP Server supports **EntraID (Azure Active Directory) authentication** for Azure Managed Redis, enabling OAuth-based authentication with automatic token management.

#### Authentication Providers

**Service Principal Authentication** - Application-based authentication using client credentials:
```bash
export REDIS_ENTRAID_AUTH_FLOW=service_principal
export REDIS_ENTRAID_CLIENT_ID=your-client-id
export REDIS_ENTRAID_CLIENT_SECRET=your-client-secret
export REDIS_ENTRAID_TENANT_ID=your-tenant-id
```

**Managed Identity Authentication** - For Azure-hosted applications:
```bash
# System-assigned managed identity
export REDIS_ENTRAID_AUTH_FLOW=managed_identity
export REDIS_ENTRAID_IDENTITY_TYPE=system_assigned

# User-assigned managed identity
export REDIS_ENTRAID_AUTH_FLOW=managed_identity
export REDIS_ENTRAID_IDENTITY_TYPE=user_assigned
export REDIS_ENTRAID_USER_ASSIGNED_CLIENT_ID=your-identity-client-id
```

**Default Azure Credential** - Automatic credential discovery (recommended for development):
```bash
export REDIS_ENTRAID_AUTH_FLOW=default_credential
export REDIS_ENTRAID_SCOPES=https://redis.azure.com/.default
```

#### EntraID Configuration Variables

| Name                                    | Description                                               | Default Value                        |
|-----------------------------------------|-----------------------------------------------------------|--------------------------------------|
| `REDIS_ENTRAID_AUTH_FLOW`               | Authentication flow type                                  | None (EntraID disabled)              |
| `REDIS_ENTRAID_CLIENT_ID`               | Service Principal client ID                               | None                                 |
| `REDIS_ENTRAID_CLIENT_SECRET`           | Service Principal client secret                           | None                                 |
| `REDIS_ENTRAID_TENANT_ID`               | Azure tenant ID                                           | None                                 |
| `REDIS_ENTRAID_IDENTITY_TYPE`           | Managed identity type                                     | `"system_assigned"`                  |
| `REDIS_ENTRAID_USER_ASSIGNED_CLIENT_ID` | User-assigned managed identity client ID                  | None                                 |
| `REDIS_ENTRAID_SCOPES`                  | OAuth scopes for Default Azure Credential                | `"https://redis.azure.com/.default"` |
| `REDIS_ENTRAID_RESOURCE`                | Azure Redis resource identifier                          | `"https://redis.azure.com/"`         |

#### Key Features

- **Automatic token renewal** - Background token refresh with no manual intervention
- **Graceful fallback** - Falls back to standard Redis authentication when EntraID not configured
- **Multiple auth flows** - Supports Service Principal, Managed Identity, and Default Azure Credential
- **Enterprise ready** - Designed for Azure Managed Redis with centralized identity management

#### Example Configuration

For **local development** with Azure CLI:
```bash
# Login with Azure CLI
az login

# Configure MCP server
export REDIS_ENTRAID_AUTH_FLOW=default_credential
export REDIS_URL=redis://your-azure-redis.redis.cache.windows.net:6379
```

For **production** with Service Principal:
```bash
export REDIS_ENTRAID_AUTH_FLOW=service_principal
export REDIS_ENTRAID_CLIENT_ID=your-app-client-id
export REDIS_ENTRAID_CLIENT_SECRET=your-app-secret
export REDIS_ENTRAID_TENANT_ID=your-tenant-id
export REDIS_URL=redis://your-azure-redis.redis.cache.windows.net:6379
```

For **Azure-hosted applications** with Managed Identity:
```bash
export REDIS_ENTRAID_AUTH_FLOW=managed_identity
export REDIS_ENTRAID_IDENTITY_TYPE=system_assigned
export REDIS_URL=redis://your-azure-redis.redis.cache.windows.net:6379
```

There are several ways to set environment variables:

1. **Using a `.env` File**:
Place a `.env` file in your project directory with key-value pairs for each environment variable. Tools like `python-dotenv`, `pipenv`, and `uv` can automatically load these variables when running your application. This is a convenient and secure way to manage configuration, as it keeps sensitive data out of your shell history and version control (if `.env` is in `.gitignore`).
For example, create a `.env` file with the following content from the `.env.example` file provided in the repository:

```bash
cp .env.example .env
```

Then edit the `.env` file to set your Redis configuration:

OR,

2. **Setting Variables in the Shell**:
You can export environment variables directly in your shell before running your application. For example:

```sh
export REDIS_HOST=your_redis_host
export REDIS_PORT=6379
# Other variables will be set similarly...
```

This method is useful for temporary overrides or quick testing.


### Logging

The server uses Python's standard logging and is configured at startup. By default it logs at WARNING and above. You can change verbosity with the `MCP_REDIS_LOG_LEVEL` environment variable.

- Accepted values (case-insensitive): `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL`, `NOTSET`
- Aliases supported: `WARN` → `WARNING`, `FATAL` → `CRITICAL`
- Numeric values are also accepted, including signed (e.g., `"10"`, `"+20"`)
- Default when unset or unrecognized: `WARNING`

Handler behavior
- If the host (e.g., `uv`, VS Code, pytest) already installed console handlers, the server will NOT add its own; it only lowers overly-restrictive handler thresholds so your chosen level is not filtered out. It will never raise a handler's threshold.
- If no handlers are present, the server adds a single stderr StreamHandler with a simple format.

Examples
```bash
# See normal lifecycle messages
MCP_REDIS_LOG_LEVEL=INFO uv run src/main.py

# Very verbose for debugging
MCP_REDIS_LOG_LEVEL=DEBUG uvx --from redis-mcp-server@latest redis-mcp-server --url redis://localhost:6379/0
```

In MCP client configs that support env, add it alongside your Redis settings. For example:
```json
{
  "mcpServers": {
    "redis": {
      "command": "uvx",
      "args": ["--from", "redis-mcp-server@latest", "redis-mcp-server", "--url", "redis://localhost:6379/0"],
      "env": {
        "REDIS_HOST": "localhost",
        "REDIS_PORT": "6379",
        "MCP_REDIS_LOG_LEVEL": "INFO"
      }
    }
  }
}
```


## Integrations

Integrating this MCP Server to development frameworks like OpenAI Agents SDK, or with tools like Claude Desktop, VS Code, or Augment is described in the following sections.

### OpenAI Agents SDK

Integrate this MCP Server with the OpenAI Agents SDK. Read the [documents](https://openai.github.io/openai-agents-python/mcp/) to learn more about the integration of the SDK with MCP.

Install the Python SDK.

```commandline
pip install openai-agents
```

Configure the OpenAI token:

```commandline
export OPENAI_API_KEY="<openai_token>"
```

And run the [application](./examples/redis_assistant.py).

```commandline
python3.14 redis_assistant.py
```

You can troubleshoot your agent workflows using the [OpenAI dashboard](https://platform.openai.com/traces/).

### Augment

The preferred way of configuring the Redis MCP Server in Augment is to use the [Easy MCP](https://docs.augmentcode.com/setup-augment/mcp#redis) feature.

You can also configure the Redis MCP Server in Augment manually by importing the server via JSON:

```json
{
  "mcpServers": {
    "Redis MCP Server": {
      "command": "uvx",
      "args": [
        "--from",
        "redis-mcp-server@latest",
        "redis-mcp-server",
        "--url",
        "redis://localhost:6379/0"
      ]
    }
  }
}
```

### Claude Desktop

The simplest way to configure MCP clients is using `uvx`. Add the following JSON to your `claude_desktop_config.json`, remember to provide the full path to `uvx`.

**Basic Redis connection:**
```json
{
  "mcpServers": {
    "redis-mcp-server": {
        "type": "stdio",
        "command": "/Users/mortensi/.local/bin/uvx",
        "args": [
            "--from", "redis-mcp-server@latest",
            "redis-mcp-server",
            "--url", "redis://localhost:6379/0"
        ]
    }
  }
}
```

**Azure Managed Redis with EntraID authentication:**
```json
{
  "mcpServers": {
    "redis-mcp-server": {
        "type": "stdio",
        "command": "/Users/mortensi/.local/bin/uvx",
        "args": [
            "--from", "redis-mcp-server@latest",
            "redis-mcp-server",
            "--url", "redis://your-azure-redis.redis.cache.windows.net:6379"
        ],
        "env": {
            "REDIS_ENTRAID_AUTH_FLOW": "default_credential",
            "REDIS_ENTRAID_SCOPES": "https://redis.azure.com/.default"
        }
    }
  }
}
```

### VS Code with GitHub Copilot

To use the Redis MCP Server with VS Code, you must nable the [agent mode](https://code.visualstudio.com/docs/copilot/chat/chat-agent-mode) tools. Add the following to your `settings.json`:

```json
{
  "chat.agent.enabled": true
}
```

You can start the GitHub desired version of the Redis MCP server using `uvx` by adding the following JSON to your `mcp.json` file:

```json
"servers": {
  "redis": {
    "type": "stdio",
    "command": "uvx",
    "args": [
      "--from", "redis-mcp-server@latest",
      "redis-mcp-server",
      "--url", "redis://localhost:6379/0"
    ]
  },
}
```

#### Suppressing uvx Installation Messages

If you want to suppress uvx installation messages that may appear as warnings in MCP client logs, use the `-qq` flag:

```json
"servers": {
  "redis": {
    "type": "stdio",
    "command": "uvx",
    "args": [
      "-qq",
      "--from", "redis-mcp-server@latest",
      "redis-mcp-server",
      "--url", "redis://localhost:6379/0"
    ]
  },
}
```

The `-qq` flag enables silent mode, which suppresses "Installed X packages" messages that uvx writes to stderr during package installation.

Alternatively, you can start the server using `uv` and configure your `mcp.json`. This is usually desired for development.

```json
// mcp.json
{
  "servers": {
    "redis": {
      "type": "stdio",
      "command": "<full_path_uv_command>",
      "args": [
        "--directory",
        "<your_mcp_server_directory>",
        "run",
        "src/main.py"
      ],
      "env": {
        "REDIS_HOST": "<your_redis_database_hostname>",
        "REDIS_PORT": "<your_redis_database_port>",
        "REDIS_USERNAME": "<your_redis_database_username>",
        "REDIS_PWD": "<your_redis_database_password>",
      }
    }
  }
}
```

For more information, see the [VS Code documentation](https://code.visualstudio.com/docs/copilot/chat/mcp-servers).

> **Tip:** You can prompt Copilot chat to use the Redis MCP tools by including `#redis` in your message.

> **Note:** Starting with [VS Code v1.102](https://code.visualstudio.com/updates/v1_102),
> MCP servers are now stored in a dedicated `mcp.json` file instead of `settings.json`.

## Testing

You can use the [MCP Inspector](https://modelcontextprotocol.io/docs/tools/inspector) for visual debugging of this MCP Server.

```sh
npx @modelcontextprotocol/inspector uv run src/main.py
```

## Example Use Cases
- **AI Assistants**: Enable LLMs to fetch, store, and process data in Redis.
- **Chatbots & Virtual Agents**: Retrieve session data, manage queues, and personalize responses.
- **Data Search & Analytics**: Query Redis for **real-time insights and fast lookups**.
- **Event Processing**: Manage event streams with **Redis Streams**.

## Contributing
1. Fork the repo
2. Create a new branch (`feature-branch`)
3. Commit your changes
4. Push to your branch and submit a PR!

## License
This project is licensed under the **MIT License**.

## Badges

<a href="https://glama.ai/mcp/servers/@redis/mcp-redis">
  <img width="380" height="200" src="https://glama.ai/mcp/servers/@redis/mcp-redis/badge" alt="Redis Server MCP server" />
</a>

## Contact
For questions or support, reach out via [GitHub Issues](https://github.com/redis/mcp-redis/issues).

Alternatively, you can join the [Redis Discord server](https://discord.gg/redis) and ask in the `#redis-mcp-server` channel.
