# redis-mcp

An [MCP](https://modelcontextprotocol.io) server for Redis fleets: one server in front of many Redis deployments,
whether standalone servers, Redis Cluster or Sentinel-managed primaries. It follows the operating model of
[openshift-mcp-server](https://github.com/openshift/openshift-mcp-server) and of its sibling kafka-mcp: one YAML
config file, tools grouped into toolsets, a read-only switch (turned on in the example config), and Streamable HTTP
or stdio as transport. It talks to Redis directly with redis-py.

Built with Python 3.10+ (the image uses Python 3.14), FastMCP from the MCP Python SDK 1.30 and redis-py 6.4.

**Highlights**

- **Many clusters, one server**: standalone servers, Redis Cluster and Sentinel-managed primaries side by side.
  Every tool that works on Redis takes a `cluster` parameter whose enum lists exactly the clusters the tool may run
  on. When only one cluster is configured, the parameter is optional.
- **55 tools in 6 toolsets**: `core`, `data`, `json`, `search`, `pubsub`, `docs`.
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
  JSON logs with one line per tool call, a `/healthz` endpoint, a non-root image on Docker Hub that runs on a
  read-only root filesystem, and kustomize manifests for OpenShift (`restricted-v2` SCC).
- **Single-cluster mode**: without a config file, the `REDIS_*` environment variables or the connection flags
  describe one Redis, with every toolset and stdio by default (see [Single-cluster mode](#single-cluster-mode)).

## Contents

- [Quick start](#quick-start)
- [Configuration](#configuration)
- [Passwords](#passwords)
- [Redis ACL user per cluster](#redis-acl-user-per-cluster)
- [Toolsets](#toolsets)
- [Tool reference](#tool-reference)
- [Time limits, retries and concurrency](#time-limits-retries-and-concurrency)
- [Transports and security](#transports-and-security)
- [JSON logs](#json-logs)
- [Container image](#container-image)
- [OpenShift deployment](#openshift-deployment)
- [Claude Code](#claude-code)
- [Single-cluster mode](#single-cluster-mode)
- [Troubleshooting](#troubleshooting)
- [Development](#development)
- [License](#license)

## Quick start

Run it either with Python and [uv](https://docs.astral.sh/uv/) or as a container with Docker/Podman (no Python
needed on the host).

### With Python

```bash
git clone https://github.com/srdrcn/redis-mcp.git && cd redis-mcp
uv sync                                          # creates .venv with the redis-mcp command
cp redis-mcp.example.yaml redis-mcp.local.yaml   # git-ignored; set apiKey and the clusters
export cbdevrds_pass='<password>'                # one <cluster>_pass variable per cluster (see Passwords)
uv run redis-mcp --config redis-mcp.local.yaml
```

The server refuses to start while `apiKey` still holds the `<api-key>` placeholder (exit code 2):

```
redis-mcp: Invalid config:
 - apiKey: apiKey still holds a placeholder (<...> or ${...}); set a real key, e.g. from `openssl rand -hex 32`
```

Set a real key, for example from `openssl rand -hex 32`, or an empty value (`apiKey: ""`) to turn authentication
off for a local run. To keep the key out of the file, delete the `apiKey` line and export `REDIS_MCP_API_KEY`
instead (see [Command line](#command-line)). The example config sets `port: 8080`, so the server listens on
`http://localhost:8080/mcp`:

```bash
curl -s localhost:8080/healthz                   # {"status":"UP"}
curl -s localhost:8080/mcp -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
  -H "X-Authorization: $REDIS_MCP_KEY" -d '{"jsonrpc":"2.0","id":1,"method":"tools/list"}'   # REDIS_MCP_KEY = the key
```

To run over stdio instead (for a local MCP client that starts the process itself):

```bash
uv run redis-mcp --config redis-mcp.local.yaml --transport stdio
```

### With Docker

Use the published image `chosenwar/redis-mcp` (see [Container image](#container-image)) or build your own with
`docker build -t redis-mcp:latest .`. Keep the container paths of the example config and mount the files there:

| File | Path in the container |
|---|---|
| config | `/etc/redis-mcp/config/redis-mcp.yaml` |
| passwords | `/etc/redis-mcp/passwords/<cluster>_pass`, one file per cluster (the example config's `passwordsDir`); keep the directory outside the repository |
| CA certificates (optional) | `/etc/redis-mcp/tls/`, for `tls: {caFile: /etc/redis-mcp/tls/ca.crt}` |

Streamable HTTP on `http://localhost:8080/mcp`:

```bash
docker run --rm -p 8080:8080 --read-only --tmpfs /tmp \
  -v "$PWD/redis-mcp.local.yaml:/etc/redis-mcp/config/redis-mcp.yaml:ro" \
  -v "$HOME/redis-mcp-passwords:/etc/redis-mcp/passwords:ro" \
  chosenwar/redis-mcp:latest /app/.venv/bin/redis-mcp --config /etc/redis-mcp/config/redis-mcp.yaml

curl -s localhost:8080/healthz                   # {"status":"UP"}
```

stdio (`-i` keeps stdin open):

```bash
docker run --rm -i --read-only --tmpfs /tmp \
  -v "$PWD/redis-mcp.local.yaml:/etc/redis-mcp/config/redis-mcp.yaml:ro" \
  -v "$HOME/redis-mcp-passwords:/etc/redis-mcp/passwords:ro" \
  chosenwar/redis-mcp:latest /app/.venv/bin/redis-mcp --config /etc/redis-mcp/config/redis-mcp.yaml --transport stdio
```

Notes:

- The image's default command (`/app/.venv/bin/python src/main.py`) also works: pass
  `-e REDIS_MCP_CONFIG=/etc/redis-mcp/config/redis-mcp.yaml` instead of the command. Without that variable it runs
  the [single-cluster mode](#single-cluster-mode) from the `REDIS_*` variables.
- To keep the API key out of the mounted file, delete `apiKey` there and add `-e REDIS_MCP_API_KEY` (which passes
  the variable from your shell).
- The process runs as UID 1001 in group 0 and must be able to read the mounted files. Files with mode `600` owned
  by your user are not readable by UID 1001; run the container as your UID with `--user "$(id -u):0"`.
- Inside the container `localhost` is the container itself. To reach a Redis on the host, use `--network host`
  (Linux; then `-p` is not needed) or the host's address.
- With Podman, use the same commands with `podman`; on SELinux hosts add `:Z` to the volume options
  (e.g. `:ro,Z`).

### On OpenShift

The short version (each step is explained in [OpenShift deployment](#openshift-deployment)):

```bash
oc new-project redis-mcp                                             # or: oc project <namespace>
# set the clusters in deploy/openshift/configmap.yaml (oc apply -k below creates the ConfigMap from it)
cp deploy/openshift/secret.yaml deploy/openshift/secret.local.yaml   # git-ignored (*.local.yaml)
openssl rand -hex 32                                                 # a value for api-key
# in secret.local.yaml: one <cluster>_pass per cluster and api-key, in place of the <...> placeholders
oc apply -f deploy/openshift/secret.local.yaml                       # Secrets redis-mcp-passwords, redis-mcp-api-key
oc apply -k deploy/openshift                                         # image: docker.io/chosenwar/redis-mcp:latest
oc rollout status deployment/redis-mcp
oc get route redis-mcp                                               # MCP endpoint: https://<route-host>/mcp
```

Clients send the `api-key` value in the `X-Authorization` header; see [Claude Code](#claude-code) for the
`.mcp.json` entry. After a change to either file, apply it again and run `oc rollout restart deployment/redis-mcp`.

### Command line

| Option | Environment variable | Meaning |
|---|---|---|
| `--config <file>` | `REDIS_MCP_CONFIG` | Main config file (YAML). |
| `--config-dir <dir>` | `REDIS_MCP_CONFIG_DIR` | Directory of drop-in `.yaml`/`.yml` files merged over the main config (see [Drop-in files](#drop-in-files---config-dir)). Can be used without `--config`. |
| `--transport <stdio\|http>` | | Transport. Defaults to `http` when an HTTP port is set (> 0) and to `stdio` otherwise. `--transport http` without a port uses 8080. |
| `--http-port <port>` | | HTTP port (1–65535). Overrides `port` from the config. (`--port` is the Redis port of the [single-cluster mode](#single-cluster-mode).) |
| `--api-key <key>` | `REDIS_MCP_API_KEY` | API key that HTTP clients must send in `X-Authorization`. Overrides `apiKey` from the config, also in the single-cluster mode. An empty value is ignored, so an empty variable cannot turn authentication off. A value of only whitespace stops the server (`redis-mcp: --api-key / REDIS_MCP_API_KEY is blank; set a key, or leave it empty or unset to keep apiKey from the config`); any other value is checked like `apiKey` (`redis-mcp: --api-key / REDIS_MCP_API_KEY: apiKey has leading or trailing whitespace ...`). Prefer the variable: a command line can be read by other users (`ps`) and anyone who can read the Deployment. |
| `--help` | | Print usage. |

The connection flags (`--url`, `--host`, `--port`, ...) and the `--entraid-*` flags belong to the
[single-cluster mode](#single-cluster-mode). Both `--option value` and `--option=value` work, and an option wins
over its environment variable.

With `--config` or `--config-dir`, the clusters come from the config only: the connection flags (`--url`,
`--host`, `--port`, `--db`, `--username`, `--password`, `--ssl*`, `--cluster-mode`) are rejected, for example
`redis-mcp: --host cannot be combined with --config/--config-dir; define the clusters in the config file`, and the
`REDIS_HOST`/`REDIS_PORT`/... variables are not used. An empty `--url` and the literal `--url '${REDIS_URL}'`
(which some MCP clients pass when `REDIS_URL` is not set) count as not given. The `--entraid-*` flags and
`REDIS_ENTRAID_*` variables still apply to clusters with `entraid: true`.

The process writes `redis-mcp: ...` to stderr and exits with code 2 for: an invalid or unreadable config (also
one that is not UTF-8: `Could not read config file: <path> (not UTF-8: invalid byte on line <n>)`; a UTF-8 byte
order mark is accepted) or password file, a blank or invalid `--api-key`, a connection flag combined with
`--config`, an unknown toolset, and a cluster with `entraid: true` but no Entra ID auth flow. A malformed option
value gets click's usage error, also with exit code 2. `python src/main.py` (the image's default command) takes no
options and reads `REDIS_MCP_CONFIG`/`REDIS_MCP_CONFIG_DIR` and `REDIS_MCP_API_KEY` from the environment.

Other environment variables:

| Variable | Meaning |
|---|---|
| `REDIS_MCP_LOG_LEVEL` | Log level: `DEBUG`, `INFO`, `WARNING`, `ERROR`, `CRITICAL` (also `WARN`, `FATAL` or a number). The default is `INFO` over HTTP and `WARNING` over stdio (see [JSON logs](#json-logs)). |
| `<name>_pass`, `<name>_sentinel_pass` | Cluster passwords, when there is no such file in `passwordsDir` (see [Passwords](#passwords)). |
| `MCP_DOCS_SEARCH_URL` | Endpoint of the `docs` toolset (default `https://redis.io/convai/api/docs/search`). |
| `REDIS_ENTRAID_*` | Entra ID settings for clusters with `entraid: true` (see [Entra ID](#entra-id)). |

A `.env` file in the repository root is loaded too (python-dotenv); variables that are already set win.

## Configuration

This is the content of `redis-mcp.example.yaml` without its header comment, the example for a local or Docker
run. On OpenShift, `deploy/openshift/configmap.yaml` holds the same settings and clusters without the `apiKey`
line: the key comes from the Secret `redis-mcp-api-key` there (see [OpenShift deployment](#openshift-deployment)).
Never commit real keys: keep a filled-in copy in `redis-mcp.local.yaml` or `redis-mcp.yaml` (both git-ignored).

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

### Top-level keys

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

### Cluster keys

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
| `entraid` | boolean | `false` | Authenticate with the Entra ID credential provider (settings from the `--entraid-*` flags / `REDIS_ENTRAID_*` variables, see [Entra ID](#entra-id)) instead of a password. It needs an Entra ID auth flow (`--entraid-auth-flow` / `REDIS_ENTRAID_AUTH_FLOW`): without one the server does not start (`redis-mcp: clusters <names> set entraid: true, but no Entra ID auth flow is configured (REDIS_ENTRAID_AUTH_FLOW or --entraid-auth-flow)`) instead of connecting without authentication. |
| `options` | map | `{}` | Client tuning for redis-py, limited to the keys of [Cluster options](#cluster-options) for the cluster's mode. Applied after the defaults, the credentials and the TLS settings. |

### Cluster options

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

### TLS keys

| Key | Type | Default | redis-py argument | Meaning |
|---|---|---|---|---|
| `caFile` | path | not set | `ssl_ca_certs` | CA bundle (PEM) trusted in addition to the system CAs. |
| `caPath` | path | not set | `ssl_ca_path` | Directory of CA certificates. Not supported with `mode: cluster`, because redis-py's `RedisCluster` ignores it and would verify the nodes against the system CAs only: the server does not start (`tls.caPath is not supported in cluster mode; use caFile`). |
| `certFile` | path | not set | `ssl_certfile` | Client certificate (mutual TLS). |
| `keyFile` | path | not set | `ssl_keyfile` | Key of the client certificate. |
| `certReqs` | `required`, `optional` or `none` | `required` | `ssl_cert_reqs` | Verification of the server certificate. `none` also turns the host name check off. |
| `checkHostname` | boolean | not set (redis-py default: on) | `ssl_check_hostname` | Match the server certificate against the host name in `hosts`. |

### Redis clients

The client arguments of each cluster are built in this order (later entries win):

```python
# 1. defaults
decode_responses=True or False          # chosen by the tool
lib_name="redis-py(redis-mcp_v<version>)"
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

### Drop-in files (`--config-dir`)

- The server reads the regular `*.yaml` and `*.yml` files directly inside the directory. Hidden files are skipped
  and subdirectories are not read, so the directory can be a mounted ConfigMap. The files are sorted by name and
  merged over `--config` one after another. `--config-dir` can also be used on its own.
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

## Passwords

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

On OpenShift, the passwords are the keys of the Secret `redis-mcp-passwords` in `deploy/openshift/secret.yaml`
(one `<cluster>_pass` key per cluster, plus `<cluster>_sentinel_pass` for Sentinels that require a password),
mounted at `/etc/redis-mcp/passwords`, one file per key. `secret.yaml` holds only `<password>` placeholders and is
not part of the kustomization; fill in a git-ignored copy and apply it:

```bash
cp deploy/openshift/secret.yaml deploy/openshift/secret.local.yaml   # git-ignored (*.local.yaml)
# replace each "<password>" in secret.local.yaml (and the api-key, see OpenShift deployment)
oc apply -f deploy/openshift/secret.local.yaml
oc rollout restart deployment/redis-mcp                              # when the pod already runs
```

The values are YAML strings under `stringData`: keep the quotes, so that a password such as `0123`, `yes` or one
with ` #` stays text as written. Inside double quotes, write `\\` for a backslash and `\"` for a double quote; inside
single quotes, only `'` changes (written `''`). The value gets no trailing line break. For a local run, `export
cbdevrds_pass=...` works in bash (names with `-` or `.` cannot be exported from a shell; use files then).

## Redis ACL user per cluster

`readOnly` only decides which tools are listed; it does not restrict what the server's Redis user may do. Give the
server its own ACL user on every cluster, with only the rights it needs. A read-only user for the `core` and `data`
toolsets:

```
ACL SETUSER mcp-reader on >REPLACE_ME ~* &* +@read +@connection -@dangerous +info +client|list +cluster|slots +cluster|nodes +cluster|info
```

and, on a writable cluster such as the sandbox, a user that may also write but still cannot flush, reconfigure or
shut down the server:

```
ACL SETUSER mcp-writer on >REPLACE_ME ~* &* +@read +@write +@connection -@dangerous +info +client|list +cluster|slots +cluster|nodes +cluster|info
```

- The rules are applied from left to right. `-@dangerous` must come **before** `+info +client|list`: `INFO` and
  `CLIENT LIST` (used by the `info`, `client_list` and `clusters_list` tools) are in the `@dangerous` category, so a
  `-@dangerous` at the end would remove them again. `-@dangerous` also removes `KEYS`, `FLUSHALL`, `FLUSHDB`,
  `CONFIG`, `SHUTDOWN` and `CLIENT KILL`.
- `+cluster|slots` is needed on a Redis Cluster: redis-py reads the slot map when it connects, and without this
  command the connection fails with `Cluster mode is not enabled on this node`. `+cluster|nodes +cluster|info` let
  `cluster_nodes` read the node list and the cluster state. They do no harm elsewhere.
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

## Toolsets

| Toolset | Tools | What it covers | Available on |
|---|---:|---|---|
| `core` (enabled by default) | 11 | The cluster list with a connection check, the nodes of a cluster (`cluster_nodes`), server information (`info`, `dbsize`, `client_list`) and key operations (`type`, `scan_keys`, `scan_all_keys`, `expire`, `rename`, `delete`) | Every cluster. `clusters_list` takes an optional `cluster` (all clusters when omitted) and `check` (default `true`: connect and report status, latency, Redis version, role and, for a Redis Cluster, the number of primaries). |
| `data` | 29 | Strings, hashes (including vectors stored as binary fields), lists, sets, sorted sets, and streams with consumer groups | Every cluster |
| `json` | 3 | RedisJSON documents: get, set and delete at a path | Clusters with `modules: [json]` |
| `search` | 6 | Redis Query Engine: indexes, index information, indexed key count, vector index creation on hashes, vector (KNN) and hybrid search | Clusters with `modules: [search]` |
| `pubsub` | 5 | Publish, subscribe to a channel or a pattern, read the queued messages, unsubscribe | Every cluster. `read_messages` and `unsubscribe` take the `subscription_id` and no `cluster`. |
| `docs` | 1 | Search the redis.io documentation | Not tied to a cluster (no `cluster` parameter). Calls `MCP_DOCS_SEARCH_URL` (default `https://redis.io/convai/api/docs/search`), so the pod needs egress to it. |

Pub/sub subscriptions live in the server process: at most 50 at a time over all clusters (one more gets `Too many
active subscriptions. Close unused subscriptions and try again.`), and a subscription that was not read for 5
minutes is closed when the next one is created. They use the cluster's pub/sub client, not the connections of the
other tools (see [Redis clients](#redis-clients)). `read_messages` waits up to `timeout_ms` (default 1000, at most
5000; `0` does not wait) and returns once `max_messages` messages (default 10, at most 100) have arrived or the
time is up; a call right after `subscribe` waits like any other. Subscriptions are lost when the pod restarts, and
`read_messages` then answers `Subscription '<id>' was not found`; this is why the Deployment runs a single replica.

On a Redis Cluster, `dbsize` adds up the keys of every primary, and `scan_all_keys` scans every primary until each
one is done. `scan_keys` runs one `SCAN` step on every primary at once (`count` applies to each of them) and
returns what they found together, with `cursor: 0`. `scan_complete` is `true` only when every primary has
finished; otherwise a `note` points to `scan_all_keys`. Every primary has its own cursor, so such a scan cannot be
continued: a call with a `cursor` other than 0 returns `Error: scan_keys cannot continue a cursor on a Redis
Cluster (every node has its own cursor); call it with cursor 0, or use scan_all_keys`. On a Redis Cluster, use
`scan_all_keys` with a narrow `pattern` (it still has to finish within the 15 s limit).

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
one cluster (a one-cluster config file or the [single-cluster mode](#single-cluster-mode)), `cluster` is optional
and comes last, described as `Cluster name (default: the only configured cluster)`: a call without it runs on that
cluster. `clusters_list` always takes an optional `cluster` (all clusters when omitted), and `read_messages`,
`unsubscribe` and `search_redis_documents` take none. With the example config, 31 of the 55 tools are listed.
For example:

| Tool | `cluster` enum |
|---|---|
| `get`, `scan_keys`, `info` (read) | `cbdevrds`, `cbtstrds`, `cbprdrds`, `cbsbxrds` |
| `set`, `hset`, `expire` (write) | `cbsbxrds` |
| `delete`, `hdel`, `lpop` (destructive) | not listed |
| `json_get`, `get_indexes` | not listed (toolsets `json`/`search` not enabled) |

The `WRITE:` prefix is added to the description of every write and destructive tool. Tools also carry MCP
annotations: `readOnlyHint: true` on read tools and `destructiveHint: true` on destructive tools (both are `false`
on the other write tools).

## Tool reference

The tables list every tool in the order of `tools/list` within its toolset. **Kind**: *read* tools are always
listed, *write* tools (the description starts with `WRITE:`, which is left out below) are hidden by `readOnly`,
and *destructive* tools are hidden by `readOnly` or `disableDestructive`. **Needs** is what a tool needs beyond a
`cluster` from its enum. The description is the first paragraph of the tool's own description.

| Toolset | Tools | Read | Write | Destructive |
|---|---:|---:|---:|---:|
| [core](#core-11-tools) | 11 | 8 | 2 | 1 |
| [data](#data-29-tools) | 29 | 10 | 11 | 8 |
| [json](#json-3-tools) | 3 | 1 | 1 | 1 |
| [search](#search-6-tools) | 6 | 5 | 1 | 0 |
| [pubsub](#pubsub-5-tools) | 5 | 4 | 1 | 0 |
| [docs](#docs-1-tool) | 1 | 1 | 0 | 0 |
| **Total** | **55** | **29** | **16** | **10** |

### core (11 tools)

| Tool | Kind | Needs | Description |
|---|---|---|---|
| `clusters_list` | read | `cluster` optional (all clusters when omitted) | List the configured Redis clusters (mode, hosts, readOnly, modules, auth) and, unless check is false, whether each one answers: status, latency, Redis version and role. Start here when a cluster misbehaves |
| `cluster_nodes` | read | — | Show the nodes of a cluster and their state: on a Redis Cluster the CLUSTER INFO summary and every node (address, id, primary or replica, the primary a replica follows, slot ranges, pfail/fail flags, link); on standalone and Sentinel the replication view (role, primary, connected replicas with lag) |
| `delete` | destructive | — | Delete a Redis key |
| `type` | read | — | Returns the string representation of the type of the value stored at key |
| `expire` | write | — | Set an expiration time for a Redis key |
| `rename` | write | — | Renames a Redis key from old_key to new_key |
| `scan_keys` | read | — | Scan keys in the Redis database using the SCAN command (non-blocking, production-safe) |
| `scan_all_keys` | read | — | Scan and return ALL keys matching a pattern using multiple SCAN iterations |
| `dbsize` | read | — | Get the number of keys stored in the Redis database |
| `info` | read | — | Get Redis server information and statistics |
| `client_list` | read | — | Get a list of connected clients to the Redis server |

### data (29 tools)

| Tool | Kind | Needs | Description |
|---|---|---|---|
| `hset` | write | — | Set a field in a hash stored at key with an optional expiration time |
| `hget` | read | — | Get the value of a field in a Redis hash |
| `hdel` | destructive | — | Delete a field from a Redis hash |
| `hgetall` | read | — | Get all fields and values from a Redis hash |
| `hexists` | read | — | Check if a field exists in a Redis hash |
| `set_vector_in_hash` | write | — | Store a vector as a field in a Redis hash |
| `get_vector_from_hash` | read | — | Retrieve a vector from a Redis hash and convert it back from binary blob |
| `lpush` | write | — | Push a value onto the left of a Redis list and optionally set an expiration time |
| `rpush` | write | — | Push a value onto the right of a Redis list and optionally set an expiration time |
| `lpop` | destructive | — | Remove and return the first element from a Redis list |
| `rpop` | destructive | — | Remove and return the last element from a Redis list |
| `lrange` | read | — | Get elements from a Redis list within a specific range |
| `llen` | read | — | Get the length of a Redis list |
| `lrem` | destructive | — | Remove elements from a Redis list |
| `sadd` | write | — | Add a value to a Redis set with an optional expiration time |
| `srem` | destructive | — | Remove a value from a Redis set |
| `smembers` | read | — | Get all members of a Redis set |
| `zadd` | write | — | Add a member to a Redis sorted set with an optional expiration time |
| `zrange` | read | — | Retrieve a range of members from a Redis sorted set |
| `zrem` | destructive | — | Remove a member from a Redis sorted set |
| `xadd` | write | — | Add an entry to a Redis stream with an optional expiration time |
| `xrange` | read | — | Read entries from a Redis stream |
| `xdel` | destructive | — | Delete an entry from a Redis stream |
| `xgroup_create` | write | — | Create a consumer group for a Redis stream |
| `xgroup_destroy` | destructive | — | Destroy a consumer group for a Redis stream |
| `xreadgroup` | write | — | Read entries from a Redis stream using a consumer group |
| `xack` | write | — | Acknowledge entries that were processed by a consumer group |
| `set` | write | — | Set a Redis string value with an optional expiration time |
| `get` | read | — | Get a Redis string value |

### json (3 tools)

| Tool | Kind | Needs | Description |
|---|---|---|---|
| `json_set` | write | `modules: [json]` | Set a JSON value in Redis at a given path with an optional expiration time |
| `json_get` | read | `modules: [json]` | Retrieve a JSON value from Redis at a given path |
| `json_del` | destructive | `modules: [json]` | Delete a JSON value from Redis at a given path |

### search (6 tools)

| Tool | Kind | Needs | Description |
|---|---|---|---|
| `get_indexes` | read | `modules: [search]` | List of indexes in the Redis database |
| `get_index_info` | read | `modules: [search]` | Retrieve schema and information about a specific Redis index using FT.INFO |
| `get_indexed_keys_number` | read | `modules: [search]` | Retrieve the number of indexed keys by the index |
| `create_vector_index_hash` | write | `modules: [search]` | Create a Redis 8 vector similarity index using HNSW on a Redis hash |
| `vector_search_hash` | read | `modules: [search]` | Perform a KNN vector similarity search using Redis 8 or later version on vectors stored in hash data structures |
| `hybrid_search` | read | `modules: [search]` | Perform a hybrid search combining a Redis filter expression with KNN vector similarity |

### pubsub (5 tools)

| Tool | Kind | Needs | Description |
|---|---|---|---|
| `publish` | write | — | Publish a message to a Redis channel |
| `subscribe` | read | — | Subscribe to a Redis channel and return a reusable subscription handle |
| `psubscribe` | read | — | Subscribe to Redis channels using a pattern |
| `read_messages` | read | no `cluster`; takes the `subscription_id` | Read pending pub/sub messages for an existing subscription |
| `unsubscribe` | read | no `cluster`; takes the `subscription_id` | Unsubscribe and close an existing pub/sub subscription |

### docs (1 tool)

| Tool | Kind | Needs | Description |
|---|---|---|---|
| `search_redis_documents` | read | no `cluster`; egress to `MCP_DOCS_SEARCH_URL` | Search Redis documentation and knowledge base to learn about Redis concepts and use cases |

## Time limits, retries and concurrency

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
A cluster that hangs ties up only its own workers. With 12 simultaneous `get` calls to a standalone cluster that
accepted connections but never answered, 8 returned `Timeout reading from socket` after 10 s and the other 4
(which got a worker only after 10 s) the 15 s timeout message, while `get` calls to another cluster made at the
same time answered within about 10 ms.

### DNS lookups

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

## Transports and security

- **Streamable HTTP** on `/mcp` (stateless, JSON responses). The server handles each request on its own and keeps
  no MCP session, so a pod restart does not break connected clients (pub/sub subscriptions excepted). The health
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
  who can reach /mcp can call the tools`. On OpenShift, the key comes from the Secret `redis-mcp-api-key` (see
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
- Keep secrets out of the repository. `redis-mcp.yaml` in the repository root, `*.local.yaml` (such as
  `deploy/openshift/secret.local.yaml`) and `conf.d/` are git-ignored, and the image build context contains only
  `src/`, `pyproject.toml`, `uv.lock`, `README.md` and `LICENSE`. Examples use `<...>` placeholders;
  `deploy/openshift/configmap.yaml` holds no secrets, and `deploy/openshift/secret.yaml` only placeholders.
- The pod does not mount a service account token (`automountServiceAccountToken: false`), because the server never
  calls the Kubernetes API.

## JSON logs

Over HTTP, logs are JSON on **stdout**, one object per line with `@timestamp`, `level`, `logger`, `message` and any
extra fields, at `INFO` (`REDIS_MCP_LOG_LEVEL` overrides it). The MCP SDK's own loggers are kept at `WARNING` and
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

Over stdio, stdout carries the JSON-RPC stream, so logs go to stderr as plain `[LEVEL] message` lines, at `WARNING`
by default; set `REDIS_MCP_LOG_LEVEL=INFO` to see the `tool call` lines. Inside a Python process that already has
log handlers (pytest, for example), the server adds none of its own.

## Container image

`Dockerfile` builds on `python:3.14-slim` and installs the locked dependencies into `/app/.venv` with
`uv sync --locked --no-dev`. It is built to run under the OpenShift **restricted-v2** SCC:

- It runs as non-root user 1001. `/app` and `/etc/redis-mcp` are group 0 with `g=u`, so a random UID in group 0
  works.
- It writes nothing at runtime: the bytecode is compiled during the build and `uv` is not used to start the
  server, so the root filesystem can be read-only, with an `emptyDir` mounted at `/tmp`. (`uv run` inside the
  container would fail as a non-root user: `Failed to initialize cache at /.cache/uv`.)
- `CMD ["/app/.venv/bin/python", "src/main.py"]`: with `REDIS_MCP_CONFIG` (or `REDIS_MCP_CONFIG_DIR`) set it runs
  the multi-cluster mode, otherwise the [single-cluster mode](#single-cluster-mode) from the `REDIS_*` variables
  over stdio. The OpenShift Deployment runs `/app/.venv/bin/redis-mcp --config
  /etc/redis-mcp/config/redis-mcp.yaml` instead. `/app/.venv/bin` is on `PATH`, so `redis-mcp` also works as a
  command. Port 8080 is exposed; keep `port: 8080` in the config to get HTTP.
- Mount points: `/etc/redis-mcp/config` (redis-mcp.yaml), `/etc/redis-mcp/passwords` (`<cluster>_pass` files),
  `/etc/redis-mcp/tls` (CA certificates).

The image works with `--user 12345:0 --read-only --tmpfs /tmp` (a random UID in group 0 and a read-only root
filesystem, as on OpenShift), in both the multi-cluster (HTTP) and the single-cluster (stdio) mode.

### Published image

`.github/workflows/docker-push.yml` builds the image and pushes it to Docker Hub as
[`chosenwar/redis-mcp`](https://hub.docker.com/r/chosenwar/redis-mcp):

| Trigger | Tags |
|---|---|
| push to `main` | `latest`, `sha-<short commit>` |
| tag `vX.Y.Z` | `X.Y.Z`, `sha-<short commit>` |
| manual run (Actions → Docker push → Run workflow) | `sha-<short commit>` (plus `latest` on `main`) |

Before pushing, the workflow smoke-tests the image the way OpenShift runs it (`--user 12345:0 --read-only --tmpfs
/tmp`): `redis-mcp --help`, the import, `/healthz`, 401 without the API key and `tools/list` with it. It needs the
repository secrets `DOCKERHUB_USERNAME` and `DOCKERHUB_TOKEN` (a Docker Hub access token with Read & Write scope).
To release a version, set it in `pyproject.toml` and `src/version.py`, then tag it:
`git tag v0.1.0 && git push origin v0.1.0`.

## OpenShift deployment

`deploy/openshift/` is a kustomize directory:

| File | Content |
|---|---|
| `configmap.yaml` | The ConfigMap `redis-mcp-config`: the config under the key `redis-mcp.yaml`, with the settings and the example clusters of [Configuration](#configuration) (`cbdevrds`, `cbtstrds`, the Redis Cluster `cbprdrds`, and `cbsbxrds` with `readOnly: false`) but no `apiKey` line. Set the clusters for your environment; it holds no secrets. |
| `deployment.yaml` | 1 replica (`Recreate`: pub/sub subscriptions and the client caches live in the process). Startup, liveness and readiness probes on `/healthz`. A restricted-v2 security context (non-root, no privilege escalation, all capabilities dropped, `RuntimeDefault` seccomp, read-only root filesystem). No service account token is mounted. A `/tmp` `emptyDir` (64Mi). Requests of 50m CPU and 128Mi memory, with a 256Mi memory limit (the server uses under 100 MB at idle). The command is `/app/.venv/bin/redis-mcp --config /etc/redis-mcp/config/redis-mcp.yaml`. The config is mounted at `/etc/redis-mcp/config`, the passwords (Secret `redis-mcp-passwords`) at `/etc/redis-mcp/passwords` (mode `0440`). `REDIS_MCP_API_KEY` comes from the key `api-key` of the Secret `redis-mcp-api-key`; the entry is active and required, so the pod does not start until that Secret exists (`CreateContainerConfigError`). An optional CA Secret mount at `/etc/redis-mcp/tls` and a `dnsConfig` with `ndots: "2"` (see [DNS lookups](#dns-lookups)) are commented out. |
| `service.yaml` | `redis-mcp` Service on port 8080 (`http`). |
| `route.yaml` | Edge TLS (HTTP redirects to HTTPS), `haproxy.router.openshift.io/timeout: 60s`. |
| `secret.yaml` | The Secrets `redis-mcp-passwords` (one `<cluster>_pass` key per cluster of `configmap.yaml`, plus commented keys for a Sentinel cluster) and `redis-mcp-api-key` (key `api-key`), with `"<password>"`/`"<api-key>"` placeholders. Not part of the kustomization: copy it to `secret.local.yaml` (git-ignored), fill it in and `oc apply -f` it. |
| `kustomization.yaml` | `configmap.yaml`, `deployment.yaml`, `service.yaml` and `route.yaml` (not `secret.yaml`), labels, and the `images:` entry that sets the image. |

`oc apply -k deploy/openshift` creates the ConfigMap `redis-mcp-config` with the Deployment, the Service and the
Route. The two Secrets hold the passwords and the API key, so the repository has only the template `secret.yaml`:
you apply a filled-in copy with `oc apply -f`.

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
    newTag: latest                           # or a release, e.g. 0.1.0
```

**3. Set the clusters** in `deploy/openshift/configmap.yaml` (keys: [Configuration](#configuration)). Keep
`port: 8080` and `passwordsDir: /etc/redis-mcp/passwords`, which match the Deployment, and leave `apiKey` out: the
key comes from the Secret (`REDIS_MCP_API_KEY` overrides `apiKey`, but the config is checked first, so an
`apiKey: <api-key>` placeholder would still stop the pod). `oc apply -k` writes this file over an existing
`redis-mcp-config` ConfigMap, also one made earlier with `oc create configmap`.

**4. Fill in the Secrets.** Copy the template to a git-ignored file, replace every `<...>` and apply it:

```bash
cp deploy/openshift/secret.yaml deploy/openshift/secret.local.yaml   # git-ignored (*.local.yaml)
openssl rand -hex 32                                                 # a value for api-key
# in secret.local.yaml: one <cluster>_pass per cluster of configmap.yaml, and api-key
oc apply -f deploy/openshift/secret.local.yaml
oc extract secret/redis-mcp-api-key --keys=api-key --to=-            # shows the key again later
```

- `redis-mcp-passwords` needs one `<cluster>_pass` key per cluster (see [Passwords](#passwords), also for quoting
  the values). A cluster without a key connects without AUTH, with a warning in the log.
- `api-key` is required: while the Secret `redis-mcp-api-key` does not exist, the pod does not start
  (`CreateContainerConfigError`), and while `api-key` is still the `<api-key>` placeholder or starts or ends with
  whitespace, the server stops (`redis-mcp: --api-key / REDIS_MCP_API_KEY: apiKey ...`).
- An empty `api-key: ""` is ignored, and with no `apiKey` in the ConfigMap authentication is then off
  (`API key authentication DISABLED` in the log).

**5. Apply the manifests** (the ConfigMap, the Deployment, the Service and the Route):

```bash
oc apply -k deploy/openshift
oc rollout status deployment/redis-mcp
oc get route redis-mcp
```

Applied before the Secrets, the pod waits (`CreateContainerConfigError`, or `ContainerCreating` with a failed mount
of `redis-mcp-passwords`) and starts once they exist.

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

**Updating the config, the passwords or the API key.** They are read only at startup, so edit
`deploy/openshift/configmap.yaml` or `deploy/openshift/secret.local.yaml`, apply it again and restart the pod:

```bash
oc apply -k deploy/openshift                         # after a change to configmap.yaml
oc apply -f deploy/openshift/secret.local.yaml       # after a change to the passwords or api-key
oc rollout restart deployment/redis-mcp
```

A new `api-key` (`openssl rand -hex 32`) means the clients need the new key too. A key deleted from `stringData`
stays in the Secret after `oc apply`; `oc replace -f deploy/openshift/secret.local.yaml` removes it.

With `Recreate`, the old pod stops before the new one starts, so there is a short gap, and open pub/sub
subscriptions are lost.

## Claude Code

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
machine, and `apiKey` is not used (but still checked: `""` or no line, not the placeholder). Pass
`--transport stdio` explicitly, because a config with `port` set would otherwise start HTTP. With the `redis-mcp`
command from a clone (after `uv sync`), the passwords come from the environment:

```json
{
  "mcpServers": {
    "redis": {
      "type": "stdio",
      "command": "/path/to/redis-mcp/.venv/bin/redis-mcp",
      "args": ["--config", "/path/to/redis-mcp.local.yaml", "--transport", "stdio"],
      "env": {
        "cbsbxrds_pass": "${CBSBXRDS_PASS}"
      }
    }
  }
}
```

The same with the container image (absolute host paths; see [With Docker](#with-docker) for the mounts and file
permissions):

```json
{
  "mcpServers": {
    "redis": {
      "type": "stdio",
      "command": "docker",
      "args": [
        "run", "--rm", "-i", "--read-only", "--tmpfs", "/tmp",
        "-v", "/path/to/redis-mcp.local.yaml:/etc/redis-mcp/config/redis-mcp.yaml:ro",
        "-v", "/path/to/redis-mcp-passwords:/etc/redis-mcp/passwords:ro",
        "chosenwar/redis-mcp:latest",
        "/app/.venv/bin/redis-mcp", "--config", "/etc/redis-mcp/config/redis-mcp.yaml", "--transport", "stdio"
      ]
    }
  }
}
```

## Single-cluster mode

Without `--config` and `--config-dir` (and without `REDIS_MCP_CONFIG`/`REDIS_MCP_CONFIG_DIR`), the server serves
one Redis described by the connection flags or the `REDIS_*` environment variables, as a cluster named `default`:

- every toolset is listed, `json` and `search` included, and nothing is hidden by `readOnly` or
  `disableDestructive`;
- the tools' `cluster` parameter is optional (see [Which tools are listed](#which-tools-are-listed));
- the transport is stdio, unless `--transport http` (port 8080) or `--http-port` is given; there is no API key
  unless `--api-key` / `REDIS_MCP_API_KEY` sets one.

```bash
uv run redis-mcp --url redis://localhost:6379/0                     # stdio
REDIS_PWD='<password>' uv run redis-mcp --host redis.example.com --port 6380 --username mcp-reader --ssl
REDIS_HOST=localhost REDIS_PORT=6379 uv run python src/main.py      # environment only (the image's default command)
REDIS_MCP_API_KEY='<api-key>' uv run redis-mcp --url redis://localhost:6379/0 --transport http   # HTTP on :8080
```

| Variable | Flag | Default | Meaning |
|---|---|---|---|
| | `--url` | not set | `redis://[user:password@]host:port/db`, or `rediss://...` for TLS. Query parameters: `ssl_cert_reqs`, `ssl_ca_certs`, `ssl_ca_path`, `ssl_certfile`, `ssl_keyfile`, `db`. Another scheme stops with `Error parsing Redis URI: ...` (exit code 1). |
| `REDIS_HOST` | `--host` | `127.0.0.1` | Host name or IP address. |
| `REDIS_PORT` | `--port` | `6379` | Port. |
| `REDIS_DB` | `--db` | `0` | Database number (not used with cluster mode). |
| `REDIS_USERNAME` | `--username` | not set | ACL user; without it the password authenticates the `default` user. |
| `REDIS_PWD` | `--password` | not set | Password. Prefer the variable: a command line can be read with `ps`. |
| `REDIS_SSL` | `--ssl` | off | TLS (`true`, `1` or `t`). |
| `REDIS_SSL_CA_CERTS` | `--ssl-ca-certs` | not set | CA bundle file (`tls.caFile`). |
| `REDIS_SSL_CA_PATH` | `--ssl-ca-path` | not set | CA directory (`tls.caPath`). Not supported with cluster mode. |
| `REDIS_SSL_CERTFILE`, `REDIS_SSL_KEYFILE` | `--ssl-certfile`, `--ssl-keyfile` | not set | Client certificate and key (mutual TLS). |
| `REDIS_SSL_CERT_REQS` | `--ssl-cert-reqs` | `required` | `required`, `optional` or `none`. |
| `REDIS_CLUSTER_MODE` | `--cluster-mode` | off | Redis Cluster (`true`, `1` or `t`); the host is then a startup node. |

- With the `redis-mcp` command, the flags that have a default (`--host`, `--port`, `--db`, `--ssl`,
  `--ssl-cert-reqs`, `--cluster-mode`) win over their variables even when they are not given, so `REDIS_HOST`,
  `REDIS_PORT`, `REDIS_DB`, `REDIS_SSL`, `REDIS_SSL_CERT_REQS` and `REDIS_CLUSTER_MODE` apply only to
  `python src/main.py`. The other variables apply to both, unless the flag is given.
- `--cluster-mode` together with `--ssl` and `--ssl-ca-path` (or the variables) stops with
  `redis-mcp: --ssl-ca-path / REDIS_SSL_CA_PATH is not supported with --cluster-mode (redis-py's RedisCluster
  ignores it); use --ssl-ca-certs / REDIS_SSL_CA_CERTS` (exit code 2), because the nodes would be verified against
  the system CAs only. Use a CA bundle file instead.
- Over HTTP the logs are JSON as described in [JSON logs](#json-logs); over stdio they are plain lines on stderr.

### Entra ID

For Azure Managed Redis, `--entraid-auth-flow` / `REDIS_ENTRAID_AUTH_FLOW` replaces the password with a Microsoft
Entra ID token (redis-entraid, renewed before it expires); the username and password are then not used.

| Flow | Settings |
|---|---|
| `service_principal` | `REDIS_ENTRAID_CLIENT_ID`, `REDIS_ENTRAID_CLIENT_SECRET`, `REDIS_ENTRAID_TENANT_ID` |
| `managed_identity` | `REDIS_ENTRAID_IDENTITY_TYPE`: `system_assigned` (default), or `user_assigned` with `REDIS_ENTRAID_USER_ASSIGNED_CLIENT_ID`; `REDIS_ENTRAID_RESOURCE` (default `https://redis.azure.com/`) |
| `default_credential` | `REDIS_ENTRAID_SCOPES` (comma-separated, default `https://redis.azure.com/.default`) |

The `--entraid-*` flags set the same values (`redis-mcp --help`; a user-assigned identity takes
`--entraid-client-id`). As with the connection flags, the ones with a default (identity type, scopes, resource,
token refresh and retry settings) win over their variables with the `redis-mcp` command. With a config file, the
same settings apply to every cluster with `entraid: true`.

## Troubleshooting

Start with `clusters_list`: it shows every cluster's mode, hosts, `readOnly`, modules and auth, and connects to each
one to report `status` (`ok`, `error` or `timeout`), `latencyMs`, `redisVersion`, `role` and, for a Redis Cluster,
`primaries`, or the `error`.

| Symptom | Cause and fix |
|---|---|
| `redis-mcp: Invalid config:` then `- apiKey: apiKey still holds a placeholder ...` | `apiKey` was not filled in. Set a real key (`openssl rand -hex 32`), `""` to turn authentication off, or delete the line when the key comes from `REDIS_MCP_API_KEY` (the config is checked even then). |
| `redis-mcp: --api-key / REDIS_MCP_API_KEY is blank; ...` | The key from `--api-key` or `REDIS_MCP_API_KEY` (on OpenShift: `api-key` of the Secret `redis-mcp-api-key`) holds only whitespace. Set a key, or leave the value empty or unset to keep `apiKey` from the config. On OpenShift, fix `api-key` in `secret.local.yaml`, `oc apply -f` it and run `oc rollout restart deployment/redis-mcp`. |
| `redis-mcp: --api-key / REDIS_MCP_API_KEY: apiKey ...` | The key from `--api-key` or `REDIS_MCP_API_KEY` is a `<...>`/`${...}` placeholder (on OpenShift: `api-key` in `secret.local.yaml` was not filled in), or starts or ends with whitespace, such as a `\n` or a space inside the quotes of the `stringData` value, or the final line break of a file given to `oc create secret --from-file`. Set the key (`openssl rand -hex 32`), apply the Secret again and restart the deployment. |
| Pod in `CreateContainerConfigError` (`secret "redis-mcp-api-key" not found`), or in `ContainerCreating` with `MountVolume.SetUp failed for volume "passwords" : secret "redis-mcp-passwords" not found` | The Secrets do not exist in the pod's namespace: `oc apply -f deploy/openshift/secret.local.yaml` in that project (see [OpenShift deployment](#openshift-deployment)). The pod starts once they exist. |
| `redis-mcp: Invalid config:` then `- clusters[0]...` | Fix the named key, for example `clusters[0]: a standalone cluster takes exactly one host:port in hosts`, `clusters[0]: masterName is required when mode is sentinel`, `clusters[0].hosts: hosts entry 1 is not host:port (or [IPv6 address]:port)` or `clusters[0].password: Extra inputs are not permitted` (passwords go into `<name>_pass`). |
| `clusters[0]: options <keys> not supported in <mode> mode (supported: ...)` | `options` only takes the keys of [Cluster options](#cluster-options) for the cluster's mode. Addresses, `db`, `username` and `tls` have their own keys; passwords go into `<name>_pass`. |
| `clusters[0]: options.<key> must be <rule>` | The option's value breaks the rule in the "Value" column of [Cluster options](#cluster-options), for example `options.socket_timeout must be a number of seconds > 0 and <= 3600` for `0`, `null` or `1e10`, or `options.client_name must be a name without spaces`. |
| `clusters[0]: tls.caPath is not supported in cluster mode; use caFile` | Put the CA certificates into one PEM bundle and set `caFile`. |
| `redis-mcp: --ssl-ca-path / REDIS_SSL_CA_PATH is not supported with --cluster-mode ...` | The single-cluster mode's equivalent of the row above: use `--ssl-ca-certs` / `REDIS_SSL_CA_CERTS` with a CA bundle file instead of the CA directory. |
| `clusters[0].hosts: hosts takes host:port entries without a scheme or credentials ...` | `hosts` contains `://` or `@`, that is a URL or `user:password@`. Write plain `host:port` entries, put the user into `username` and the password into `<name>_pass`. |
| `clusters[0].hosts: hosts entry <n> is not host:port (or [IPv6 address]:port)` or `hosts entry <n>: the port must be between 1 and 65535` | Entry `<n>` of `hosts` (1 for the first) is empty (for example after a trailing comma), has no port (or one not written with the digits 0-9), has a space inside the host, or its port is outside 1–65535. Write IPv6 addresses in brackets and quote them in YAML (`"[2001:db8::1]:6379"`). |
| `clusters: cluster name <x>_sentinel clashes with the Sentinel password of cluster <x> ...` | Both clusters would read the password file `<x>_sentinel_pass`. Rename one of them. |
| `redis-mcp: clusters <names> set entraid: true, but no Entra ID auth flow is configured ...` | Set `--entraid-auth-flow` / `REDIS_ENTRAID_AUTH_FLOW` and its settings (see [Entra ID](#entra-id)), or remove `entraid: true`. |
| `redis-mcp: Could not read config file: <path> (not UTF-8: invalid byte on line <n>)` or `Could not read password file <path> (not UTF-8)` | Save the file as UTF-8 (a byte order mark is accepted in config files). The config message names the line of the first byte that is not UTF-8. |
| `redis-mcp: Could not parse config file: <path> (... line <l>, column <c> ...)` | The file is not valid YAML; fix it at the line and column named. The message does not quote the line. |
| `redis-mcp: Unknown toolset: [...]` | Valid toolsets are `core`, `data`, `json`, `search`, `pubsub` and `docs`. |
| `redis-mcp: --host cannot be combined with --config/--config-dir; ...` | The connection flags belong to the single-cluster mode; define the clusters in the config file. |
| HTTP 401 `X-Authorization header is missing or the API key does not match` | The key does not match the server's key exactly. Check that `REDIS_MCP_KEY` is exported in the shell that starts Claude Code and that it equals `api-key` in the Secret `redis-mcp-api-key` (`oc extract secret/redis-mcp-api-key --keys=api-key --to=-`; after a change to the Secret, the pod needs `oc rollout restart deployment/redis-mcp`): no extra whitespace, no `Bearer` prefix, header name `X-Authorization`. The server log shows `unauthorized request` with `headerPresent`. |
| HTTP 403 `Requests with an Origin header (from a web page) are not accepted` | The request came from a web page or a browser-based client. Use a non-browser MCP client such as Claude Code. |
| Startup warning `cluster <name>: no password (...); connecting without AUTH`, then `Authentication required.` | The Secret has no `<name>_pass` key for this cluster (the name is case-sensitive and must match `name` exactly), or the Secret is not mounted at `passwordsDir`. On OpenShift, add the key to `redis-mcp-passwords` in `secret.local.yaml`, `oc apply -f` it and restart the deployment. |
| `invalid username-password pair or user is disabled.` | Wrong password or `username`, or the ACL user is `off`. On OpenShift, check the value with `oc extract secret/redis-mcp-passwords --keys=<name>_pass --to=-`: inside double quotes YAML reads `\` as an escape, so a backslash is written `\\` (see [Passwords](#passwords)). |
| `this user has no permissions to run the '<command>' command` | The cluster's ACL user lacks the command. Expected for writes with `mcp-reader`; otherwise add the rule (see [Redis ACL user per cluster](#redis-acl-user-per-cluster)). |
| `Redis Cluster cannot be connected. Please provide at least one reachable node: Cluster mode is not enabled on this node` | Either the cluster is not a Redis Cluster (use `mode: standalone` or `sentinel`), or its ACL user lacks `+cluster\|slots`. |
| Results like `Error retrieving key a: 15495 127.0.0.1:17002` (a slot and an address: a `MOVED` redirect) | The cluster is a Redis Cluster but configured with `mode: standalone`. Set `mode: cluster`. |
| `Error 111 connecting to <host>:<port>. Connection refused.` or `Timeout connecting to server` | Check `hosts` and the network path from the pod (egress, firewall). The connect timeout is 5 s; raise it with `options: {socket_connect_timeout: ...}` if needed. |
| `Error -2 connecting to <host>:<port>. Name or service not known.` (or `Error -3 ... Temporary failure in name resolution`) | The host name does not resolve from the pod. Check the name in `hosts` and the pod's DNS (see [DNS lookups](#dns-lookups)). |
| `No master found for '<masterName>'` | Without anything after it: the Sentinels answered, but none of them knows a healthy primary named `masterName`; check `masterName`. With ` : <host>:<port> - <error type>: <message>, ...` after it: those Sentinels could not be asked. `ConnectionError` or `TimeoutError`: check the Sentinel addresses (usually port 26379) and the network path (each Sentinel gets at most 2 s). `AuthenticationError: Authentication required.` or `invalid username-password pair ...`: set or fix `<name>_sentinel_pass`. |
| `Timeout connecting to server` after about 10 s on a cluster with `tls`, or TLS errors such as `CERTIFICATE_VERIFY_FAILED` | `tls` set on a port without TLS: the TLS handshake gets no answer. For certificate errors, set `caFile` to the CA that signed the server certificate (mounted at `/etc/redis-mcp/tls`), and use the host name the certificate is issued for in `hosts`. |
| `Tool did not finish within 15 s (timeout). The cluster may be slow or unreachable (check clusters_list), or the request too large (narrow the pattern or count).` | Run `clusters_list`. If the cluster shows `status: timeout` or an error, the cluster or the path to it is the cause: Redis Cluster startup nodes that do not answer cost one timeout each and can add up to more than 15 s, and so can more than 8 simultaneous calls to a cluster that hangs, or a slow DNS lookup (see [Time limits, retries and concurrency](#time-limits-retries-and-concurrency)); fix `hosts`, the network or DNS, or lower the timeouts in `options`. If the cluster is fine, the request is too large: `scan_all_keys` walks the whole keyspace in one call, whatever the `pattern`. On large standalone or Sentinel databases use `scan_keys` and page through the keys with the `cursor` it returns, or use a more specific tool; on a Redis Cluster `scan_keys` cannot page (see [Toolsets](#toolsets)), so narrow the `pattern`. |
| `Error: scan_keys cannot continue a cursor on a Redis Cluster ...` | Every primary of a Redis Cluster has its own cursor. Call `scan_keys` with cursor 0, or use `scan_all_keys` with a narrow `pattern`. |
| `status: timeout` in `clusters_list` | The cluster's check did not finish within 13 s, for example two or more Redis Cluster startup nodes that accept connections but do not answer, or a slow DNS lookup. See the `Tool did not finish within 15 s` row. |
| `Redis Cluster cannot be connected ...` again at once, right after the cluster was fixed | A failed Redis Cluster connection is remembered for 5 s (see [Redis clients](#redis-clients)). The first call after that tries again. |
| A call fails once with `Error while reading from <host>:<port> : (104, 'Connection reset by peer')`, `Connection closed by server.` or `Timeout reading from socket`, and the next call works | The connection broke while the command was in flight, typically during a restart or a failover. The server sends no command twice, so a write in such a call may or may not have been applied: check before you repeat it. |
| `Too many connections` | More calls ran on the cluster at once than its tools' client has connections, usually because `options` sets `max_connections` below the 8 calls a cluster runs at a time. Remove the option (default 16) or raise it. Pub/sub subscriptions do not count: they have a client of their own. |
| A tool or cluster is missing from the list, or `Unknown tool: <name>` | Its toolset is not in `toolsets`, the cluster is read-only (write tools), `disableDestructive` is on (delete/pop/remove tools), or the cluster does not list the module (`json`/`search` tools). |
| `Input should be '<cluster>'` | The tool is not available on the named cluster (for example a write tool on a read-only cluster); the error lists the allowed values. |
| `Subscription '<id>' was not found` | The pod restarted (subscriptions live in the process) or the subscription was closed. Subscribe again. |

## Development

```
src/
  main.py                    command line (click), single-cluster mode, transport selection
  version.py                 version
  common/
    config_loader.py         YAML loading, --config-dir merge, validation, passwords
    config.py                REDIS_* and REDIS_ENTRAID_* variables of the single-cluster mode, Redis URL parsing
    connection.py            per-cluster client cache and client arguments (standalone, Redis Cluster, Sentinel)
    registry.py              toolsets, tool kinds, tools per config, cluster enum, time limit, worker pools
    server.py                FastMCP server, /healthz
    http.py                  Streamable HTTP: API key and Origin checks, uvicorn
    logging_utils.py         JSON logs (HTTP), stderr logs (stdio)
    entraid_auth.py          Entra ID credential providers
    subscription_manager.py  pub/sub subscriptions
  tools/                     the tools, one module per area (string, hash, list, set, sorted_set, stream, json, ...)
tests/                       pytest suite; redis_servers.py starts throwaway redis-server processes
deploy/openshift/            kustomize manifests and templates
```

| Command | What it does |
|---|---|
| `uv sync` | Creates `.venv` with the dependencies and the dev tools (pytest, ruff, bandit, ...) |
| `uv run pytest` | All tests, with coverage (fails under 80 %; reports in `htmlcov/` and `coverage.xml`) |
| `uv run pytest --no-cov tests/test_config_loader.py` | One test file, without coverage |
| `uv run ruff check src/ tests/` | Lint |
| `uv run ruff format --check src/ tests/` | Format check (`uv run ruff format src/ tests/` fixes it) |
| `uv run bandit -r src/` | Security scan |
| `docker build -t redis-mcp:latest .` | Container image |

- The suite has over 1,500 tests. `tests/test_connection.py` and `tests/test_multi_cluster_integration.py` start
  their own `redis-server` processes on free ports (`tests/redis_servers.py`), and a Redis Cluster of three
  primaries built with `redis-cli`, when those binaries are installed; without them these tests are skipped.
  `tests/test_integration.py` starts the server in the single-cluster mode against the Redis at
  `REDIS_HOST`/`REDIS_PORT` (default `localhost:6379`); its Redis tests are skipped when nothing answers there.
- CI (`.github/workflows/ci.yml`) runs ruff and bandit, runs the tests on Python 3.10 to 3.14 (Ubuntu with
  `redis-server` and `redis-tools` installed, plus Windows and macOS without Redis), builds the wheel and checks
  that `redis-mcp --help` runs from it, and builds the image.
- To add a tool, write a function in a module of `src/tools/` and decorate it with `@redis_tool("<toolset>")`.
  Pass `ToolKind.WRITE` or `ToolKind.DESTRUCTIVE` as the second argument for a tool that changes data,
  `requires="json"` or `requires="search"` for a module tool, and `cluster="optional"` or `cluster="none"` for a
  tool that is not tied to one cluster. The function name becomes the tool name and the docstring its description.
  The `cluster` parameter, the `WRITE:` prefix, the annotations, the time limit and the `tool call` log line are
  added by the registry, and the modules of `src/tools/` are imported automatically. Inside the tool,
  `RedisConnectionManager.get_connection()` returns the client of the call's cluster. Then add the tool to the
  [Tool reference](#tool-reference).

## License

MIT, see [LICENSE](LICENSE). redis-mcp started from redis/mcp-redis; LICENSE keeps its copyright notice.
