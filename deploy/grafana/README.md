# Grafana: Redis Cluster Overview

`redis-cluster-dashboard.json` is a Grafana dashboard for Redis clusters monitored with
**Telegraf `inputs.redis` → InfluxDB (InfluxQL)**.

## Requirements

- Grafana 10.x (built for 10.3; uses `palette-classic-by-name`, table gauge cells, `negative-Y`).
- InfluxDB 1.x / InfluxQL datasource. The JSON references datasource uid `000000021` —
  change it on import (the import dialog lets you pick a datasource) or search/replace the uid.
- Telegraf `inputs.redis` measurements: `redis`, `redis_keyspace`, `redis_cmdstat`, with the
  standard `replication_role` tag (`master` / `slave`) and these custom tags on every point:
  `instance_group`, `instance_name`, `instance_node` (one value per Redis instance).
- Collection interval ≤ 30s. Every panel has **Min interval = 30s**; if Telegraf collects less
  often, raise it (it is the `"interval"` key on each panel) so no bucket is empty.

## Layout

| Row | What it answers |
|---|---|
| Cluster Overview | Is the cluster healthy right now? Nodes / masters / replicas, clients, ops/s, memory, hit rate, rejected connections, a per-node status table and load distribution. |
| Performance | Where is the load? Ops/s and clients per node, network in/out, CPU per node, rejected connections. |
| Memory | Are we running out of memory or fragmenting? Used memory, fragmentation ratio, evicted/expired keys, used vs RSS. |
| Keyspace | How much data, and is the cache effective? Keys (masters only), keys per node, hit rate per node, hits vs misses, keys with TTL. |
| Replication & Persistence | Are replicas in sync and is data safe on disk? Connected replicas per master, sync events, master link I/O age, fork duration, RDB changes since last save, time since last save. |
| Commands (collapsed) | Which commands are hot or slow? Calls/s and latency for the selected commands, top 10 commands. |

Variables: `Instance Group`, `Instance Name`, `Node` (multi, defaults to All), `Command`
(multi; only used by the Commands row).

## Why the cluster totals are stable

Cluster-wide numbers (`sum` over nodes) are computed from a subquery that uses
`GROUP BY time($__interval), "instance_node" fill(previous)`: a node that has not yet reported
inside the last, still-open interval keeps its previous value instead of dropping out of the sum.
Point-in-time counts (nodes online, masters, replicas, per-node bars, the status table) do not
bucket by time at all — they take `last()` per node over the last 2 minutes.
