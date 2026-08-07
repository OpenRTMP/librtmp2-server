# Clustering (optional HA)

`librtmp2-server` can run as a multi-node cluster using **OpenRaft 0.9** for
durable state and a separate **media mesh** for inter-node relay. Clustering is
**off by default**. Standalone behavior is unchanged when `CLUSTER_ENABLED=false`.

## Build

```bash
cargo build --release --features cluster
# Docker image builds with --features cluster; runtime still defaults off.
```

Without the `cluster` Cargo feature, OpenRaft and TLS peer deps are not linked.

## Architecture

| Plane | Port (default) | Role |
| --- | --- | --- |
| Control | `CLUSTER_BIND` `1940` | Raft RPC, join/admin, heartbeats, StatsProxy (authenticated) |
| Media | `CLUSTER_MEDIA_BIND` `1941` | Multiplexed media frames, subscribe, init-cache |
| RTMP | `1935` | Client publish/play (unchanged) |
| HTTP | `8080` | Admin API + health |

```
┌────────────┐   Raft/control :1940    ┌────────────┐
│  Node A    │◄───────────────────────►│  Node B    │
│ (leader)   │   media mesh :1941      │ (follower) │
│ SQLite+SM  │◄───────────────────────►│ SQLite+SM  │
└─────▲──────┘                         └─────▲──────┘
      │ RTMP/HTTP                            │ RTMP/HTTP
   publishers/players                   players (relay)
```

- **One SQLite DB per node** (`LRTMP2_DB`). Raft log/vote/snapshots live in
  `raft_*` tables in the same file; app tables (`streams`, `stream_viewers`,
  `settings`, `stream_owners`) are the state machine.
- **No central media proxy**, no mandatory Postgres/Redis.
- Publisher **ownership** is acquired via Raft (`AcquireStreamOwner`) with
  epoch fencing on media frames. Acquire happens **before** the local
  publisher slot. A minority partition cannot steal ownership.
- Heartbeats are **ephemeral** (not Raft). Dead owners are released only after
  quorum-aware failure detection on the leader.
- Durable mutations (stream/viewer/token/ownership) go through
  `StateCoordinator` → Raft `client_write`. Reads stay on the local DB.
- Writes that land on a follower are forwarded to the current leader over the
  authenticated control plane (`ClientWrite`); API clients never need to
  discover the Raft leader.
- `CreateStream` Raft commands include a pre-generated default viewer so every
  replica applies identical IDs (no per-node keygen on apply).

## Configuration

Set in `.env` or via `LRTMP2_CLUSTER_*` process overrides:

| Variable | Default | Notes |
| --- | --- | --- |
| `CLUSTER_ENABLED` | `false` | Master switch |
| `CLUSTER_NODE_ID` | — | Required, positive integer |
| `CLUSTER_BIND` | `0.0.0.0:1940` | Control plane |
| `CLUSTER_MEDIA_BIND` | `0.0.0.0:1941` | Media plane |
| `CLUSTER_BOOTSTRAP` | `false` | First voter; mutually exclusive with JOIN |
| `CLUSTER_JOIN` | — | Address of an existing control peer |
| `CLUSTER_SECRET` | — | Shared secret (≥16 chars); never logged |
| `CLUSTER_TLS_ENABLED` | `false` | mTLS for control/media when true |
| `CLUSTER_TLS_CERT_FILE` / `KEY` / `CA` | — | Required if TLS enabled |
| `CLUSTER_HEARTBEAT_MS` / `CLUSTER_HEARTBEAT_INTERVAL_MS` | `500` | Peer heartbeat interval |
| `CLUSTER_CAPACITY` | `1.0` | Admission headroom (0–1) |
| `CLUSTER_CAPACITY_MBPS` | — | Alternate: absolute capacity; sets drain/resume ratios vs Mbps |
| `CLUSTER_DRAIN_THRESHOLD` / `CLUSTER_DRAIN_AT_MBPS` | `0.85` | Enter DRAINING |
| `CLUSTER_RESUME_THRESHOLD` / `CLUSTER_RESUME_AT_MBPS` | `0.70` | Leave DRAINING (hysteresis) |
| `CLUSTER_BANDWIDTH_INTERFACE` | — | Optional iface for load |
| `CLUSTER_BANDWIDTH_MODE` | `tx` | `tx` / `rx` / `max` / `sum` |
| `CLUSTER_BANDWIDTH_MAX_MBPS` | `0` | Denominator for utilization |
| `CLUSTER_MEDIA_REPLICAS` | `0` | Standby mesh fan-out |
| `CLUSTER_MEDIA_QUEUE_MB` | `32` | Per-peer backpressure |
| `CLUSTER_ADVERTISE_ADDR` | — | Control addr peers dial (defaults to loopback rewrite of BIND) |
| `CLUSTER_MEDIA_ADVERTISE_ADDR` | — | Media addr peers dial |

### Bootstrap (first node)

```bash
CLUSTER_ENABLED=true
CLUSTER_NODE_ID=1
CLUSTER_BOOTSTRAP=true
CLUSTER_SECRET=<long-random-secret>
CLUSTER_BIND=0.0.0.0:1940
CLUSTER_MEDIA_BIND=0.0.0.0:1941
CLUSTER_ADVERTISE_ADDR=10.0.0.1:1940
CLUSTER_MEDIA_ADVERTISE_ADDR=10.0.0.1:1941
```

Existing standalone streams/viewers/token are seeded into Raft on first bootstrap.
A `cluster_id` UUID is written via Raft (`SetClusterId`) and included in snapshots.
`JoinResponse` returns `cluster_id` plus known peer control/media addresses so the
joiner can heartbeat and open media mesh links.

### Join (additional node)

Use an **empty** database (no prior `streams` / `raft_*` state):

```bash
CLUSTER_ENABLED=true
CLUSTER_NODE_ID=2
CLUSTER_JOIN=10.0.0.1:1940
CLUSTER_SECRET=<same-secret>
LRTMP2_DB=/data/node2.db   # fresh file
```

Joined nodes start as **learners**. Promote to voter after catch-up:

```http
POST /api/v1/cluster/nodes/{id}/promote
Authorization: Bearer <token>
```

## Reseed

Join is refused if the local DB has populated streams **without** raft state, or
a leftover `cluster_id` without raft membership.

**Existing member restart:** if `CLUSTER_JOIN` is still set but local `raft_*`
state already exists, the node **resumes** (skips the join handshake) instead of
failing.

**To reseed a node:**

1. Stop the process.
2. Delete `server.db`, `server.db-wal`, `server.db-shm` (or use a new path).
3. Start again with `CLUSTER_JOIN=...` (learner) or `CLUSTER_BOOTSTRAP=true`
   only for a brand-new cluster.

Do **not** copy a live DB from another cluster node and join — that creates
conflicting Raft state.

## HTTP API (Bearer required)

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/v1/health` | Authenticated body includes `cluster` block |
| GET | `/api/v1/cluster` | Cluster status (leader, term, load, quorum, …) |
| GET | `/api/v1/cluster/nodes` | Peer list (panel node fields) |
| GET | `/api/v1/cluster/streams` | Streams + ownership / mesh subscriptions |
| POST | `/api/v1/cluster/nodes/{id}/drain` | Mark node DRAINING |
| POST | `/api/v1/cluster/nodes/{id}/resume` | Mark node READY |
| POST | `/api/v1/cluster/nodes/{id}/promote` | Promote learner → voter |
| DELETE | `/api/v1/cluster/nodes/{id}` | Remove voter (releases its stream owners) |

Public `/api/v1/health` stays minimal (`{"status":"ok"}`).

### Authenticated health `cluster` block

`enabled`, `cluster_id`, `node_id`, `node_name`, `role`, `leader_id`, `term`,
`quorum`, `state`, and a `load` object (`rx_mbps`, `tx_mbps`, `capacity_mbps`,
`admission`).

### Node JSON

`id`, `name`, `role`, `voter`, `state`, `healthy`, `rx_mbps`, `tx_mbps`,
`capacity_mbps`, `publishers`, `players`, `last_heartbeat`.

### Stream cluster JSON

`stream_id`, `owner_node_id`, `epoch`, `subscribed_nodes`, `standby_nodes`,
`cluster_players`.

### Stats proxy

Authenticated `GET /api/v1/streams/{id}/stats` on a non-owner node attempts a
control-plane `StatsProxy` fetch from the owner and embeds it under
`cluster_proxy` when available.

## Health states

`READY`, `DRAINING`, `DOWN`, `ISOLATED`, `JOINING`, `LEARNER`, `LEAVING`
(API `state` fields are lowercase).

Admission hysteresis uses drain/resume thresholds so load flaps do not flip
ingress eligibility rapidly.

## Media path

1. Owner RTMP poll loop: `enable_relay_export` → `drain_exported_relay_frames`
   → `ClusterManager::enqueue_export` → media hub fan-out.
2. Non-owner play: `notify_play_subscription` → media `SUBSCRIBE` to owner.
3. Inject path: hub → `drain_injects` → `inject_relay_frame` (route key =
   durable stream id / `relay_key`).

## Limitations

- Media inject/export requires librtmp2 ≥ 0.7 APIs (`enable_relay_export`,
  `drain_exported_relay_frames`, `inject_relay_frame`, `stream_init_snapshot`).
- Control/media use shared-secret challenge-response auth; enable
  `CLUSTER_TLS_ENABLED` with cert/key/CA for mTLS in production.
- Invalid `CLUSTER_ENABLED=true` config fails startup hard (no silent standalone fallback).
- Automatic learner→voter promotion is available via
  `POST /api/v1/cluster/nodes/{id}/promote` but not forced on every join.
- Interface bandwidth probing is best-effort (Linux sysfs; Windows may report 0).
- Aggregate cluster Mbps in status is derived from local load × capacity until
  full cross-node metering lands.

## Docker

Dockerfile builds with `--features cluster`. Expose control/media when running
a cluster:

```yaml
ports:
  - "1935:1935"
  - "8080:8080"
  - "1940:1940"   # cluster control
  - "1941:1941"   # cluster media
```

## Tests

```powershell
$env:PATH = "C:\Users\alexg\.cargo\bin;" + $env:PATH
cargo test --features test-support
cargo test --features cluster,test-support
```

`tests/cluster_ha.rs` covers 3-node bootstrap, create via leader/follower,
ownership conflict/failover, subscription/timeline units, and reseed refusal.
