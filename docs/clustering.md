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
| `CLUSTER_JOIN_PROOF` | — | Required for a fresh join; mint via authenticated `POST /api/v1/cluster/join-proof` |
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
| `CLUSTER_MEDIA_QUEUE_MB` | `64` | Byte bound of each per-peer outbound media queue (and of the local export/inject queues); see [Backpressure](#media-backpressure-live-first) |
| `CLUSTER_MEDIA_MAX_AGE_MS` | `0` (= 3000, **experimental**) | A peer whose queued media gets older than this drops that stream's stale frames and resyncs at the next keyframe; `0` or 100–600000 |
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

Use an **empty** database (no prior `streams` / `raft_*` state). Before starting
the new node, mint a one-time join proof on an existing member using the normal
HTTP API bearer token. The `control_addr` and `media_addr` in this request must
match the addresses the joining node will advertise:

```bash
curl -sS -X POST http://10.0.0.1:8080/api/v1/cluster/join-proof \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "node_id": 2,
    "control_addr": "10.0.0.2:1940",
    "media_addr": "10.0.0.2:1941"
  }'
```

Example response:

```json
{
  "node_id": 2,
  "control_addr": "10.0.0.2:1940",
  "media_addr": "10.0.0.2:1941",
  "proof": "<join-proof>"
}
```

Configure the joining node with the returned `proof`:

```bash
CLUSTER_ENABLED=true
CLUSTER_NODE_ID=2
CLUSTER_JOIN=10.0.0.1:1940
CLUSTER_JOIN_PROOF=<join-proof>
CLUSTER_SECRET=<same-secret>
CLUSTER_BIND=0.0.0.0:1940
CLUSTER_MEDIA_BIND=0.0.0.0:1941
CLUSTER_ADVERTISE_ADDR=10.0.0.2:1940
CLUSTER_MEDIA_ADVERTISE_ADDR=10.0.0.2:1941
LRTMP2_DB=/data/node2.db   # fresh file
```

A fresh join without `CLUSTER_JOIN_PROOF` is rejected before the join request is
sent. The proof is bound to the node ID and advertised control/media addresses,
so mint a new proof if any of those values change.

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
failing. A restart on this `ResumeExisting` path does not require a new
`CLUSTER_JOIN_PROOF`; the proof is only consumed by a fresh join.

**To reseed a node:**

1. Stop the process.
2. Delete `server.db`, `server.db-wal`, `server.db-shm` (or use a new path).
3. Mint a new join proof for the node ID and advertised addresses.
4. Start again with `CLUSTER_JOIN=...` and `CLUSTER_JOIN_PROOF=...` (learner),
   or `CLUSTER_BOOTSTRAP=true` only for a brand-new cluster.

Do **not** copy a live DB from another cluster node and join — that creates
conflicting Raft state.

## HTTP API (Bearer required)

| Method | Path | Purpose |
| --- | --- | --- |
| GET | `/api/v1/health` | Authenticated body includes `cluster` block |
| GET | `/api/v1/cluster` | Cluster status (leader, term, load, quorum, …) |
| GET | `/api/v1/cluster/nodes` | Peer list (panel node fields) |
| GET | `/api/v1/cluster/streams` | Streams + ownership / mesh subscriptions |
| POST | `/api/v1/cluster/join-proof` | Mint a proof authorizing one fresh node join |
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

1. Owner RTMP poll loop: `enable_relay_export` →
   `drain_exported_relay_frames_with_hints` → `ClusterManager::enqueue_export`
   → media hub fan-out.
2. Non-owner play: `notify_play_subscription` → media `SUBSCRIBE` to owner.
3. Inject path: hub → `drain_injects` → `inject_relay_frame` (route key =
   durable stream id / `relay_key`).

### Delivery hints

librtmp2 classifies every exported frame once, where it already parses media
for its init cache, and attaches a codec-neutral `DeliveryHint`:

| Hint | Meaning |
| --- | --- |
| `Critical` | codec headers (sequence headers), metadata/script |
| `ResyncPoint` | a point a receiver can start decoding again: a video keyframe, or any audio frame on a route **without** video |
| `Droppable` | everything else (dependent video frames, audio next to video) |

The cluster never looks at codec payloads (no NAL/OBU parsing); it only reads
the hint, which travels on the media wire.

### Media backpressure (live first)

Each media connection (outbound `MediaPeer` and inbound sink) owns a
`LiveMediaQueue` instead of a plain bounded channel. Freshness beats
completeness for live video, so a peer that falls behind jumps to the live
edge instead of replaying a stale backlog:

* Bounds: `CLUSTER_MEDIA_QUEUE_MB` bytes and 1024 messages, plus an **age**
  bound (`CLUSTER_MEDIA_MAX_AGE_MS`). A single frame larger than half the byte
  bound is refused (it cannot empty the queue).
* **Normal → AwaitingResync**: when a stream's queued media is older than the
  age bound (or the queue hits a bound), the stream's queued non-critical
  frames are discarded and further `Droppable` frames of that stream are
  dropped on arrival. A queued *fresh* keyframe is kept, so the stream is
  already back near live. Otherwise the stream waits for the next
  `ResyncPoint`; that frame is queued and the stream is `Normal` again
  (audio-only streams resync on the next audio frame). Waiting is capped
  (10 s) so a publisher without keyframes is not starved.
* Protected: `Critical` frames and non-media control messages (`Subscribe`,
  `InitCache`, …) are never dropped by the age/resync rules. Only the absolute
  bounds can evict a `Critical` frame, as a last resort, after which the hub
  re-sends the stream's cached init data ahead of the next resync point.
* Fairness: under the absolute bounds the stream holding the most evictable
  bytes loses its frames first (ties: smaller `(app, stream)`), so one
  overloaded stream does not destroy the frames of the others. After a
  reconnect, media queued for the dead connection is discarded the same way.
* The write of one frame is bounded by an 8 s timeout; a peer that stops
  reading is dropped and redialled.

The local `ExportQueue` and `InjectQueue` are separate types (different
consumers) but share one eviction policy: **evict, don't reject** — the
heaviest stream gives up its oldest `Droppable` frame first, then
`ResyncPoint`, and `Critical` only when nothing else of that stream is left;
only a frame larger than the whole queue is refused. (Both used to be
documented as "reject-new"/"drop-oldest" respectively; the inject queue has
always evicted.)

Counters (per peer and summed) are in `GET /api/v1/cluster` under `media`:
`queue_messages`, `queue_bytes`, `oldest_queue_age_ms`,
`dropped_frames_total` = `dropped_droppable_frames` + `dropped_stale_frames` +
`dropped_critical_frames` + `oversized_frames_dropped` (each dropped frame is
counted once), `resync_count`, `resync_timeouts`, `streams_awaiting_resync`,
high-water marks (`max_queue_bytes_seen`, `max_oldest_queue_age_ms_seen`),
`write_timeouts`, `reconnects`, `version_fallbacks`, and the protocol version
of each connection; `export_queue` / `inject_queue` report their evictions.
`dropped_stale_frames` counts frames discarded because they were too old or
belonged to a purged stream (stall, reconnect); `dropped_droppable_frames`
counts frames given up under queue pressure or while awaiting a resync point.

The simulation behind the numbers (two streams, 2 s GOP, 8 s stall) runs as
`cargo test --features cluster,test-support overload_report -- --nocapture`.

### Media protocol v2

`MEDIA_PROTOCOL_VERSION` is now `2` (`1` is still accepted). Authentication,
`Hello` and a possible `Error{VERSION}` are always v1 (JSON) frames, so any two
nodes can read each other's first messages; `Hello.version` then fixes the
framing for the rest of the connection, in both directions:

* v1: `u32` length + JSON of the message (a payload becomes a JSON array of
  numbers — 2–4× the bytes).
* v2: `u32` length + `u8` kind. `MediaFrame` and `InitCache` are compact binary
  records (big endian: hint, frame type, epoch, timestamps, name lengths,
  names, **raw payload**); all other messages are JSON inside a `Control`
  record. Every length is validated against the frame length and hard caps
  (32 MiB frame, 255 B app, 1024 B stream) before anything is allocated, and
  the global read budget is reserved before the body is read. See
  `src/cluster/media/wire.rs`.

An acceptor that supports a `Hello` version newer than 1 confirms it with its
own `Hello`. A v2 node dialing a v1-only node gets `Error{VERSION}` (or a
hang-up instead of the confirmation) and redials with v1 for 60 s, then tries
v2 again; a v1 node dialing a v2 node simply gets v1 framing. **Rolling
upgrades therefore work in any order**; the two encodings are never mixed on
one connection. A v1 node cannot read the `hint` of a v2 peer's frames
(irrelevant: it never forwards them).

## Limitations

- Media inject/export requires librtmp2 ≥ 0.7 APIs (`enable_relay_export`,
  `drain_exported_relay_frames`, `inject_relay_frame`, `stream_init_snapshot`).
- Control/media use shared-secret challenge-response auth; enable
  `CLUSTER_TLS_ENABLED` with cert/key/CA for mTLS in production.
- When mTLS is enabled, each node client certificate must embed its
  `CLUSTER_NODE_ID` as the exact printable string `lrtmp2-node-{id}` in the
  leaf certificate's subject CN or SAN. The authenticated control/media
  `node_id` must match that leaf-certificate identity; issuer certificates
  and arbitrary certificate data are not considered. Existing certificates
  whose CN/SAN merely contains the marker (for example,
  `node-lrtmp2-node-42`) must be reissued before upgrading.
- HA relay export carries live frames only. Peers that join after export
  starts must also fetch `stream_init_snapshot` (or receive init-cache via
  the media mesh `InitCache` subscribe path) before playing.
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
