# Changelog

All notable changes to this project will be documented in this file.

> ⚠️ **Alpha software.** `librtmp2-server` is in active early development. It has
> **no fixed, stable release version yet** — everything below is pre-release
> (alpha) and configuration, APIs, and behavior may change at any time without
> notice. Pin to a specific git commit if you depend on it.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
While in alpha the project stays on `0.x`; semantic-versioning guarantees only
begin at `1.0.0`.

## [Unreleased]

### Fixed
- Outbound media-peer readers stamp the authenticated peer id so inbound
  `MediaFrame`/`InitCache` require `accepts_owner` (same fence as direct accepts).
- Failed outbound `Subscribe` try_send rolls back the subscription refcount so
  later players can retry instead of assuming the owner is already forwarding.
- Standalone Docker builds pin the cloned `librtmp2` checkout to `Cargo.toml`'s
  `rev` (override with `LIBRTMP2_REF`) instead of tracking moving `main`.
- Join rejects address replacement for an already-active member node id until
  that member is `DOWN` or `LEAVING`.
- Session-hook force-unpublish/drain clones hooks before nested lock re-entry
  (membership Leaving, isolation reconcile, ownership sync).
- Heartbeats carry per-viewer player counts; play admission enforces the
  per-play-key connection cap cluster-wide against that cache.
- Failed first `Subscribe` rolls back only a sole refcount; concurrent holders
  keep their refs and get a retry send so they are not stranded without wire
  Subscribe.
- A node pruned out of Raft membership enters `Leaving`, force-drains, and
  force-unpublishes local streams so it does not keep serving after removal.
- `force_drain` sets the manual-drain flag without weakening stronger health
  states (`Leaving`/`Isolated`/`Down`/`Learner`) to `Draining`.
- Raft state-machine apply treats every `ClusterResponse::Error` as a storage
  failure so `last_applied` cannot advance past a partial mutation.
- Followers that are already members may proxy `JoinRequest` for a fresh
  learner to the leader (join via non-leader `CLUSTER_JOIN` address).
- Outbound media TLS handshakes are bounded by the same auth timeout as the
  control plane.
- Timed-out `BeginDeleteStream` keeps the local delete marker and schedules
  finalize reconciliation instead of abandoning the workflow.
- Inbound media accepts are capped (`MAX_MEDIA_CONN_INFLIGHT`) like control
  connections.
- Quorum recovery counts reachable `ISOLATED` peers so a restored majority can
  leave isolation.
- Snapshot restore emits `DrainStream` for pending-delete streams and
  `RevokeViewer` for players whose viewer row did not survive.
- Stream-switch publish rejects when prior ownership release fails (keeps the
  prior epoch; rolls back the new claim) instead of leaking the old owner.
- Ownership changes preserve media subscription refcounts per active player;
  unreachable peers keep their last cached session counts individually.
- Init-cache fields are cleared when the ownership epoch changes.
- Exported relay frames are stamped only with the publisher connection's
  claimed epoch (no durable/current-stream fallback).
- File-based absolute drain/resume Mbps thresholds re-ratio when
  `BANDWIDTH_MAX` is overridden via process env.
- Ambiguous timed-out acquires stay queued until the ownership row appears
  (or is superseded) instead of being dropped on the first empty lookup.
- Ownership sync force-unpublishes local publishers when the applied owner
  moves away from this node after Raft catch-up.
- Failed ownership releases on publisher close are retried from the RTMP poll
  loop; removed-node ownership cleanup is queued when the release write fails.
- `FinalizeDeleteStream` deletes only while `pending_delete=1`, so stale
  recovery cannot wipe a re-enabled stream.
- Snapshot restore drains live sessions for streams fully absent from the
  snapshot (not only pending-delete ids).
- Media peer reconnect aborts a blocked reader; control-plane snapshot RPCs
  accept up to the 64 MiB frame limit.
- Cluster status `healthy_nodes` excludes `ISOLATED` peers (same predicate as
  per-node `healthy`).
- `scripts/docker_cargo_test.py` exits nonzero when any cargo suite fails.
- `AcquireStreamOwner` on a missing stream returns `NotFound` (not storage
  `Error`) so Raft apply cannot stall after a racing finalize-delete.
- Inbound media frames require the authenticated peer to be the recorded owner
  (node id + epoch), not epoch alone.
- Topology resume fallbacks skip the local node; cluster Mbps totals include
  peer heartbeat load; snapshot control RPCs use a 120s round-trip budget;
  member-forwarded admin `ClientWrite` commands are accepted on the control
  plane; Docker monorepo builds `[patch]` the sibling `librtmp2` path.
- Ownership acquire rejects disabled/pending-delete streams; peer session
  caches survive DOWN until ownership release; standby placement skips
  unavailable peers; invalid cluster boolean env overrides error; concurrent
  last-viewer delete races return 400; security test matches control-plane
  policy.

## [0.2.0] — 2026-08-08

### Added
- Optional high-availability clustering behind Cargo feature `cluster`
  (OpenRaft 0.9.24 control plane, SQLite state machine, media mesh on
  ports 1940/1941). Runtime default remains `CLUSTER_ENABLED=false`
  (standalone unchanged). See `docs/clustering.md`.
- `StateCoordinator` routes durable stream/viewer/token/ownership
  mutations through local SQLite or Raft.
- Authenticated cluster APIs: `GET /api/v1/cluster`, `/nodes`, `/streams`,
  `POST .../nodes/{id}/drain|resume`, `DELETE .../nodes/{id}`; health
  includes panel-shaped `cluster` block when enabled.
- Raft `CreateStream` carries a pre-generated default viewer for identical
  replica applies; StatsProxy for non-owner stream stats.
- Multi-node loopback tests in `tests/cluster_ha.rs`.
- Depends on `librtmp2` 0.7 path (relay export / inject / init snapshot).
- Follower durable writes forward to the Raft leader over the authenticated
  control plane (`ClientWrite`); clients never need leader discovery.

### Changed
- Package version `0.1.9` → `0.2.0`.
- Official Docker image builds with `--features cluster` on
  `rust:1.97-bookworm` / `debian:bookworm-slim`; clustering stays disabled
  unless configured. Compose documents ports 1940/1941.

## [0.1.9] — 2026-08-06

### Security
- The HTTP rate limiter's route classification for `/stats*` now matches
  only the exact `/stats` and `/stats-nginx` paths; previously any unmatched
  path starting with `/stats` (e.g. `/stats-<random>`) got its own bucket,
  letting a client mint unbounded rate-limit buckets by hitting unique
  unmatched paths instead of falling under the shared default bucket.
- The admin API rate limiter now previews Bearer authentication before
  bucketing: authenticated `/api/*` requests still draw from
  `HTTP_RATE_LIMIT_API` as before, but unauthenticated `/api/*` requests —
  including the public `GET /api/v1/health` — now draw from
  `HTTP_RATE_LIMIT_DEFAULT` under a separate "public" bucket instead of the
  authenticated admin budget. This closes a path where a client sharing an
  IP with an admin (NAT, reverse proxy) could exhaust the admin API's
  request budget without a valid token. `/stats` and `/stats-nginx` again
  track independent budgets from each other.
- `publisher_update`/`player_update` now re-validate the single-active-
  publisher and per-viewer connection-cap invariants when a row transitions
  into an active slot (or moves to a different stream/viewer while active),
  closing a path where an RTMP stream-switch rollback could leave a "ghost"
  active row that blocked new publishes until restart.
- Publish/play rejections that occur after a successful key lookup (disabled
  stream, pending delete, publisher slot already taken, play connection cap)
  now count toward the per-IP auth-failure rate-limit budget, closing a side
  channel that let a remote peer distinguish a valid key from a guess by
  whether the attempt consumed rate-limit quota.
- `/stats`, `/stats-nginx`, and `GET /api/v1/streams/{id}/stats` now return
  identical "offline" responses for an invalid `stats_key` and a stream with
  no active publisher, closing the `stats_key` enumeration oracle for that
  case. `/stats-nginx` can still distinguish a valid key from an invalid one
  when the stream has an active player but no publisher, since its response
  includes connected-viewer data.

### Fixed
- `-p`/`-w` CLI overrides no longer rewrite `RTMP_BIND`/`HTTP_BIND` to
  `0.0.0.0:{port}`; they now replace only the port and preserve the
  configured host (including bracketed IPv6 literals), so overriding the
  port no longer silently exposes a previously localhost-only listener on
  all interfaces.
- `publisher_update`/`player_update` skip their active-slot uniqueness/cap
  re-check on routine stats-flush updates that leave a row's slot unchanged
  (`active=true`, same stream/viewer), instead of re-running the indexed
  scan on every periodic update.

## [0.1.8] — 2026-07-24

### Added
- `RTMP_MAX_CONNECTIONS_PER_ADDR` / `LRTMP2_RTMP_MAX_CONNECTIONS_PER_ADDR`
  configure the maximum number of active RTMP/RTMPS connections accepted from
  one source IP.
- `RTMP_MAX_PENDING_TLS_PER_ADDR` /
  `LRTMP2_RTMP_MAX_PENDING_TLS_PER_ADDR` independently configure the number of
  incomplete RTMPS handshakes retained per source IP. Both per-IP settings
  default to `i32::MAX` in the server application to preserve the previous
  behavior for deployments behind NAT, load balancers, or proxies; operators
  can opt into stricter limits explicitly.

### Changed
- Bump the `librtmp2` dependency to the crates.io release **0.5.0**. This
  includes the new `ServerConfig::max_connections_per_addr` field, so both
  server and test configuration literals now set the active-connection and
  pending-TLS per-address limits explicitly.
- README and `.env.example` now document both the `.env` names (`RTMP_*`) and
  direct process-environment overrides (`LRTMP2_RTMP_*`), including the
  remaining global admission-control backstop when the per-IP TLS limit is
  effectively disabled.

### Security
- The `librtmp2` 0.5.0 upgrade closes post-connect idle-slot exhaustion,
  separates active-connection and pending-TLS per-IP admission caps, bounds
  accept-loop work per poll, and prevents stalled partial RTMP messages from
  retaining large duplicate chunk scratch buffers.
- Cached init-frame replay is deduplicated and rate-limited, publisher cache
  eviction is isolated per publisher, and multitrack codec authorization
  validates every contained track.

### Fixed
- The inherited RTMP session teardown paths now consistently clear
  publish/play/paused state, refresh the idle grace window only after real
  active-to-idle transitions, and allow valid connections to publish or play
  again without being prematurely disconnected.
- Reconnects are no longer rejected against stale per-IP connection counts,
  and newly accepted sockets are processed during the same server poll tick.
- Client AMF3 data handling avoids an unnecessary intermediate payload copy.

## [0.1.7] — 2026-07-21

### Added
- Connection and access logging aligned with srt-live-server style: RTMP
  accept/publish/play/release/disconnect and kicks now include the peer
  `IP:port`, and HTTP `/stats`, `/stats-nginx`, `/stat.xsl`, and per-stream
  stats requests log client IP, status, and stream id. Admin stream/play-key
  mutations and HTTP 429 rate-limit hits are logged with client IP as well.
- Docker startup logs now print an OpenRTMP ASCII banner followed by the
  `librtmp2-server` name and running image version. Release builds embed the
  workflow version, while local builds fall back to the package version from
  `Cargo.toml`.

### Security
- Bump the `librtmp2` dependency to the crates.io release **0.4.2**, which
  bounds `Client::publish()`/`Client::play()`'s blocking AMF exchange to the
  configured connect-timeout wall-clock deadline instead of allowing
  indefinite blocking, strictly UTF-8-validates route strings (app/stream
  names), rejects embedded NUL bytes in `read_string_checked()` instead of
  copying them, and has the server session layer reject empty app/stream
  names and gate metadata relay to players on callback registration.

### Fixed
- (via the `librtmp2` 0.4.2 bump) `bytes_received` tracking now uses 64-bit
  integers instead of 32-bit, so pacing stays correct after a connection
  exceeds 4 GiB of inbound data. Client Aggregate-message playback now
  passes sub-tag slices directly to callbacks instead of cloning each into
  separate vectors.

## [0.1.6] — 2026-07-18

### Fixed
- Release the DB publisher/player role and sync `relay_key` when a session
  drops publish/play without closing the TCP connection (FCUnpublish /
  closeStream / publish↔play switch), instead of leaving stale role and
  relay-routing state behind.
- `release_publisher`/`release_player` now retry the DB deactivation on
  failure and arm a stats rebase for the connection's next session, instead
  of losing track of an `active=1` row or misattributing prior-session bytes
  to the next one.
- Restart the idle-eviction window and re-enable relay when a role survives
  mid-session teardown, so a client that intends to republish/replay shortly
  isn't judged against a stale `first_seen_at` or left without relay.
- Clear tracked codec strings and `conn.pending_relay` when the last role on
  a connection ends mid-session, so a later publish/play on the same
  connection doesn't inherit stale codec metadata or a buffered relay queue.
- Only clear a connection's tracked publish/play role after the bridge
  confirms the DB deactivation actually succeeded, so idle eviction can't
  reclaim a connection whose role row is still active and blocking others.

### Changed
- Bump the `librtmp2` dependency to the crates.io release **0.4.1**, which
  tracks the exact claimed publish-route key independently of `relay_key`
  and clears `Stream.is_playing` on `publish()`.

## [0.1.5] — 2026-07-15

### Changed
- Bump the `librtmp2` dependency to the crates.io release **0.4.0** (E-RTMP v2 connect negotiation,
  multitrack relay, Enhanced-RTMP init-cache/onMetaData replay, legacy pause/seek).
- Update README protocol notes to match inherited `librtmp2` 0.4.0 behaviour.
- Update the RTMP HTTP E2E test `Frame` initializer for the expanded 0.4.0
  `librtmp2::types::Frame` shape, including the optional multitrack `track_id`.

## [0.1.4] — 2026-07-13

### Fixed
- Stream delete no longer re-enables publish/play keys when the 30-second
  wait for active RTMP sessions times out; the stream stays disabled
  (`pending_delete=1`) so operators can retry once sessions drop.

### Security
- RTMP publish/play/media callbacks now register the connection for auth
  tracking before authorization runs, so per-IP auth-failure rate limits
  apply even when `publish` arrives before `on_connect` processes the
  session.
- Auth-failure rate limiting uses a per-connection bucket when the remote
  IP is not yet known, instead of sharing one empty-key bucket across all
  such sessions.
- `rtmp_media_cb` now fails closed (`unwrap_or(false)`) when the bridge
  lock is unavailable, instead of accepting media frames.
- Auth-failure rate limiting now rejects untracked remote IPs when the
  per-IP failure map is fully saturated, instead of silently allowing
  further attempts.
- Rate-limited auth-failure buckets are no longer evicted from the failure
  map, so a saturated map cannot reset an IP's lockout window early.

### Changed
- Bump the pinned `librtmp2` dependency to `0.3.1`, pulling in bounded DNS
  resolution during client connect, nonblocking ping/pong handling during
  publish and poll, server-side connect-setup and stale-ping timeouts, and
  capped DNS worker queue depth. No code changes were needed on this side:
  the connection fields this crate reads off `librtmp2::session::conn::Conn`
  (`client_fd`, `conn_id`, `remote_addr`, `relay_enabled`, `relay_key`,
  `pending_relay`, `rtt_ms`) are unchanged.

## [0.1.3] — 2026-07-12

### Changed
- Bump the pinned `librtmp2` dependency to `0.3.0`, pulling in a fix for
  `ServerConfig.tls_ca_file`/`ServerConfig.tls_insecure` being silently
  ignored by `lrtmp2_client_create()` — the client previously always
  verified `rtmps://` peers against only the system trust store regardless
  of those fields. No code changes were needed on this side: the new
  `Transport::connect_tls()` parameters (`ca_file`, `insecure`) are a
  Rust-only API addition this crate doesn't call directly, and the FFI/ABI
  surface (`tls_ca_file`, `tls_insecure` on `ServerConfig`) is unchanged.

## [0.1.2] — 2026-07-10

### Added
- `GET /stat.xsl` — a dark-themed XSLT stylesheet for `/stats-nginx`. The
  XML response now links it via an `<?xml-stylesheet?>` processing
  instruction, so opening `/stats-nginx?key=<stats_key>` directly in a
  browser renders a readable table instead of raw XML — the same mechanism
  `nginx-rtmp-module`'s classic `stat.xsl` uses, just restyled for dark
  mode. Layout mirrors the classic table: split video (codec/bits-per-
  second/size/fps) and audio (codec/bits-per-second/freq/channels)
  sub-columns, in/out bytes and bitrate, live/offline state, and
  expandable per-client detail (publisher vs. player, dropped frames) —
  no extra page chrome, just the stats table.

### Fixed
- `/stats-nginx`'s `<meta>` element now always emits both `<video>` and
  `<audio>` children — as an empty self-closing element if that codec
  wasn't detected (e.g. a video-only publisher). NOALBS's `Nginx` provider
  models `meta` as requiring both children (neither is optional in its
  Rust struct); a `<meta>` with only one of them failed to deserialize
  there, and NOALBS treated the whole stream as unreachable/offline even
  though it was live. Verified against NOALBS's actual `quick_xml`
  deserialization code.
- `/stats-nginx` now emits stream-level `bw_audio`/`bw_video` and self-closing
  `active`/`publishing` markers, matching real `nginx-rtmp-module` output.
  Tools that consume nginx-rtmp XML — e.g. [NOALBS](https://github.com/NOALBS/nginx-obs-automatic-low-bitrate-switching)'s
  `Nginx` stream server — read `bw_video` for bitrate and stream-level
  `active` for publish state; without these fields they always saw a
  stalled/offline stream. No API shape change, only additional XML fields.
- `build_nginx_xml()` now emits one `<stream>` element per stream name, with
  one `<client>` child per connected session (publisher and players alike),
  matching how `nginx-rtmp-module` structures its XML. Previously a
  publisher and each of its players got separate `<stream>` blocks; once a
  viewer connected, its player entry — sharing the same (possibly redacted)
  stream name — could sort after the publisher's and shadow the real
  bitrate with `bw_video=0` in consumers that pick the last matching
  `<stream>`, such as NOALBS's `Nginx` stream server.
- README's NOALBS example now documents that `/stats-nginx` always redacts
  the application/stream name to `live`/`stream`, and that the NOALBS
  `Nginx` provider's `application`/`key` config fields must be set to those
  fixed values rather than the real stream name.
- The merged `<stream>` element only carries `<active/>`/`<publishing/>`
  while a publisher is actually live. A leftover player session with no
  publisher (broadcaster dropped, viewer connection not yet torn down) no
  longer gets marked `<active/>` with `bw_video=0` — NOALBS's `Nginx`
  provider treats "active present + 0 bitrate" as "keep the previous
  scene", not offline, so the stale marker was masking real disconnects.
- Publisher `<video>`/`<audio>` blocks in `/stats-nginx` are now nested
  inside a `<meta>` element, matching `nginx-rtmp-module`'s schema. NOALBS's
  `Nginx` provider reads codec/resolution info from `stream/meta/video` and
  `stream/meta/audio` for its `source_info()` chat command; without the
  wrapper that data never matched and the command always came back empty.

### Changed
- Bump the pinned `librtmp2` dependency to `0.2.1`, pulling in RTMPS client
  hardening (bounded TLS handshake timeout, write-readiness polling on read
  retries, EINTR retry in transport polling), RTMP Aggregate message
  support, and the FFI/recv-path security fixes described in `librtmp2`'s
  own changelog. No code changes were needed on this side: the connection
  fields this crate reads off `librtmp2::session::conn::Conn` (`client_fd`,
  `conn_id`, `remote_addr`, `relay_enabled`, `relay_key`, `pending_relay`,
  `rtt_ms`) are unchanged.

## [0.1.1] — 2026-07-10

### Changed
- Bump the pinned `librtmp2` dependency to `0.2.0`, pulling in RTMPS client
  hardening (bounded TLS handshake timeout, write-readiness polling on read
  retries, EINTR retry in transport polling), RTMP Aggregate message
  support, and the FFI/recv-path security fixes described in `librtmp2`'s
  own changelog. No code changes were needed on this side: the connection
  fields this crate reads off `librtmp2::session::conn::Conn` (`client_fd`,
  `conn_id`, `remote_addr`, `relay_enabled`, `relay_key`, `pending_relay`,
  `rtt_ms`) are unchanged.

## [0.1.0] — 2026-07-08

First tagged pre-release. `librtmp2-server` is a Rust crate built on `axum`
(HTTP API) and `rusqlite` (SQLite persistence). The RTMP/E-RTMP protocol
implementation is developed separately as the `librtmp2` crate and plugs into
this server through the [`RtmpEventHandler`](src/rtmp_bridge.rs) trait
(`on_connect` / `on_publish` / `on_play` / `on_frame` / `on_close`); the RTMP
listener (`src/server.rs`) drives a real `librtmp2::server::Server` over both
plaintext RTMP and RTMPS.

### Added
- RTMP and RTMPS (TLS) listeners, unified onto a single `librtmp2::server::Server`
  so plaintext and TLS clients share one relay — toggled by the operator
  through the `tls` config block (`enabled`, `cert_file`, `key_file`) or the
  `LRTMP2_TLS_ENABLED` / `LRTMP2_TLS_CERT_FILE` / `LRTMP2_TLS_KEY_FILE`
  environment variables; validated at startup (enabling TLS without both a
  cert and key file is refused with a clear error). Off by default.
- HTTP API with SQLite backend persistence (streams, publishers, players, stats)
- Key-based access control (`publish_key`, `play_key`, `stats_key`), including
  optional operator-supplied custom keys
- JSON and Nginx-compatible XML stats endpoints
- Configuration file support (`.env.example`)
- CLI interface (`./librtmp2-server`) for quick starts
- Docker image (`rust:1-alpine` → `alpine:latest` multi-stage build)
- Unit tests covering config, db, HTTP API, keygen, rate limiting, and the
  RTMP bridge

### Changed
- Standardized the config file name on `.env` (was `config.env`); the example
  template is now `.env.example`. The server loads `.env` by default, and the
  Docker image starts without an explicit `-c` path.

### Fixed
- Avoid redundant `on_connect` re-registration on every publish/play callback
  for an already-registered connection
- Register the client's `remote_addr` inside publish/play callbacks during
  `poll()` so per-IP auth failure tracking applies before the first
  publish/play attempt, closing a rate-limit bypass race

### Security
- Input validation and rate limiting for HTTP requests
- Secure configuration handling with environment variables
- Constant-time Bearer token comparison
- Weak/placeholder API token rejection
- Per-key connection caps for RTMP publish/play (the RTMP auth path itself is
  not rate-limited, so operator-supplied custom keys have an enforced minimum
  length to resist brute-forcing)

### Documentation
- `README.md` updated for the Rust build/run/architecture

### Planned
- REST API enhancements for server management

[Unreleased]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.9...v0.2.0
[0.1.9]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.8...v0.1.9
[0.1.8]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.7...v0.1.8
[0.1.7]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.6...v0.1.7
[0.1.6]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.5...v0.1.6
[0.1.5]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.4...v0.1.5
[0.1.4]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.3...v0.1.4
[0.1.3]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/OpenRTMP/librtmp2-server/releases/tag/v0.1.0
