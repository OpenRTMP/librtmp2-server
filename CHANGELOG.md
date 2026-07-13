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

[Unreleased]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.3...HEAD
[0.1.3]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.2...v0.1.3
[0.1.2]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.1...v0.1.2
[0.1.1]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/OpenRTMP/librtmp2-server/releases/tag/v0.1.0
