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

[Unreleased]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.0...v0.1.1
[0.1.0]: https://github.com/OpenRTMP/librtmp2-server/releases/tag/v0.1.0
