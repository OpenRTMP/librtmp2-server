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

## [0.1.0] — 2026-07-04

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
- Configuration file support (`config.example.env`)
- CLI interface (`./librtmp2-server`) for quick starts
- Docker image (`rust:1-alpine` → `alpine:latest` multi-stage build)
- Unit tests covering config, db, HTTP API, keygen, rate limiting, and the
  RTMP bridge

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

[Unreleased]: https://github.com/OpenRTMP/librtmp2-server/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/OpenRTMP/librtmp2-server/releases/tag/v0.1.0
