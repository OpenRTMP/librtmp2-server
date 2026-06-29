# Changelog

All notable changes to this project will be documented in this file.

> ⚠️ **Alpha software.** `librtmp2-server` is in active early development. It has
> **no fixed, stable release version yet** — everything below is pre-release
> (alpha) and configuration, APIs, and behavior may change at any time without
> notice. Pin to a specific git commit if you depend on it.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
While in alpha the project stays on `0.x`; semantic-versioning guarantees only
begin at `1.0.0`.

## [Unreleased] — alpha

### Changed
- **Rewritten in Rust.** The C implementation (CMake/Makefile build, Mongoose
  HTTP server, hand-rolled JSON/SQLite glue) has been fully replaced by a Rust
  crate built on `axum` (HTTP API) and `rusqlite` (SQLite persistence).
  Config, db, HTTP API, CLI, logging, and key generation are all native Rust
  now; the HTTP API surface is unchanged.
- The RTMP/E-RTMP protocol implementation is **not** part of this crate. It is
  developed separately as a Rust `librtmp2` crate and plugs into this server
  through the [`RtmpEventHandler`](src/rtmp_bridge.rs) trait
  (`on_connect` / `on_publish` / `on_play` / `on_frame` / `on_close`), which
  replaces the old C function-pointer + `userdata` callback contract.
- CI (`tests.yml`, `release.yml`) now runs `cargo fmt --check`,
  `cargo clippy --all-targets -- -D warnings`, `cargo build`, and
  `cargo test` instead of the old CMake/ctest pipeline.
- `Dockerfile` is now a `rust:1-alpine` → `alpine:latest` multi-stage build.

### Added
- RTMPS (TLS) configuration support — toggled by the operator through a `tls`
  config block (`enabled`, `cert_file`, `key_file`) or the
  `LRTMP2_TLS_ENABLED` / `LRTMP2_TLS_CERT_FILE` / `LRTMP2_TLS_KEY_FILE`
  environment variables; validated at startup (enabling TLS without both a
  cert and key file is refused with a clear error). Off by default. Actual
  RTMPS termination lands once the RTMP listener is wired in.
- HTTP API with SQLite backend persistence
- Configuration file support (`config.example.json`)
- CLI interface (`./librtmp2-server`) for quick starts
- Unit tests covering config, db, HTTP API, keygen, and the RTMP bridge seam

### Security
- Input validation for HTTP requests
- Secure configuration handling with environment variables
- Constant-time Bearer token comparison
- Weak/placeholder API token rejection

### Documentation
- `README.md` updated for the Rust build/run/architecture

### Planned
- Wire the Rust `librtmp2` crate's RTMP listener into `RtmpEventHandler`
- REST API enhancements for server management
- First tagged pre-release once config and APIs settle
