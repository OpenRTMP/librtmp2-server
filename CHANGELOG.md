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

### Added
- RTMPS (TLS) support via librtmp2/OpenSSL, toggled by the operator through a
  `tls` config block (`enabled`, `cert_file`, `key_file`) or the
  `LRTMP2_TLS_ENABLED` / `LRTMP2_TLS_CERT_FILE` / `LRTMP2_TLS_KEY_FILE`
  environment variables; off by default
- Build toggles to drop the OpenSSL dependency (`make TLS=0`,
  `cmake -DENABLE_TLS=OFF`); enabling TLS without library support is refused at
  startup with a clear error
- Full RTMP server implementation with librtmp2 integration
- HTTP API with SQLite backend persistence
- Configuration file support (`config.example.json`)
- CLI interface (`./librtmp2-server`) for quick starts
- Integration tests for RTMP:// ingest, client publish, and E-RTMP v1/v2 flows
- Frame logging and diagnostic capabilities
- Asan/UBSan hardened builds
- **Interop tests** — automated end-to-end verification of real-world
  compatibility scenarios:
  - `test_interop_obs` — OBS-style publish/play handshake via librtmp2 client
  - `test_interop_ffmpeg` — FFmpeg-style ingestion and stream lifecycle
  - `test_interop_haishinkkit` — HaishinKit-style mobile publish pattern
  - `test_interop_concurrent_streams` — multiple concurrent publishers/players

### Security
- Input validation for HTTP requests and RTMP streams
- Secure configuration handling with environment variables
- Bounds-checked database operations

### Documentation
- `README.md` for server setup
- `docs/rtmp-server-architecture.md` for architectural overview
- Example configurations for various use cases
- Integration test documentation

### Planned
- REST API enhancements for server management
- Enhanced WebRTC integration
- First tagged pre-release once config and APIs settle
