# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- RTMPS (TLS) support via librtmp2/OpenSSL, toggled by the operator through a
  `tls` config block (`enabled`, `cert_file`, `key_file`) or the
  `LRTMP2_TLS_ENABLED` / `LRTMP2_TLS_CERT_FILE` / `LRTMP2_TLS_KEY_FILE`
  environment variables; off by default
- Build toggles to drop the OpenSSL dependency (`make TLS=0`,
  `cmake -DENABLE_TLS=OFF`); enabling TLS without library support is refused at
  startup with a clear error

## [0.1.0] - 2025-06-27

### Added
- Full RTMP server implementation with librtmp2 integration
- HTTP API with SQLite backend persistence
- Configuration file support (`config.example.json`)
- CLI interface (`./librtmp2-server`) for quick starts
- Integration tests for RTMP:// ingest, client publish, and E-RTMP v1/v2 flows
- Frame logging and diagnostic capabilities
- Asan/UBSan hardened builds

### Security
- Input validation for HTTP requests and RTMP streams
- Secure configuration handling with environment variables
- Bounds-checked database operations

### Documentation
- `README.md` for server setup
- `docs/rtmp-server-architecture.md` for architectural overview
- Example configurations for various use cases
- Integration test documentation

## [Unreleased]

### Planned
- OBS → librtmp2-server interop verification
- ffmpeg → librtmp2-server interop verification
- HaishinKit interop verification
- REST API for server management (planned for 0.2.0)
- Enhanced WebRTC integration (planned for 0.2.0)