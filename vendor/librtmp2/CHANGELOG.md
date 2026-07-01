# Changelog

All notable changes to this project will be documented in this file.

> ⚠️ **Alpha software.** `librtmp2` is in active early development. It has **no
> fixed, stable release version yet** — everything below is pre-release (alpha)
> and the API/ABI may change at any time without notice. Pin to a specific git
> commit if you depend on it.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
While in alpha the project stays on `0.x`; semantic-versioning guarantees only
begin at `1.0.0`.

## [Unreleased] — alpha

### Added
- TLS / RTMPS support via OpenSSL, built in by default (disable with
  `make TLS=0` / `meson -Dtls=disabled` for a zero-dependency build)
- Transport abstraction (`src/core/transport.{h,c}`) under the raw send/recv
  path so plaintext RTMP and TLS share a single code path
- Server-side TLS termination via `tls_enabled` / `tls_cert_file` /
  `tls_key_file` in `lrtmp2_server_config`
- Client-side `rtmps://` connect with SNI and certificate verification
  (`tls_ca_file`, `tls_insecure` to tune verification)
- `lrtmp2_tls_supported()` runtime capability check
- Transport unit tests and an end-to-end RTMPS integration test
- Legacy RTMP protocol support (handshake, chunk, message, AMF0)
- Enhanced RTMP v1 support (ExVideo/ExAudio headers, FourCC registry, HDR/colorInfo)
- Enhanced RTMP v2 support (capsEx, reconnect, multitrack, ModEx)
- Full server API with callbacks (`on_connect`, `on_publish`, `on_play`, `on_frame`, `on_close`)
- Full client API with publish/play flows
- Frame API supporting audio, video, script, and metadata types
- H.264, H.265, AV1, and legacy video codec support
- AAC, Opus, MP3, G.711 audio codec support
- `dump_frames` example for stream debugging
- Meson build system with subproject support
- pkg-config file (`librtmp2.pc`)
- Comprehensive unit and integration tests
- ASan/UBSan support for hardening
- Fuzz harnesses for all critical parsers

### Security
- Bounds-checked parsers for all network-provided length fields
- Constant-time RNG for handshake
- Safe handling of unknown E-RTMP v2 ModEx types (degrades to NOP)

### Documentation
- `CLAUDE.md` with build commands and architecture guide
- `docs/abi-policy.md` with ABI compliance checklist
- Protocol mapping documents for legacy, E-RTMP v1, and E-RTMP v2
- Example programs (`minimal_server`, `minimal_client`, `dump_frames`)

### Planned
- OBS → librtmp2 interop verification
- SRS → librtmp2 interop verification
- HaishinKit interop verification
- Automated ABI compliance checks in CI
- `CONTRIBUTING.md` guidelines
- First tagged pre-release once the API settles
