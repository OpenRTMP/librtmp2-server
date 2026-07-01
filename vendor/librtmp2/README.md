# librtmp2

A modern, open-source **Rust library** for Legacy RTMP and Enhanced RTMP v1/v2.  
`librtmp2` is a complete 1:1 Rust port of the original C `librtmp2` — a reusable protocol foundation, not a media server.

[![License](https://img.shields.io/github/license/OpenRTMP/librtmp2)](LICENSE)
![Status](https://img.shields.io/badge/status-alpha-orange)
![Language](https://img.shields.io/badge/language-Rust-orange)

---

## Overview

`librtmp2` implements the lowest protocol layer of RTMP: handshake, chunking, AMF, commands, audio/video tags, Enhanced RTMP extensions, state machine, and a clean callback API.

It is designed to be embedded into custom servers, clients, relay tools, OBS/FFmpeg integrations, or any future product that needs a solid RTMP foundation.

**What it is:**
- A complete Legacy RTMP implementation (handshake, chunk streams, message reassembly, commands, control messages)
- E-RTMP v1 support: ExVideoTagHeader, FourCC codecs (`hvc1`, `av01`, `vp09`), HDR metadata
- E-RTMP v2 support: capability negotiation (`capsEx`, `videoFourCcInfoMap`), reconnect, multitrack, ModEx
- An idiomatic Rust API plus an `extern "C"` FFI layer for use from C, Go, Python, PHP, and others
- RTMPS (RTMP over TLS) via the optional `tls` Cargo feature (OpenSSL), enabled by default

**What it is not:**
- Not an HTTP server or web UI
- Not a media server with business logic
- Not a push-relay to third-party platforms
- Not an FFmpeg wrapper

---

## Architecture

```text
OBS / FFmpeg / App
        │
        ▼
      librtmp2          ← this library
      ├── Handshake
      ├── Chunking
      ├── AMF (AMF0 / AMF3)
      ├── RTMP Commands
      ├── E-RTMP v1
      └── E-RTMP v2
```

The library processes bytes, frames, commands, and protocol states. What a host program does with them is decided entirely via callbacks and configuration structures.

---

## State Machine

```text
TCP_ACCEPTED
  → HANDSHAKE
  → CONNECTED
  → CAPS_NEGOTIATED     (E-RTMP v2)
  → APP_CONNECTED
  → STREAM_CREATED
  → PUBLISHING | PLAYING
  → CLOSING
  → CLOSED
```

---

## Build

```bash
# Debug build
cargo build

# Release build
cargo build --release

# Run tests
cargo test

# Build without TLS (no OpenSSL dependency)
cargo build --no-default-features
```

The crate uses `crate-type = ["cdylib", "staticlib", "lib"]` and produces:
- `librtmp2.so` / `librtmp2.dll` — cdylib for FFI consumers
- `librtmp2.a` / `librtmp2.lib` — staticlib for FFI consumers
- Rust `lib` — for direct Cargo dependency

### TLS / RTMPS

RTMPS (RTMP over TLS) is supported via OpenSSL and is **enabled by default** via the `tls` Cargo feature. To produce a plaintext-only build without the optional TLS/OpenSSL dependency:

```bash
cargo build --no-default-features
```

Call `lrtmp2_tls_supported()` at runtime to check whether the library was built with TLS.

---

## Using as a Rust Crate

Add to your `Cargo.toml`:

```toml
[dependencies]
librtmp2 = { path = "../librtmp2" }
```

Without TLS:

```toml
[dependencies]
librtmp2 = { path = "../librtmp2", default-features = false }
```

---

## Using via the `extern "C"` FFI

The crate also builds as a `cdylib`/`staticlib` and exposes a stable `extern "C"` API for use from C, Go, Python, PHP, and others. See `src/lib.rs` for the full FFI surface.

### Server

```c
lrtmp2_server_t *lrtmp2_server_create(const lrtmp2_server_config_t *config);
void             lrtmp2_server_destroy(lrtmp2_server_t *server);
int              lrtmp2_server_listen(lrtmp2_server_t *server, const char *bind_addr);
int              lrtmp2_server_poll(lrtmp2_server_t *server, int timeout_ms);
void             lrtmp2_server_stop(lrtmp2_server_t *server);
```

### Client

```c
lrtmp2_client_t *lrtmp2_client_create(const lrtmp2_server_config_t *config);
void             lrtmp2_client_destroy(lrtmp2_client_t *client);
int              lrtmp2_client_connect(lrtmp2_client_t *client, const char *url);
int              lrtmp2_client_publish(lrtmp2_client_t *client);
int              lrtmp2_client_play(lrtmp2_client_t *client);
int              lrtmp2_client_send_frame(lrtmp2_client_t *client, const lrtmp2_frame_t *frame);
int              lrtmp2_client_poll(lrtmp2_client_t *client, int timeout_ms);
```

### Utilities

```c
int          lrtmp2_tls_supported(void);
const char  *lrtmp2_version_string(void);
int          lrtmp2_version_major(void);
int          lrtmp2_version_minor(void);
int          lrtmp2_version_patch(void);
const char  *lrtmp2_error_string(int code);
```

---

## Repository Structure

```text
librtmp2/
├── src/
│   ├── lib.rs              Rust API + extern "C" FFI layer
│   ├── alloc.rs            Custom allocator hook
│   ├── amf.rs              AMF0 + AMF3 encoding/decoding
│   ├── buffer.rs           Growable byte buffers
│   ├── bytes.rs            Big-endian byte helpers
│   ├── chunk.rs            Chunk reader/writer/state (per-csid)
│   ├── client.rs           Outbound client: connect → publish/play
│   ├── ertmp.rs            E-RTMP v1/v2 extensions
│   ├── flv.rs              FLV audio/video/script tag parsing
│   ├── handshake.rs        C0/C1/C2 ↔ S0/S1/S2
│   ├── log.rs              Logging
│   ├── message.rs          Message reassembly, control, commands
│   ├── net.rs              Network helpers
│   ├── server.rs           Listening socket, accept loop, per-connection poll
│   ├── session.rs          Connection state machine, stream bookkeeping
│   ├── transport.rs        TLS/plaintext transport abstraction
│   └── types.rs            Shared types
├── tests/
│   ├── interop/
│   └── server_client_loopback.rs
├── build.rs
├── Cargo.toml
└── docs/
```

---

## Roadmap

| Feature | Status |
|---------|--------|
| Legacy RTMP server — minimal | Implemented |
| Legacy RTMP client — minimal | Implemented |
| E-RTMP v1 receive (HEVC/AV1 detection) | Implemented |
| E-RTMP v1 send | Implemented |
| E-RTMP v2 capability layer | Implemented |
| Multitrack / reconnect / ModEx | Implemented |
| RTMPS (TLS) | Implemented |
| End-to-end test suites | In progress |
| Performance benchmarks | Planned |

---

## License

MIT — see [LICENSE](LICENSE)
