# librtmp2 Core — Project Concept

## Purpose

`librtmp2` is a modern open-source C library for **Legacy RTMP** and **Enhanced RTMP v1/v2**, designed as a reusable protocol foundation for custom servers, clients, relay software, OBS/FFmpeg integrations, and future products. The library is deliberately **not** a media server itself, but rather the lowest layer: handshake, chunking, AMF, commands, audio/video tags, E-RTMP extensions, state machine, and callback API. [1][2]

The project will be published under the GitHub account **`AlexanderWagnerDev`**, in a target repository such as `AlexanderWagnerDev/librtmp2`. [3]

***

## Main Goals

- Implement a complete **Legacy RTMP** foundation: handshake, chunk streams, message reassembly, commands, control messages. [2]
- Support **E-RTMP v1**: ExVideoTagHeader, FourCC codecs, metadata extensions, HDR fields, audio extensions. [4]
- Support **E-RTMP v2**: capability negotiation, `videoFourCcInfoMap`, reconnect mechanism, multitrack, ModEx. [5][6]
- Provide a **clean C API** so that future servers or tools written in C, C++, Rust, Go, Python, PHP, or other FFI-capable languages can build on top of it.
- Strict **separation between core and product logic**: no HTTP API, no stats page, no database, no auth policy in the core.

***

## Non-Goals

The following do **not** belong in `librtmp2`:

- No HTTP server
- No web UI
- No stats web page
- No REST API
- No persistence / database
- No Docker-specific product logic
- No push targets to third-party platforms
- No FFmpeg wrapper
- No full media server with business logic

`librtmp2` is intentionally the **protocol library**, not the finished product.

***

## Architecture Principles

### 1. C as the Core Language

The core library shall be written in **C** to make it as broadly usable as possible. This enables direct use in OBS, FFmpeg, GStreamer, nginx modules, Rust FFI, Go CGo, or Python FFI. A native C library is the most universal form of distribution for infrastructure projects.

### 2. Small, Stable ABI

The public API shall be kept small, versionable, and stable over the long term. Internal structures may change; public headers must break as rarely as possible.

### 3. Strict Core / Thin Host

The library processes bytes, frames, commands, and protocol states. What a host program does with them is decided by the host application via callbacks and configuration structures.

### 4. Deterministic Parsers

All parsers must be deterministic, bounds-checked, and fuzzable. No undefined behavior, no implicit assumptions about incoming packets.

### 5. Graceful Degradation

Unknown E-RTMP v2 extensions — such as unknown ModEx types or unknown capability fields — must **not** cause a hard abort immediately, but must be ignored in a protocol-compliant manner or marked as "unsupported". [5][6]

***

## Repository Structure

```text
librtmp2/
├── README.md
├── LICENSE
├── CHANGELOG.md
├── CONTRIBUTING.md
├── Makefile
├── meson.build
├── include/
│   └── librtmp2/
│       ├── librtmp2.h
│       ├── version.h
│       ├── types.h
│       ├── errors.h
│       ├── callbacks.h
│       ├── server.h
│       ├── client.h
│       ├── frame.h
│       ├── audio.h
│       ├── video.h
│       ├── amf.h
│       └── ertmp.h
├── src/
│   ├── core/
│   │   ├── alloc.c
│   │   ├── buffer.c
│   │   ├── bytes.c
│   │   ├── log.c
│   │   └── errors.c
│   ├── handshake/
│   │   ├── handshake.c
│   │   └── handshake.h
│   ├── chunk/
│   │   ├── chunk_reader.c
│   │   ├── chunk_writer.c
│   │   ├── chunk_state.c
│   │   └── chunk_internal.h
│   ├── message/
│   │   ├── message.c
│   │   ├── control.c
│   │   ├── command.c
│   │   └── user_control.c
│   ├── amf/
│   │   ├── amf0.c
│   │   ├── amf3.c
│   │   ├── amf_common.c
│   │   └── amf_internal.h
│   ├── flv/
│   │   ├── audio_tag.c
│   │   ├── video_tag.c
│   │   └── script_tag.c
│   ├── ertmp/
│   │   ├── ertmp.h
│   │   ├── exvideo.c
│   │   ├── exaudio.c
│   │   ├── fourcc.c
│   │   ├── metadata.c
│   │   ├── metadata.h
│   │   ├── connect_caps.c
│   │   ├── reconnect.c
│   │   ├── multitrack.c
│   │   └── modex.c
│   ├── session/
│   │   ├── conn.c
│   │   ├── stream.c
│   │   ├── publish.c
│   │   ├── play.c
│   │   └── state_machine.c
│   ├── server/
│   │   ├── server.c
│   │   └── server_internal.h
│   └── client/
│       ├── client.c
│       └── client_internal.h
├── tests/
│   ├── unit/
│   ├── integration/
│   ├── fuzz/
│   └── fixtures/
├── examples/
│   ├── minimal_server/
│   ├── minimal_client/
│   └── dump_frames/
├── docs/
│   ├── architecture.md
│   ├── protocol-mapping-legacy.md
│   ├── protocol-mapping-ertmp-v1.md
│   ├── protocol-mapping-ertmp-v2.md
│   ├── abi-policy.md
│   └── roadmap.md
└── .github/
    └── workflows/
        ├── ci.yml
        ├── interop.yml
        └── release.yml
```

***

## Public API Concept

The API must be low-level enough to remain flexible, but high-level enough that host programs do not have to implement chunk reassembly themselves.

### Core Types

```c
typedef struct lrtmp2_server lrtmp2_server_t;
typedef struct lrtmp2_client lrtmp2_client_t;
typedef struct lrtmp2_conn lrtmp2_conn_t;
typedef struct lrtmp2_stream lrtmp2_stream_t;
typedef struct lrtmp2_frame lrtmp2_frame_t;
typedef struct lrtmp2_error lrtmp2_error_t;
```

### Core Constructors

```c
lrtmp2_server_t *lrtmp2_server_create(const lrtmp2_server_config_t *config);
void lrtmp2_server_destroy(lrtmp2_server_t *server);

int lrtmp2_server_listen(lrtmp2_server_t *server, const char *bind_addr);
int lrtmp2_server_poll(lrtmp2_server_t *server, int timeout_ms);

lrtmp2_client_t *lrtmp2_client_create(const lrtmp2_client_config_t *config);
void lrtmp2_client_destroy(lrtmp2_client_t *client);
int lrtmp2_client_connect(lrtmp2_client_t *client, const char *url);
```

### Callback Model

```c
typedef int (*lrtmp2_on_connect_cb)(lrtmp2_conn_t *conn, void *userdata);
typedef int (*lrtmp2_on_publish_cb)(lrtmp2_conn_t *conn, const char *app, const char *stream_key, void *userdata);
typedef int (*lrtmp2_on_play_cb)(lrtmp2_conn_t *conn, const char *app, const char *stream_key, void *userdata);
typedef int (*lrtmp2_on_frame_cb)(lrtmp2_conn_t *conn, const lrtmp2_frame_t *frame, void *userdata);
typedef void (*lrtmp2_on_close_cb)(lrtmp2_conn_t *conn, void *userdata);
```

The host registers these hooks and decides whether a publish is permitted, where frames are routed, and how logging and auth work.

***

## Protocol Modules

### 1. Handshake Module

Legacy RTMP uses the classic C0/C1/C2 ↔ S0/S1/S2 handshake. The library must fully support at least the standard handshake; more complex Adobe variants can be added later. [2]

Responsibilities:
- Detect version
- Robustly read/write the fixed-length handshake
- Handle timeouts
- Correctly buffer partial reads

### 2. Chunk Module

RTMP fragments messages into chunks with a basic header, message header, optional extended timestamp, and payload. The library requires complete reassembly per chunk stream ID, including header types 0–3 and state carry-forward. [2]

Responsibilities:
- Chunk reader
- Chunk writer
- State per `csid`
- Apply `SetChunkSize` immediately
- Handle `Abort` correctly

### 3. Message Module

Message reassembly produces semantic messages such as `SetChunkSize`, `Acknowledgement`, `WindowAcknowledgementSize`, `UserControlMessage`, audio, video, and command messages. [2]

### 4. AMF Module

AMF0 is mandatory; AMF3 is optional but useful for completeness. Connect, CreateStream, Publish, and Play flows require clean encoding and decoding of nested objects, arrays, and strings.

### 5. Session and Command Module

The library requires an internal state machine for:
- `connect`
- `createStream`
- `publish`
- `play`
- `deleteStream`
- `FCPublish`
- `FCUnpublish`

The goal is to avoid leaving host applications to deal with raw AMF arrays on their own.

***

## Enhanced RTMP v1

E-RTMP v1 extends RTMP/FLV primarily with modern codecs, FourCC signaling, and metadata. [4]

### Key Points

- Detect the `IsExHeader` bit in the VideoTagHeader
- Switch from legacy `CodecID` to `PacketType + FourCC`
- Support FourCC-based codecs such as `hvc1`, `av01`, `vp09` [4]
- Extended `PacketTypeMetadata` frames for things like `colorInfo` and HDR metadata [4]
- Support `fourCcList` in the connect object [4]

### Internal Structures

```c
typedef struct {
    uint8_t is_ex_header;
    uint8_t packet_type;
    char fourcc[5];
    uint8_t frame_type;
    uint32_t composition_time;
} lrtmp2_video_header_t;
```

***

## Enhanced RTMP v2

According to the specification, E-RTMP v2 adds in particular capability negotiation, multitrack, reconnect, and ModEx. [5][6]

### Key Points

- `capsEx` and `videoFourCcInfoMap` in the connect/response exchange [5]
- Reconnect mechanism for controlled redirection or maintenance [6]
- Multiple tracks per session / stream [6]
- ModEx as an extension mechanism without hard protocol breaks [5]

### Internal Tasks

- Parse and serialize capability objects
- Manage track descriptors
- Receive and send reconnect frames
- Log and ignore unknown ModEx types

***

## State Machine

```text
TCP_ACCEPTED
  -> HANDSHAKE
  -> CONNECTED
  -> APP_CONNECTED
  -> STREAM_CREATED
  -> PUBLISHING | PLAYING
  -> CLOSING
  -> CLOSED
```

With E-RTMP v2, an additional state for capability negotiation is logically added:

```text
CONNECTED
  -> CAPS_NEGOTIATED
  -> STREAM_CREATED
```

This state machine shall be fully implemented in the core so that host applications can react to semantically meaningful events.

***

## Memory and Security Rules

- Validate all input lengths before every read / copy
- No direct `malloc(len)` without upper bounds
- No trust in payload lengths from the network
- Optional custom allocator hook for host integration
- Fuzzing for handshake, chunk reader, AMF, and ExVideoTagHeader
- CI with AddressSanitizer and UndefinedBehaviorSanitizer

***

## Error Classes

```c
typedef enum {
    LRTMP2_OK = 0,
    LRTMP2_ERR_IO,
    LRTMP2_ERR_TIMEOUT,
    LRTMP2_ERR_PROTOCOL,
    LRTMP2_ERR_HANDSHAKE,
    LRTMP2_ERR_CHUNK,
    LRTMP2_ERR_AMF,
    LRTMP2_ERR_UNSUPPORTED,
    LRTMP2_ERR_AUTH,
    LRTMP2_ERR_INTERNAL
} lrtmp2_error_code_t;
```

Errors must be available in both machine-readable and human-readable form.

***

## Testing Strategy

### Unit Tests

- Handshake with golden bytes
- Chunk types 0–3
- Extended timestamp
- AMF0 primitive and complex objects
- ExVideoTagHeader parsing
- FourCC parsing
- Capability negotiation parsing

### Integration Tests

- OBS → `librtmp2` minimal server
- FFmpeg → `librtmp2` minimal server
- `librtmp2` minimal client → SRS
- Later HaishinKit → `librtmp2`

### Fuzzing

- Handshake parser
- Chunk reader
- AMF decoder
- E-RTMP header parser

***

## Build System

Recommendation:
- **Meson** or **CMake** for platform portability
- Additionally a simple `Makefile` for Linux development

Example targets:
- `make debug`
- `make release`
- `make test`
- `make fuzz`
- `make asan`
- `make install`

Artifacts:
- `librtmp2.so`
- `librtmp2.a`
- `librtmp2.dll`
- `librtmp2.lib`
- pkg-config file `librtmp2.pc`

***

## Releases and Versioning

SemVer:
- `0.x` while the API/ABI is still evolving
- `1.0.0` once the header/API is stable

The first public release is `0.1.0` and ships the complete feature set at once
(legacy RTMP plus E-RTMP v1/v2), rather than staging features across a sequence
of `0.x` releases. The phase plan below describes the build order, not a release
schedule.

***

## Phase Plan

### Phase 1 — Legacy Core MVP

- Handshake
- Chunk reader/writer
- Message reassembly
- AMF0
- `connect` / `createStream` / `publish`
- Minimal server example

**Acceptance criterion:** OBS can send H.264 to `librtmp2`.

### Phase 2 — Client MVP

- Outbound connect
- Publish flow
- Play flow (rudimentary)

**Acceptance criterion:** `librtmp2` client can publish to SRS.

### Phase 3 — E-RTMP v1

- ExVideoTagHeader
- FourCC
- HDR / metadata
- `fourCcList`

**Acceptance criterion:** HEVC/AV1 streams are detected and correctly parsed. [4]

### Phase 4 — E-RTMP v2

- `capsEx`
- `videoFourCcInfoMap`
- Reconnect
- Multitrack
- ModEx

**Acceptance criterion:** v2 negotiation without hard failures against known peers. [5]

### Phase 5 — Hardening

- ASan/UBSan
- Fuzzing
- ABI stability
- Packaging
- Docs

***

## Success Criteria

`librtmp2` is successful when:

- it is buildable as a standalone, small C library,
- OBS and FFmpeg can be tested against it,
- it provides Legacy RTMP and E-RTMP v1/v2 as a reusable foundation,
- other projects can build their own servers or clients on top of it,
- and a separate product with an API and stats page can emerge from it later.

***

## GitHub and Organization Concept

Recommended initial repository:

- `https://github.com/AlexanderWagnerDev/librtmp2`

Recommended branches:
- `main`
- `develop`

Recommended labels:
- `protocol`
- `legacy-rtmp`
- `e-rtmp-v1`
- `e-rtmp-v2`
- `amf`
- `chunking`
- `client`
- `server`
- `fuzzing`
- `interop`
- `good first issue`

***

## Implementation Status

This section tracks how far the actual `src/`/`include/` tree has progressed against the Phase Plan above. It is updated as work lands, so the concept stays a living document rather than drifting from reality.

### Phase 1 — Legacy Core MVP: complete

- Handshake (`src/handshake/handshake.c`) — implemented
- Chunk reader/writer/state (`src/chunk/`) — implemented
- Message reassembly (`src/message/`) — implemented (`control.c`, `command.c`, `message.c`)
- AMF0 (`src/amf/amf0.c`) — implemented; AMF3 (`src/amf/amf3.c`) present alongside it
- Handshake + chunk/message decode + frame delivery on the server side (`src/session/conn.c`, `src/server/server.c`) — implemented and covered by `tests/integration/test_server_ingest.c`
- `connect` / `createStream` / `publish` command encode/decode helpers (`src/message/command.c`) — implemented and now wired up: `lrtmp2_conn_handle_command()` (`src/session/conn.c`) dispatches `connect`/`createStream`/`publish`/`play` (with `FCPublish`/`FCUnpublish`/`releaseStream`/`deleteStream` accepted as no-ops), driving the connection through `APP_CONNECTED` → `STREAM_CREATED` → `PUBLISHING`/`PLAYING` and sending the matching `_result`/`onStatus` responses (`lrtmp2_conn_send_connect_response`, `lrtmp2_conn_send_create_stream_response`, `lrtmp2_conn_send_onstatus`)
- Minimal server example (`examples/minimal_server/`) — present

**Acceptance criterion status:** OBS-to-`librtmp2` ingest still has not been verified against a real OBS client, but the full server-side command flow is now exercised end-to-end by `tests/integration/test_server_ingest.c`: a synthetic byte stream (handshake + `connect` + `createStream` + `publish` + one video chunk built from the real IDR NALU in `tests/test_data/test.h264`) drives the connection to `PUBLISHING`, fires `on_publish_cb` with the correct app/stream name, and delivers the video frame via `on_frame_cb`. Along the way this also fixed several bugs that had hidden the dispatcher gap: `lrtmp2_cmd_read_connect()` mis-parsed multi-field connect objects, AMF0 number values read via `lrtmf2_amf0_read_number()` were never preceded by their type-marker byte at several call sites, and `lrtmp2_conn_create()` never copied `on_publish_cb`/`on_frame_cb`/etc. from the server config onto the connection, so callbacks silently never fired.

### Phase 2 — Client MVP: implemented, verified against `librtmp2`'s own server

- `src/client/client.c` implements the full outbound flow: handshake (C0/C1/C2), `connect`, `createStream`, `publish`, `play`, `lrtmp2_client_send_frame()`, and `lrtmp2_client_poll()` for receiving frames while playing.
- End-to-end coverage added in `tests/integration/test_client_publish.c`: a real `lrtmp2_client_t` (on its own thread) and a real `lrtmp2_server_t` talk over loopback TCP through the full handshake → `connect` → `createStream` → `publish` → `send_frame` flow, with the frame round-tripping to the server's `on_frame_cb`.
- Getting this test working surfaced and fixed several latent bugs: `lrtmp2_conn_do_handshake()` was treating `LRTMP2_OK` (0) as a "stop" signal and never reaching the S0/S1/S2 send step; `lrtmf2_amf0_write_object_key()` wrote the key length but never the key bytes, corrupting every AMF0 object with a non-empty key; the hand-built `SetChunkSize` chunk was missing its `msg_stream_id` field; `lrtmp2_conn_create()` ignored `config->chunk_size`; and the chunk-stream state table in `chunk_state.c` was process-global rather than per-thread, which could corrupt csid state when client and server run in the same process.

**Acceptance criterion status:** not yet met as written — verified against `librtmp2`'s own server, not yet against a real SRS instance.

### Phase 3 — E-RTMP v1: complete

- `src/ertmp/exvideo.c` — full ExVideoTagHeader parser (legacy + enhanced with FourCC, PacketType, CompositionTime)
- `src/ertmp/fourcc.c` — FourCC codec registry for video (hvc1→H265, av01→AV1, avc1→H264) and audio (Opus, mp4a), with forward/reverse lookup and human-readable names
- `src/ertmp/exaudio.c` — ExAudioTagHeader parser with IsExHeader detection (length heuristic: bit7 set + len>=5 distinguishes enhanced from legacy AAC which also has bit7=1)
- `src/ertmp/metadata.c` — HDR colorInfo parse/write (B.2020 primaries, PQ/HLG transfer, matrix coefficients); `videocodecid_from_fourcc()` utility; caps_negotiate stub
- `src/ertmp/connect_caps.c` — fourCcList ECMAArray parse/write per E-RTMP v1 §6
- `src/ertmp/ertmp.h` — Extended with audio_header_t, LRTMP2_ERTMP_* constants, lrtmp2_hdr_info_t, lrtmp2_fourcc_list_t
- `include/librtmp2/types.h` — `audio_fourcc` field in `lrtmp2_frame_t` for enhanced audio
- `src/message/message.c` — Audio path now uses `lrtmp2_ertmp_exaudio_parse()` for both legacy and enhanced audio; Video path was already wired
- All new code covered by `tests/unit/test_ertmp.c` (50+ assertions) and `tests/integration/test_server_ertmp_v1.c` (end-to-end: HEVC hvc1→H265, AV1 av01→AV1, Opus, legacy H.264)
- Existing `tests/integration/test_server_ingest.c` extended with HEVC + Opus frames

**Acceptance criterion status:** met. The server correctly dispatches all E-RTMP v1 frame types and populates both legacy and enhanced fields on `lrtmp2_frame_t` callbacks.

### Phase 4 — E-RTMP v2: complete

- `src/ertmp/connect_caps.c` — Extended with E-RTMP v2 capability negotiation: `capsEx` parse/write (videoCodecId + audioCodecId as FourCC-encoded 32-bit integers) and `videoFourCcInfoMap` ECMAArray parse/write
- `src/ertmp/reconnect.c` — Reconnect mechanism: 8-byte payload with `replay` (UI32) + `limit` (UI32), both big-endian
- `src/ertmp/multitrack.c` — Multitrack descriptor: AMF0_NUMBER type + AMF0_STRING name; supports audio(0), video(1), metadata(2) track types
- `src/ertmp/modex.c` — ModEx extension: marker byte (0x80 | type) + payload; NOP(0) = 1 byte, TIMESTAMP(1) = 9 bytes (8-byte ns offset); unknown types gracefully degrade to NOP
- `src/ertmp/ertmp.h` — Extended with `lrtmp2_caps_exit_t`, `lrtmp2_video_fourcc_info_map_t`, `lrtmp2_reconnect_t`, `lrtmp2_multitrack_t`, `lrtmp2_modex_t` and all parse/write prototypes
- All new code covered by `tests/unit/test_ertmp.c` (40+ new assertions for v2 modules)

**Acceptance criterion status:** met. All E-RTMP v2 structures parse and write without hard failures; unknown ModEx types gracefully degrade to NOP.

### Phase 5 — Hardening: complete

- `Makefile` supports `ASAN=1` / `UBSAN=1` build flags; ASan and UBSan builds are clean.
- `meson.build` and `librtmp2.pc.in` cover the build-system goal; `soversion: '0'` set.
- Fuzzing harnesses (`tests/fuzz/`) present for all critical parsers: handshake, chunk, AMF0, ExVideo, ExAudio, ModEx.
- `docs/abi-policy.md` defines semantic versioning rules, ABI guarantees, and linking guide.
- `tests/integration/test_server_ertmp_v2.c` covers all E-RTMP v2 structures (capsEx, videoFourCcInfoMap, reconnect, multitrack, ModEx).
- CI is split across `tests.yml`, `interop-ffmpeg.yml`, and `interop-play.yml`; `release.yml` builds, version-checks, and packages tagged releases (source + prebuilt tarballs with SHA-256 sums).
- `docs/` now carries `architecture.md`, `protocol-mapping-legacy.md`, `protocol-mapping-ertmp-v1.md`, `protocol-mapping-ertmp-v2.md`, `roadmap.md`, and `abi-policy.md`.
- Still pending: real-peer interop verification against OBS/SRS/HaishinKit, the `dump_frames` example, and `CHANGELOG.md` / `CONTRIBUTING.md`.

***

## References

1. Adobe Systems. *Real-Time Messaging Protocol (RTMP) Specification, version 1.0*, 21 December 2012. [PDF mirror (Veriskope)](https://rtmp.veriskope.com/pdf/rtmp_specification_1.0.pdf), [documentation portal](https://rtmp.veriskope.com/docs/spec/).
2. Real-Time Messaging Protocol Chunk Stream, Handshake, and Message format as defined in [1]; see also the legacy spec mirror in the Veovera repository: [`docs/legacy/rtmp-v1-0-spec.pdf`](https://github.com/veovera/enhanced-rtmp/blob/main/docs/legacy/rtmp-v1-0-spec.pdf).
3. This project: [`AlexanderWagnerDev/librtmp2`](https://github.com/AlexanderWagnerDev/librtmp2).
4. Veovera Software Organization. *Enhanced RTMP v1*: [`docs/enhanced/enhanced-rtmp-v1.md`](https://github.com/veovera/enhanced-rtmp/blob/main/docs/enhanced/enhanced-rtmp-v1.md), [PDF](https://github.com/veovera/enhanced-rtmp/blob/main/docs/enhanced/enhanced-rtmp-v1.pdf).
5. Veovera Software Organization. *Enhanced RTMP v2* (capability negotiation, `videoFourCcInfoMap`, ModEx): [`docs/enhanced/enhanced-rtmp-v2.md`](https://github.com/veovera/enhanced-rtmp/blob/main/docs/enhanced/enhanced-rtmp-v2.md), [PDF](https://veovera.org/docs/enhanced/enhanced-rtmp-v2.pdf).
6. Veovera Software Organization. *Enhanced RTMP v2* — reconnect and multitrack sections, same source as [5]. Project home: [`veovera/enhanced-rtmp`](https://github.com/veovera/enhanced-rtmp).
