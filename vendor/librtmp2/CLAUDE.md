# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

**Makefile (primary, for development):**
```bash
make debug          # debug build (-g -O0 -DDEBUG)
make release        # release build (-O2 -DNDEBUG)
make test           # build + run unit tests (tests/run_tests)
make asan           # debug build + AddressSanitizer
make ubsan          # debug build + UndefinedBehaviorSanitizer
make clean          # remove all build artifacts
make install        # install to /usr/local (PREFIX= to override)
```

**Running with sanitizers (CI pattern):**
```bash
make clean && make DEBUG=1 ASAN=1 test
make clean && make DEBUG=1 UBSAN=1 test
```

**Integration tests** — each is a standalone binary, built separately:
```bash
make tests/integration/run_ingest         # server ingest test
make tests/integration/run_client         # client publish test
make tests/integration/run_ertmp_v1       # E-RTMP v1 server test
make tests/integration/run_ertmp_v2       # E-RTMP v2 server test
make tests/integration/run_tls            # RTMPS (TLS) end-to-end test
```

**TLS / RTMPS** is compiled in by default (OpenSSL). Build a zero-dependency,
plaintext-only library with `make TLS=0` (Makefile) or `meson -Dtls=disabled`.
The transport abstraction lives in `src/core/transport.{h,c}`; plaintext and TLS
share one send/recv path so the layers above never branch on the wire type.

**Meson (for CI / subproject embedding):**
```bash
meson setup builddir -Dtests=true -Dexamples=true
meson compile -C builddir
meson test -C builddir
```

The `CC` environment variable selects compiler (gcc or clang); CI tests both.

## Architecture

`librtmp2` is a pure protocol library — no media server logic, no HTTP, no auth policy. The host application registers callbacks and the library delivers semantic events.

**Layer stack (bottom → top):**

```
core/        alloc hook, growable buffers, big-endian byte helpers, logging, errors
handshake/   C0/C1/C2 ↔ S0/S1/S2; partial-read buffering; version detection
chunk/       chunk_reader, chunk_writer, chunk_state (per-csid); SetChunkSize/Abort
message/     reassembled message dispatch: control, user-control, AMF command decode/encode
amf/         AMF0 (mandatory) + AMF3 (optional)
flv/         FLV audio/video/script tag parsing
ertmp/       E-RTMP v1 (ExVideo/ExAudio, FourCC, HDR) + v2 (capsEx, reconnect, multitrack, ModEx)
session/     connection object (conn.c), state machine, stream bookkeeping, publish/play flows
server/      listening socket, accept loop, per-connection poll
client/      outbound connect → createStream → publish/play, frame send, receive poll
```

**Ingest data flow:** `lrtmp2_server_poll()` → `handshake/` → `chunk/` (reassemble per csid) → `message/` (classify) → `amf/` (decode commands) or `flv/` + `ertmp/` (decode frames) → `session/` (advance state machine, emit `_result`/`onStatus`) → host callbacks (`on_connect`, `on_publish`, `on_play`, `on_frame`, `on_close`).

**Connection state machine:**
```
TCP_ACCEPTED → HANDSHAKE → CONNECTED → [CAPS_NEGOTIATED] → APP_CONNECTED → STREAM_CREATED → PUBLISHING | PLAYING → CLOSING → CLOSED
```
`CAPS_NEGOTIATED` is the E-RTMP v2 capability exchange state between CONNECTED and APP_CONNECTED.

## Key Design Rules

**ABI boundary:** Only `include/librtmp2/*.h` is the public API. All internal headers (`src/**/*.h`) may change freely. Internal functions must be `static` or use hidden visibility — nothing in `src/` is a stable exported symbol.

**Threading:** The library is single-threaded per connection. `chunk_state` is per-connection (not global), so a client and server can coexist in the same process or on separate threads without shared mutable state. Each `lrtmp2_conn_t` must be driven from one thread at a time.

**Parser safety:** Never trust network-provided length fields. All parsers are bounds-checked and have corresponding fuzz harnesses in `tests/fuzz/`. Unknown E-RTMP v2 ModEx types must degrade gracefully to NOP, not abort.

## Test Structure

- `tests/unit/` — custom test runner (`main.c` calls `test_*_main()` per suite); no external framework dependency
- `tests/integration/` — standalone C programs that spin up real server/client pairs over loopback TCP using pthreads
- `tests/fuzz/` — libFuzzer harnesses (require `clang -fsanitize=fuzzer,address`)
- `tests/test_data/test.h264` — real H.264 IDR NALU used by integration tests to build synthetic RTMP streams

## Versioning

SemVer: `0.x` while API/ABI is evolving; `1.0.0` once stable. Before any release, run `abi-compliance-checker` against the previous release — see `docs/abi-policy.md` for the full checklist.
