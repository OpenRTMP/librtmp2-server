# Architecture

`librtmp2` is the lowest protocol layer of RTMP: it turns a raw byte stream into
semantic RTMP/FLV frames and protocol events, and back again. It deliberately
contains no media-server business logic — routing, auth policy, persistence, and
HTTP/stats all belong to the host application.

## Layered Overview

```text
            Host application (server / client / relay)
                          │  callbacks + config
┌─────────────────────────▼──────────────────────────────┐
│                       librtmp2                           │
│                                                         │
│   server/         client/                               │
│      │                │                                  │
│      └──────┬─────────┘                                  │
│             ▼                                            │
│          session/        state machine, publish/play    │
│             │                                            │
│             ▼                                            │
│          message/        reassembly, control, command   │
│             │                                            │
│      ┌──────┼───────┐                                    │
│      ▼      ▼       ▼                                    │
│    amf/   flv/    ertmp/   AMF0/3, FLV tags, E-RTMP v1/2 │
│      │      │       │                                    │
│      └──────┼───────┘                                    │
│             ▼                                            │
│          chunk/          chunk reader/writer + csid state│
│             │                                            │
│             ▼                                            │
│        handshake/        C0/C1/C2 ↔ S0/S1/S2             │
│             │                                            │
│             ▼                                            │
│          core/           buffers, bytes, alloc, log, err │
└─────────────────────────────────────────────────────────┘
                          │  bytes (TCP)
                          ▼
                     OS socket
```

## Module Responsibilities

| Module (`src/`) | Responsibility |
|-----------------|----------------|
| `core/`      | Allocation hook (`alloc.c`), growable buffers (`buffer.c`), big-endian byte helpers (`bytes.c`), logging (`log.c`), error formatting (`errors.c`). No protocol logic. |
| `handshake/` | Legacy C0/C1/C2 ↔ S0/S1/S2 handshake; version detection; partial-read buffering. |
| `chunk/`     | Chunk reader/writer for header types 0–3, extended timestamps, per-`csid` state carry-forward, `SetChunkSize`/`Abort`. |
| `message/`   | Reassembled-message dispatch: control messages, user-control messages, and AMF command encode/decode. |
| `amf/`       | AMF0 (mandatory) and AMF3 (optional) readers/writers for primitives, strings, objects, arrays. |
| `flv/`       | FLV audio/video/script tag parsing — the payload format carried inside RTMP audio/video messages. |
| `ertmp/`     | Enhanced-RTMP v1/v2: ExVideo/ExAudio headers, FourCC registry, HDR metadata, capability negotiation, reconnect, multitrack, ModEx. |
| `session/`   | The connection object (`conn.c`), per-connection state machine, stream bookkeeping, publish/play flows. |
| `server/`    | Listening socket, accept loop, per-connection driving via `lrtmp2_server_poll()`. |
| `client/`    | Outbound connect → createStream → publish/play, frame send, and receive polling. |

## Data Flow (ingest)

1. The host calls `lrtmp2_server_poll()`. The server reads available bytes per
   connection.
2. `handshake/` consumes bytes until the handshake completes, then hands the
   connection to the chunk layer.
3. `chunk/` reassembles complete messages per `csid`, applying chunk-size and
   abort control as it goes.
4. `message/` classifies each message. Command messages are decoded via `amf/`
   and dispatched by `lrtmp2_conn_handle_command()`; audio/video messages are
   parsed by `flv/` + `ertmp/` into an `lrtmp2_frame_t`.
5. `session/` advances the state machine (`connect` → `createStream` →
   `publish`/`play`) and emits responses (`_result`, `onStatus`).
6. The host's `on_connect` / `on_publish` / `on_play` / `on_frame` / `on_close`
   callbacks fire with semantic events — never raw AMF arrays.

The client path mirrors this in reverse for outbound connections.

## Design Principles

- **Strict core / thin host** — the library moves bytes and state; policy is the
  host's job (see `concept/librtmp2-core.md`).
- **Deterministic, bounds-checked parsers** — every parser is fuzzable and never
  trusts a length field from the network. See `tests/fuzz/`.
- **Graceful degradation** — unknown E-RTMP v2 ModEx types and capability fields
  are ignored protocol-compliantly rather than aborting the connection.
- **Small, stable ABI** — only `include/librtmp2/*.h` is public; everything else
  may change. See [`abi-policy.md`](abi-policy.md).

## Threading Model

The library does no threading of its own. `chunk_state` is kept per connection
rather than process-global, so a host may run a client and a server in the same
process, or one connection per thread, without shared mutable state between
connections. Each `lrtmp2_conn_t` must, however, be driven from a single thread
at a time.
