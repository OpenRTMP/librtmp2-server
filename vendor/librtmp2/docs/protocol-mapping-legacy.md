# Protocol Mapping — Legacy RTMP

This document maps the Adobe *RTMP Specification 1.0* onto the `librtmp2`
source tree, so contributors can find the code that implements each part of the
spec. Section numbers refer to the Adobe RTMP 1.0 spec (see References in
[`concept/librtmp2-core.md`](../concept/librtmp2-core.md)).

## Handshake (spec §5.2)

| Spec element | Implementation |
|--------------|----------------|
| C0/S0 version byte | `src/handshake/handshake.c` |
| C1/S1 (time + zero + random) | `src/handshake/handshake.c` |
| C2/S2 echo | `src/handshake/handshake.c` |
| Partial-read buffering, timeout | `src/handshake/handshake.c`, driven by `src/session/conn.c` |

Only the standard (non-encrypted, non-Adobe-digest) handshake is implemented;
more complex Adobe variants are out of scope for now.

## Chunk Stream (spec §5.3)

| Spec element | Implementation |
|--------------|----------------|
| Basic header (fmt + csid, 1/2/3-byte forms) | `src/chunk/chunk_reader.c`, `src/chunk/chunk_writer.c` |
| Message header types 0–3 | `src/chunk/chunk_reader.c` |
| Extended timestamp | `src/chunk/chunk_reader.c`, `src/chunk/chunk_writer.c` |
| Per-`csid` state carry-forward | `src/chunk/chunk_state.c` (per-connection, not global) |
| Reassembly to full messages | `src/chunk/chunk_reader.c` → `src/message/message.c` |

## Protocol Control Messages (spec §5.4)

| Message | Type ID | Implementation |
|---------|---------|----------------|
| Set Chunk Size | 1 | `src/message/control.c` (applied immediately to chunk state) |
| Abort Message | 2 | `src/message/control.c` |
| Acknowledgement | 3 | `src/message/control.c` |
| Window Acknowledgement Size | 5 | `src/message/control.c` |
| Set Peer Bandwidth | 6 | `src/message/control.c` |

## User Control Messages (spec §6.2 / 7.1.7)

| Element | Implementation |
|---------|----------------|
| StreamBegin / StreamEOF / etc. | `src/message/user_control.h`, `src/message/control.c` |

## Command Messages / NetConnection & NetStream (spec §7.2)

| Command | Direction | Implementation |
|---------|-----------|----------------|
| `connect` | C→S | decode/encode `src/message/command.c`; dispatch `lrtmp2_conn_handle_command()` in `src/session/conn.c` |
| `createStream` | C→S | `src/message/command.c` + `src/session/conn.c` |
| `publish` | C→S | `src/message/command.c` + `src/session/publish.c` |
| `play` | C→S | `src/message/command.c` + `src/session/play.c` |
| `deleteStream` | C→S | accepted as no-op in `src/session/conn.c` |
| `FCPublish` / `FCUnpublish` / `releaseStream` | C→S | accepted as no-ops in `src/session/conn.c` |
| `_result` / `_error` | S→C | `lrtmp2_conn_send_connect_response`, `lrtmp2_conn_send_create_stream_response` (`src/session/conn.c`) |
| `onStatus` | S→C | `lrtmp2_conn_send_onstatus` (`src/session/conn.c`) |

## AMF0 (spec §A; AMF0 spec)

| Type | Implementation |
|------|----------------|
| Number, Boolean, String, Object, Null, ECMA Array, Strict Array, Long String | `src/amf/amf0.c` |
| Nesting-depth bound (DoS guard) | `src/amf/amf0.c` |

AMF3 (`src/amf/amf3.c`) is implemented for completeness but not required by the
legacy connect/publish/play flows.

## Audio / Video / Script Data Messages (spec §7.1)

| Message | Type ID | Implementation |
|---------|---------|----------------|
| Audio | 8 | `src/flv/audio_tag.c` → `lrtmp2_frame_t` |
| Video | 9 | `src/flv/video_tag.c` → `lrtmp2_frame_t` |
| Data / Script (`@setDataFrame`, `onMetaData`) | 18/15 | `src/flv/script_tag.c` (hardened against malformed input) |

## State Machine (spec flow)

```text
TCP_ACCEPTED → HANDSHAKE → CONNECTED → APP_CONNECTED
            → STREAM_CREATED → PUBLISHING | PLAYING → CLOSING → CLOSED
```

Implemented in `src/session/state_machine.c`, driven by `src/session/conn.c`.

## Tests

- Handshake golden bytes: `tests/unit/test_handshake.c`
- Chunk types 0–3, extended timestamp: `tests/unit/test_chunk.c`
- AMF0 primitives/objects: `tests/unit/test_amf.c`
- End-to-end ingest (handshake → connect → createStream → publish → frame):
  `tests/integration/test_server_ingest.c`
- End-to-end client publish over loopback: `tests/integration/test_client_publish.c`
