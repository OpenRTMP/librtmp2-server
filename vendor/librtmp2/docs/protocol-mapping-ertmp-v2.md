# Protocol Mapping — Enhanced RTMP v2

This maps the Veovera *Enhanced RTMP v2* specification onto the `librtmp2`
source tree. Enhanced RTMP v2 adds capability negotiation, multitrack,
reconnect, and the ModEx extension mechanism on top of v1.

Source spec: [`veovera/enhanced-rtmp` — enhanced-rtmp-v2.md](https://github.com/veovera/enhanced-rtmp/blob/main/docs/enhanced/enhanced-rtmp-v2.md).

## Capability Negotiation (`capsEx`, `videoFourCcInfoMap`)

| Spec element | Implementation |
|--------------|----------------|
| `capsEx` parse/write (videoCodecId + audioCodecId as FourCC-encoded UI32) | `src/ertmp/connect_caps.c` |
| `videoFourCcInfoMap` ECMAArray parse/write | `src/ertmp/connect_caps.c` |

Internal structs: `lrtmp2_caps_exit_t`, `lrtmp2_video_fourcc_info_map_t`
(`src/ertmp/ertmp.h`).

State machine adds a `CAPS_NEGOTIATED` state between `CONNECTED` and
`STREAM_CREATED` (`src/session/state_machine.c`).

## Reconnect

| Spec element | Implementation |
|--------------|----------------|
| Reconnect payload: `replay` (UI32) + `limit` (UI32), big-endian, 8 bytes | `src/ertmp/reconnect.c` |

Internal struct: `lrtmp2_reconnect_t` (`src/ertmp/ertmp.h`).

## Multitrack

| Spec element | Implementation |
|--------------|----------------|
| Track descriptor: AMF0_NUMBER track id + AMF0_STRING name | `src/ertmp/multitrack.c` |
| Track types: audio (0), video (1), metadata (2) | `src/ertmp/multitrack.c` |

Internal struct: `lrtmp2_multitrack_t` (`src/ertmp/ertmp.h`).

## ModEx (Modular Extension)

| Spec element | Implementation |
|--------------|----------------|
| Marker byte `0x80 \| type` + payload | `src/ertmp/modex.c` |
| `NOP` (type 0) — 1 byte | `src/ertmp/modex.c` |
| `TIMESTAMP` (type 1) — 9 bytes (8-byte ns offset) | `src/ertmp/modex.c` |
| **Graceful degradation:** unknown types fall back to NOP | `src/ertmp/modex.c` |

Internal struct: `lrtmp2_modex_t` (`src/ertmp/ertmp.h`).

This is the key "graceful degradation" requirement from the concept: an unknown
ModEx type or capability field never aborts the connection — it is ignored
protocol-compliantly.

## Tests

- Unit: `tests/unit/test_ertmp.c` (capsEx, videoFourCcInfoMap, reconnect,
  multitrack, ModEx assertions, including unknown-ModEx → NOP)
- Integration: `tests/integration/test_server_ertmp_v2.c` — exercises all v2
  structures end-to-end
- Fuzz: `tests/fuzz/fuzz_modex.c`

## Known Limitations

- v2 structures parse and serialize correctly, but full v2 negotiation has not
  yet been verified against an external peer (OBS / SRS / HaishinKit). See
  [`roadmap.md`](roadmap.md).
