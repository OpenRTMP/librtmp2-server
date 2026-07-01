# Protocol Mapping — Enhanced RTMP v1

This maps the Veovera *Enhanced RTMP v1* specification onto the `librtmp2`
source tree. Enhanced RTMP v1 extends legacy RTMP/FLV mainly with modern codecs,
FourCC signaling, and richer metadata.

Source spec: [`veovera/enhanced-rtmp` — enhanced-rtmp-v1.md](https://github.com/veovera/enhanced-rtmp/blob/main/docs/enhanced/enhanced-rtmp-v1.md).

## ExVideoTagHeader

| Spec element | Implementation |
|--------------|----------------|
| `IsExHeader` bit detection in VideoTagHeader | `src/ertmp/exvideo.c` |
| `PacketType` (SequenceStart, CodedFrames, SequenceEnd, CodedFramesX, Metadata, MPEG2TSSequenceStart) | `src/ertmp/exvideo.c` |
| FourCC codec id (replaces legacy `CodecID`) | `src/ertmp/exvideo.c` + `src/ertmp/fourcc.c` |
| `frame_type` | `src/ertmp/exvideo.c` |
| Composition time (CodedFrames) | `src/ertmp/exvideo.c` |

Internal struct: `lrtmp2_video_header_t` (`is_ex_header`, `packet_type`,
`fourcc[5]`, `frame_type`, `composition_time`), declared in `src/ertmp/ertmp.h`.

## FourCC Codecs

| FourCC | Maps to | Implementation |
|--------|---------|----------------|
| `avc1` | H.264 | `src/ertmp/fourcc.c` |
| `hvc1` | H.265 / HEVC | `src/ertmp/fourcc.c` |
| `av01` | AV1 | `src/ertmp/fourcc.c` |
| `vp09` | VP9 | `src/ertmp/fourcc.c` |
| `Opus`, `mp4a` (audio) | Opus / AAC | `src/ertmp/fourcc.c` |

The registry provides forward (FourCC → codec), reverse (codec → FourCC), and
human-readable name lookups.

## ExAudioTagHeader

| Spec element | Implementation |
|--------------|----------------|
| `IsExHeader` detection (bit7 set + len ≥ 5 + recognized FourCC distinguishes enhanced from legacy SoundFormat 8-15, incl. AAC) | `src/ertmp/exaudio.c` |
| Audio FourCC (`Opus`, `mp4a`, `mp3 `, `ec-3`) | `src/ertmp/exaudio.c` + `src/ertmp/fourcc.c` |
| Wiring into the audio frame path | `src/message/message.c` (uses `lrtmp2_ertmp_exaudio_parse()` for legacy + enhanced) |

The enhanced audio FourCC is surfaced on `lrtmp2_frame_t.audio_fourcc`
(`include/librtmp2/types.h`).

## Metadata / HDR (`PacketTypeMetadata`, `colorInfo`)

| Spec element | Implementation |
|--------------|----------------|
| `colorInfo` parse/write | `src/ertmp/metadata.c` |
| Color primaries (e.g. BT.2020) | `src/ertmp/metadata.c` |
| Transfer characteristics (PQ / HLG) | `src/ertmp/metadata.c` |
| Matrix coefficients | `src/ertmp/metadata.c` |
| `videocodecid_from_fourcc()` utility | `src/ertmp/metadata.c` |

Internal struct: `lrtmp2_hdr_info_t` (`src/ertmp/ertmp.h`).

## connect-object `fourCcList`

| Spec element | Implementation |
|--------------|----------------|
| `fourCcList` ECMAArray parse/write (E-RTMP v1 §6) | `src/ertmp/connect_caps.c` |

Internal struct: `lrtmp2_fourcc_list_t` (`src/ertmp/ertmp.h`).

## Tests

- Unit: `tests/unit/test_ertmp.c` (ExVideo/ExAudio/FourCC/HDR assertions)
- Integration: `tests/integration/test_server_ertmp_v1.c` — end-to-end HEVC
  (`hvc1`→H265), AV1 (`av01`→AV1), Opus, and legacy H.264
- `tests/integration/test_server_ingest.c` also carries HEVC + Opus frames
- Fuzz: `tests/fuzz/fuzz_ertmp_video.c`, `tests/fuzz/fuzz_ertmp_audio.c`
