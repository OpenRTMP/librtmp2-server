# ABI Stability Policy

## Versioning Rules

librtmp2 follows [Semantic Versioning](https://semver.org/):

- **Patch** (`0.x.Z`): Bug fixes only. No API/ABI changes. Always safe to upgrade.
- **Minor** (`0.X.0`): New functions, types, or enum values. Existing symbols unchanged. Safe to re-link.
- **Major** (`X.0.0`): Breaking API/ABI changes. Not expected before `1.0.0`.

## ABI Guarantees

### Stable (guaranteed ABI-compatible across minor/patch releases)

- Public function signatures (parameter types, return types)
- Public struct layouts (`lrtmp2_frame_t`, `lrtmp2_hdr_info_t`, etc.)
- Enum values (`lrtmp2_video_codec_t`, `lrtmp2_audio_codec_t`, error codes)
- Type definitions and constants in `include/librtmp2/*.h`

### May Change

- Internal struct layouts (anything in `src/*.h`)
- Internal function signatures
- The `lrtmp2_server_t`, `lrtmp2_client_t`, `lrtmp2_conn_t` opaque types
- Configuration struct field order (new fields may be appended)

### Symbol Visibility

- Only functions declared in `include/librtmp2/*.h` are exported.
- All internal functions MUST be `static` or have hidden visibility.
- Shared library builds MUST use a version script or `-fvisibility=hidden`.

## Linking Recommendations

- **Stable programs**: Link against the shared library (`liblrtmp2.so`).
- **Embedders**: Link against the static library (`liblrtmp2.a`) for isolation.
- **FFI**: Use `dlopen`/`dlsym` for runtime binding against the soname `liblrtmp2.so.0`.

## ABI Check Checklist (before each release)

1. `abi-compliance-checker` against previous release.
2. No removal or reordering of public struct fields.
3. No changes to public function signatures.
4. New enum values are appended (not inserted).
5. `soname` bumped on ABI break (not expected before 1.0.0).
