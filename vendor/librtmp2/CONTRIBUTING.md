# CONTRIBUTING

This project follows the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification for commit messages. All commits must use English language for messages, even though the repository requires UTF-8 encoding.

## Code Contribution Guidelines
- **API Compatibility**: No breaking API changes unless accompanied by ABIs changes in `include/librtmp2/*`. For non-breaking changes that modify internals, only update shared headers if necessary.
- **Testing**: All new features must be covered by unit/integration tests in `tests/unit` and `tests/integration`
- **ABI Stability**: Only change public headers (located in `include/librtmp2/`) with major version bumps. Internal headers (in `include/librtmp2-server/`) can change freely.
- **Documentation**: Update `CLAUDE.md`, `docs/abi-policy.md`, and `docs/protocol-mapping` files when modifying API behavior
- **Fuzzing**: Add new fuzz targets for data-payload areas (especially chunk/AMF/frame parsers)

## Build Guidelines
1. Use `make asan` or `make ubsan` for CI-style builds
2. Fuzz testing requires `clang -fsanitize=fuzzer,address`

## Subproject Additions
When adding new subprojects (e.g., plugins), use Meson's `declarative_parameters` format and add them to `meson.build`.

## License
Source code is under [Apache 2.0 license](https://www.apache.org/licenses/LICENSE-2.0). Additional assets have their own license information.