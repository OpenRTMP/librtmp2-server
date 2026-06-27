# CONTRIBUTING

This project follows the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification for commit messages. All commits must use English language for messages.

## Code Contribution Guidelines
- **API Compatibility**: No breaking librtmp2 API changes without coordinated updates to librtmp2 and librtmp2-server in the same session
- **librtmp2 Dependency**: librtmp2-server depends on librtmp2 as a sibling directory. Changes to librtmp2 headers may require updates in librtmp2-server
- **Testing**: All new features must be covered by unit/integration tests in `tests/unit` and `tests/integration`
- **Security**: Input validation on all HTTP and RTMP entry points; sanitize all user-provided data before database operations
- **Build**: Use CMake for production builds; Makefile for quick development builds

## Build Guidelines
```bash
# Quick build with Makefile (pulls librtmp2 from sibling)
make debug     # Debug build with symbols
make release   # Release build with optimizations
make test      # Build and run unit tests
make asan      # AddressSanitizer build
make ubsan     # UndefinedBehaviorSanitizer build

# Production build with CMake
cmake -B build -DENABLE_TESTS=ON
cmake --build build
ctest --test-dir build
```

## Dependencies
- **librtmp2**: Must be present as sibling directory (`../librtmp2`)
- **Mongoose**: Fetched automatically by CMake; Makefile downloads from GitHub
- **SQLite3**: System library (`pkg-config --libs sqlite3`)

## License
Source code is under [Apache 2.0 license](https://www.apache.org/licenses/LICENSE-2.0). Additional assets have their own license information.