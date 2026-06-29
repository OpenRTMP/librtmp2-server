# CONTRIBUTING

This project follows the [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/) specification for commit messages. All commits must use English language for messages.

## Code Contribution Guidelines
- **Integration seam stability**: `librtmp2-server` does not implement the RTMP/E-RTMP protocol itself — that lives in the separate `librtmp2` crate. Changes to the [`RtmpEventHandler`](src/rtmp_bridge.rs) trait are a breaking change for that integration and should be coordinated
- **Testing**: All new features must be covered by unit tests alongside the module they touch (`#[cfg(test)] mod tests` in the relevant `src/*.rs` file)
- **Security**: Input validation on all HTTP entry points; sanitize all user-provided data before database operations
- **Lints**: `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` must pass clean — this is enforced in CI

## Build Guidelines
```bash
cargo build           # Debug build
cargo build --release # Release build with optimizations
cargo test             # Run unit tests
cargo fmt              # Format code
cargo clippy --all-targets -- -D warnings  # Lint (same as CI)
```

## Dependencies
- **Rust** stable toolchain
- **SQLite**: vendored via rusqlite's `bundled` feature — no system SQLite3 needed

## License
Source code is under [Apache 2.0 license](https://www.apache.org/licenses/LICENSE-2.0). Additional assets have their own license information.
