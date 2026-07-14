# AGENTS.md

## Cursor Cloud specific instructions

`librtmp2-server` is a Rust RTMP/E-RTMP media server (axum HTTP API + bundled SQLite). Standard build/run/config commands are in `README.md` — use those.

Non-obvious notes for this environment:

- **Toolchain:** `Cargo.toml` sets `edition = "2024"`, `rust-version = "1.95"`. The VM snapshot ships stable Rust 1.97 as the default `rustup` toolchain; the older system Rust (1.83) will not compile this crate.
- **Dependency source:** the server pins `librtmp2 = "0.4.0"` (currently via git rev until the crate is on crates.io). It does **not** use the sibling `../librtmp2` checkout unless you add a `[patch]` override locally.
- **SQLite is bundled** via `rusqlite` `bundled` feature — no system SQLite needed.
- **Running (dev):** `LRTMP2_DB=./server.db ./target/debug/librtmp2-server -v`. RTMP listens on `:1935`, HTTP API on `:8080`. Runtime files (`server.db*`) are untracked and must not be committed.
- **API token:** generated once on first start and stored in SQLite (printed to stderr). To use a known token instead, export a real `LRTMP2_API_TOKEN` **process env var before first startup** — the `.env` loader deliberately ignores `LRTMP2_API_TOKEN` in the file. The token is only re-read from env while seeding a fresh DB, so delete `server.db*` to re-seed.
- **Tests:** `cargo test` covers unit tests; the end-to-end suite `tests/rtmp_http_e2e.rs` requires `cargo test --features test-support`.
- **End-to-end publish check:** `ffmpeg -re -f lavfi -i testsrc -f lavfi -i sine -c:v libx264 -c:a aac -f flv rtmp://localhost:1935/live/<publish_key>`, then `curl "http://localhost:8080/stats?key=<stats_key>"`.
