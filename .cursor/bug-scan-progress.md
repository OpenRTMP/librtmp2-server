# Bug scan progress

Last scanned: logger (2026-07-02)

## Modules

- [x] config — .env loader, env overrides
- [x] db — SQLite persistence, stream/publisher/player CRUD
- [x] http — REST API, auth, stats endpoints
- [x] server — App lifecycle, HTTP+RTMP wiring, deleted_streams eviction
- [x] rtmp_bridge — RTMP protocol ↔ DB integration seam
- [x] keygen — Stream key generation
- [x] logger — Logging

## Findings (2026-07-02 logger pass)

- **Medium (fixed):** `write_line()` only escaped `\r`/`\n` in log messages.
  `app` (RTMP `connect`/`publish`/`play` command target) is attacker-controlled
  and unauthenticated at the point it's logged (`authorize_publish`/
  `authorize_play` in rtmp_bridge.rs log it before the stream key is even
  validated), and was interpolated directly into `log_info!`/`log_warn!`
  format strings. Any other C0/C1 control byte — notably ANSI escape
  sequences (`\x1b[...`) — passed straight through into the log file/stderr,
  letting a remote peer inject terminal escape sequences (rewrite/hide prior
  log lines, move cursor, etc.) when an operator tails the log in a real
  terminal. Fixed by escaping every `char::is_control()` codepoint (not just
  `\r`/`\n`) as `\xHH` in `sanitize_for_log()`; added unit tests for
  newline/CR forging and ANSI escape injection.

## Findings (2026-07-02 keygen pass)

No critical bugs found. `keygen_with_entropy()` uses `rand::rngs::SysRng`
(OS/`getrandom`-backed CSPRNG, not a PRNG) with 128-bit entropy for
stream/play/stats/viewer keys and 256-bit for the API token; all four key
columns (`publish_key`, `play_key`, `stats_key` in `streams`, `play_key` in
`stream_viewers`) are `UNIQUE NOT NULL` in the schema, so a (practically
impossible) collision would surface as an insert error rather than silently
overwriting another row. All call sites (http.rs, rtmp_bridge.rs, db.rs,
server.rs) propagate `Err` on RNG failure instead of falling back to a
predictable key.

## Findings (2026-07-02 rtmp_bridge pass)

- **Critical (fixed):** `on_connect()` used `HashMap::insert`, wiping ConnState when
  `authorize_publish()` had already run during the same `poll()` tick (fast handshake +
  publish). Legitimate publishers were rejected as unauthorized; the DB kept an active
  publisher row with no in-memory owner (ghost slot blocking re-publish).
- **Critical (fixed):** `authorize_publish()` / `on_play()` overwrote per-connection
  session rows without deactivating the prior DB row when a client switched streams on the
  same TCP connection, leaving ghost active publishers/players.

## Findings (2026-07-01 server pass)

- **Critical (fixed):** librtmp2 relayed audio/video before librtmp2-server validated
  publish/play keys in its poll loop. A holder of a viewer `play_key` could publish
  under that stream name and inject frames to legitimate players until the
  connection was evicted on the next poll iteration. Patched vendored librtmp2
  with `Conn::relay_enabled` (default false); enabled only after
  `DbRtmpBridge::on_publish` / `on_play` succeeds.

## Findings (2026-06-30 http pass)

No critical bugs found.

## Findings (2026-06-29 db pass)

- `db_col_text()` — strncpy without forced NUL on max-length strings caused buffer overread
- `db_stream_delete()` cascade — ghost active publishers after stream delete + recreate
