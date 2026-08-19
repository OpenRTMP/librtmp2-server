# Bug scan progress

Last scanned: keygen (2026-08-19)

## Modules

- [x] config — .env loader, env overrides
- [x] db — SQLite persistence, stream/publisher/player CRUD
- [x] http — REST API, auth, stats endpoints
- [x] server — App lifecycle, HTTP+RTMP wiring, deleted_streams eviction
- [x] rtmp_bridge — RTMP protocol ↔ DB integration seam
- [x] keygen — Stream key generation
- [ ] logger — Logging

## Findings (2026-08-10 rtmp_bridge pass)

- **Critical (fixed):** `live_conn_count_for_stream` and delete/eviction helpers
  keyed only on `ConnState.stream_id`, which follows the publisher when both
  roles are active. A connection publishing stream A while playing stream B
  (supported by `authorize_play` stream-switch tests) was invisible to stream B
  drain/kick: `wait_and_finalize_stream_delete` saw `local=0`, finalized early,
  and `deleted_streams` markers for B could drop while the player was still live.
  Counting and eviction now use publisher/player row `stream_id` via
  `stream_ids_for_conn`.

## Findings (2026-08-08 server pass)

- **Critical (fixed):** `deleted_streams` retention keyed only on
  `TrackedConn.stream_id`, while `revoked_viewers` already used the bridge.
  When the bridge held a live publisher/player but the poll tracker had not
  yet copied `stream_id` (same-tick authorize before `current_stream` is
  observed), the retain pass dropped the deletion marker while sessions were
  still live. Subsequent poll ticks no longer kicked those connections, so a
  publisher could keep broadcasting on a disabled/pending-delete stream until
  manual disconnect. Retention and delete kicks now prefer the bridge stream
  id (with tracker fallback).
- **High (fixed):** If the RTMP poll loop exited on an internal error while
  HTTP kept serving, the process stayed half-alive — API/health looked fine but
  all publish/play was dead until restart. Unexpected RTMP thread exit now
  triggers HTTP graceful shutdown.

## Findings (2026-08-07 http pass)

- **Critical (fixed):** `wait_and_finalize_stream_delete()` removed the stream from
  `deleted_streams` after a 30s timeout and returned without finalizing. The RTMP
  poll loop only kicks connections whose stream id is still in that set, so a
  publisher that outlived the timeout kept broadcasting indefinitely on a
  disabled/pending-delete stream. The drain loop now logs periodically but never
  clears `deleted_streams` early; duplicate DELETE while draining returns 202.

## Findings (2026-08-06 db pass)

- **Critical (fixed):** `publisher_update()` / `player_update()` could set
  `active=1` without enforcing the same single-publisher-per-stream and
  per-viewer connection-cap invariants as `publisher_try_acquire()` /
  `player_try_acquire()`. During RTMP stream-switch rollback
  (`restore_publisher_row` / `restore_player_row` in rtmp_bridge.rs), a
  brief deactivation window let another client take the slot; the rollback
  path then reactivated the old row via `publisher_update`, leaving two
  active publisher rows for one stream — blocking legitimate publishes until
  restart. Guarded `publisher_update` / `player_update` when `active=true`.

## Findings (2026-08-05 config pass)

- **Critical (fixed):** CLI `-p`/`-w` port overrides rewrote bind addresses as
  `0.0.0.0:{port}`, discarding a configured localhost-only host
  (`RTMP_BIND=127.0.0.1:1935` or `HTTP_BIND=127.0.0.1:8080`). An operator
  changing only the port via `-p`/`-w` would unintentionally expose RTMP/HTTP on
  all interfaces. Fixed with `set_bind_port()` that preserves the configured host
  (including bracketed IPv6) while replacing the port.

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

## Findings (2026-08-19 keygen pass)

No critical bugs found. Re-verified `keygen_with_entropy()` (SysRng / OS CSPRNG,
128-bit stream keys, 256-bit API token), `is_valid_access_key()` enforcement at
all RTMP/DB lookup boundaries, global uniqueness via `key_globally_in_use_locked`
+ UNIQUE constraints, RNG-failure propagation (no predictable fallback), and
cluster leader-only viewer-id generation for Raft apply parity.

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