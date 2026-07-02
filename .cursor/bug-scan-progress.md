# Bug scan progress

Last scanned: rtmp_bridge (2026-07-02)

## Modules

- [x] config — .env loader, env overrides
- [x] db — SQLite persistence, stream/publisher/player CRUD
- [x] http — REST API, auth, stats endpoints
- [x] server — App lifecycle, HTTP+RTMP wiring, deleted_streams eviction
- [x] rtmp_bridge — RTMP protocol ↔ DB integration seam
- [ ] keygen — Stream key generation
- [ ] logger — Logging

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
