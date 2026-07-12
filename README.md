# librtmp2-server

RTMP / E-RTMP media server built on [librtmp2](https://github.com/OpenRTMP/librtmp2).

Focused on RTMP/E-RTMP only. SQLite-backed. JSON stats. Nginx-compatible XML.

[![License](https://img.shields.io/github/license/OpenRTMP/librtmp2-server)](LICENSE)
![Version](https://img.shields.io/badge/version-v0.1.2-orange)
![Language](https://img.shields.io/badge/language-Rust-orange)

---

## Project Status

`librtmp2-server` is currently **Alpha** software.

Implemented in this repository:

- **Integrated RTMP listener** — listens on the configured `RTMP_BIND` address through the Rust `librtmp2` server implementation
- **RTMPS listener** — when `TLS_ENABLED=true`, a second listener on `RTMPS_BIND` accepts RTMPS *alongside* the plaintext RTMP listener
- **OBS / FFmpeg publishing path** — publish requests are routed through the DB-backed stream-key validation layer
- **Play / publish authentication** — separate `publish_key` and `play_key` validation
- **SQLite persistence** — streams, publishers, players, and stats are stored in a database
- **Live publisher/player tracking** — connection state is mirrored into the database
- **JSON stats** — `/stats?key=***` clean modern JSON
- **Nginx-RTMP XML** — `/stats-nginx?key=***` for existing tools
- **REST API** — Stream CRUD, Bearer token auth
- **Docker-ready** — Lightweight Alpine container

Still under active development:

- RTMPS/TLS production readiness
- Protocol completeness and compatibility hardening
- Performance optimization
- Production hardening
- Additional E-RTMP functionality

The server is not intended to be presented as a drop-in production replacement for `nginx-rtmp` yet. Test your OBS/FFmpeg workflow before using it for critical streams.

---

## Features

- **Integrated RTMP listener** — built on the Rust `librtmp2` implementation
- **RTMP + RTMPS at the same time** — RTMPS is an additional listener, not a mode switch; plaintext RTMP keeps working when TLS is enabled
- **SQLite persistence** — Streams, publishers, players, stats all in a DB
- **Unique keys per stream** — `publish_key`, `play_key`, `stats_key`
- **Privacy by design** — No one can see streams/stats without the exact key
- **JSON stats** — `/stats?key=***` clean modern JSON
- **Nginx-RTMP XML** — `/stats-nginx?key=***` for existing tools
- **REST API** — Stream CRUD, Bearer token auth
- **Docker-ready** — Lightweight Alpine container
- **Alpha quality** — Interfaces and implementation details may still change

---

## Architecture

`librtmp2-server` is written in Rust (axum + rusqlite). It owns the server
application layer around the RTMP protocol — configuration, persistence, the
HTTP/REST API, CLI, logging, key generation, stream management, authentication,
and stats.

The RTMP protocol layer is provided by the Rust
[`librtmp2`](https://github.com/OpenRTMP/librtmp2) crate and is integrated into
this server's listener. `src/server.rs` starts the RTMP listener(s) — one
plaintext, plus a second RTMPS listener when TLS is enabled — polls active
connections on both, and forwards connect/publish/play/close events into the
DB-backed [`RtmpEventHandler`](src/rtmp_bridge.rs) bridge while also updating
codec/stats data.

Both `librtmp2-server` and `librtmp2` are under active development and should be
considered Alpha software.

```text
OBS / FFmpeg / App
        │
        ▼
  librtmp2-server (Rust)
  ├── RTMP Listener (port 1935)      ← integrated via librtmp2 (Alpha), always on
  ├── RTMPS Listener (port 1936)     ← alongside RTMP, only when TLS_ENABLED=true
  ├── SQLite (streams, publishers, players, stats)
  ├── HTTP API     (port 8080, axum)
  │   ├── /api/v1/streams    CRUD
  │   ├── /stats             JSON stats (key-protected)
  │   └── /stats-nginx       XML stats (nginx-compatible)
  └── Config
        │
        ▼
      librtmp2 (Rust, in progress)
      ├── Handshake
      ├── Chunking
      ├── AMF
      ├── RTMP Commands
      ├── E-RTMP v1
      └── E-RTMP v2
```

---

## Build

### Dependencies

- Rust (stable toolchain) — see [rust-lang.org/tools/install](https://www.rust-lang.org/tools/install)
- SQLite is vendored via rusqlite's `bundled` feature — no system SQLite3 needed

### Compile

```bash
git clone https://github.com/OpenRTMP/librtmp2-server.git
cd librtmp2-server
cargo build --release
```

### Run

```bash
cp .env.example .env
LRTMP2_DB=./server.db ./target/release/librtmp2-server
```

Or with CLI port/log-level flags:

```bash
LRTMP2_DB=./server.db ./target/release/librtmp2-server -p 1935 -w 8080 -v
```

The API token is generated on first startup, stored in the SQLite database, and printed once to stderr. Use that printed token for Bearer-authenticated API calls and for `librtmp2-server-panel`.

---

## Configuration

`LRTMP2_DB` or `LRTMP2_DB_PATH` must point to the SQLite database file. Listener and logging settings live in `.env` (loaded by default, or pass `-c <path>`):

```env
# RTMP listener address (always active, regardless of TLS_ENABLED)
RTMP_BIND=0.0.0.0:1935

# Maximum concurrent RTMP/RTMPS connections across all listeners combined
RTMP_MAX_CONNECTIONS=100

# RTMPS (TLS) - disabled by default.
# When enabled, RTMPS_BIND runs *alongside* RTMP_BIND rather than replacing it.
TLS_ENABLED=false
TLS_CERT_FILE=/etc/librtmp2-server/fullchain.pem
TLS_KEY_FILE=/etc/librtmp2-server/privkey.pem
RTMPS_BIND=0.0.0.0:1936

# HTTP API and UI listener address
HTTP_BIND=0.0.0.0:8080

# Log level: 0=error 1=warn 2=info 3=debug
LOG_LEVEL=2

# Log file path (empty = stderr only)
LOG_FILE=
```

---

## RTMPS (TLS)

The RTMP listener is implemented through the integrated Rust `librtmp2` server.
Setting `TLS_ENABLED=true` starts a **second** listener on `RTMPS_BIND` that
speaks RTMPS — the plaintext RTMP listener on `RTMP_BIND` keeps running
unchanged, so existing publishers/players are unaffected. Both listeners are
bound on the same underlying `librtmp2` server instance, so they share one
connection pool, one media relay, and one `RTMP_MAX_CONNECTIONS` /
`RTMP_MAX_REASSEMBLY_MB` / `RTMP_MAX_CACHE_MB` / `RTMP_MAX_RELAY_QUEUE_MB`
budget rather than doubling it — and a publisher on one listener is relayed
to players on the other (publish over `rtmp://`, watch over `rtmps://`, or
vice versa, works out of the box).

Enabling TLS without both a cert and key file configured is refused with a
clear error at startup.

```env
TLS_ENABLED=true
TLS_CERT_FILE=/path/fullchain.pem
TLS_KEY_FILE=/path/privkey.pem
RTMPS_BIND=0.0.0.0:1936
```

`GET /api/v1/health` reports whether RTMPS is currently enabled and which
ports the RTMP/RTMPS listeners are bound to (`rtmp_port`, `rtmps_enabled`,
`rtmps_port`), so integrations like `librtmp2-server-panel` can show RTMPS
URLs only when they'll actually work.

RTMPS support should still be considered experimental while the protocol layer
and server integration are being hardened.

---

## Stream Keys & Privacy

Each stream has **three unique, auto-generated keys**:

| Key | Purpose | Used by |
|-----|---------|---------|
| `publish_key` | OBS/FFmpeg publishes with this | Publisher |
| `play_key` | Players connect with this | Player |
| `stats_key` | Access stats for this stream | Monitoring |

**No one can see your streams or stats without the exact key.** There is no public list of active streams.

---

## HTTP API

### Public

| Method | Endpoint | Auth |
|--------|----------|------|
| GET | `/api/v1/health` | None |

`/api/v1/health` returns a JSON object like:

```json
{
  "status": "ok",
  "timestamp": 1720000000,
  "rtmp_port": 1935,
  "rtmps_enabled": true,
  "rtmps_port": 1936
}
```

### API (Bearer token required)

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/streams` | List streams (keys hidden) |
| POST | `/api/v1/streams` | Create stream (returns keys) |
| DELETE | `/api/v1/streams/:id` | Delete stream |
| GET | `/api/v1/streams/:id/stats?key=<sk>` | Per-stream JSON stats |

### Stats (key-protected via query param)

| Endpoint | Format | Description |
|----------|--------|-------------|
| `/stats?key=<stats_key>` | JSON | Modern stats |
| `/stats-nginx?key=<stats_key>` | XML | Nginx-rtmp compatible |

---

## Example: Create a stream

```bash
curl -X POST http://localhost:8080/api/v1/streams \
  -H "Authorization: Bearer <generated-api-token>" \
  -H "Content-Type: application/json" \
  -d '{"id":"mystream","name":"My Live Stream","app":"live"}'
```

Response:
```json
{
  "id": "mystream",
  "name": "My Live Stream",
  "app": "live",
  "publish_key": "pub_a1b2c3d4e5f6789012345678abcdef01",
  "play_key": "pl_fedcba0987654321fedcba0987654321",
  "stats_key": "st_0123456789abcdef0123456789abcdef",
  "enabled": true
}
```

### Publish with OBS

- Server: `rtmp://your-server/live`
- Stream Key: use the `publish_key` returned by `POST /api/v1/streams`

### View stats (JSON)

```bash
curl "http://localhost:8080/stats?key=st_mystream_1719480002"
```

```json
{
  "streams": [{
    "name": "My Live Stream",
    "app": "live",
    "uptime": 12345,
    "bitrate_kbps": 2450.5,
    "bytes_in": 1234567,
    "video": {"codec": "h264", "width": 1920, "height": 1080, "fps": 30.0},
    "audio": {"codec": "aac"},
    "client": {"address": "1.2.3.4:56789", "publisher": true}
  }],
  "players": [],
  "summary": {"publishers": 1, "players": 0, "total_clients": 1}
}
```

### Stats nginx-rtmp XML

```bash
curl "http://localhost:8080/stats-nginx?key=st_mystream_1719480002"
```

Returns the same XML format as `nginx-rtmp-module`, including the
`bw_video`/`bw_audio` and stream-level `active`/`publishing` markers that
tools like [NOALBS](https://github.com/NOALBS/nginx-obs-automatic-low-bitrate-switching)
expect. `/stats-nginx` always redacts the real application and stream name
(to `live` and `stream`) since the key in the URL is a public, unauthenticated
query parameter — so a NOALBS `Nginx` stream server config must use those
fixed values, not your real app/stream name:

```json
{
  "type": "Nginx",
  "statsUrl": "http://<host>:8080/stats-nginx?key=<stats_key>",
  "application": "live",
  "key": "stream"
}
```

The `key=<stats_key>` in `statsUrl` is the per-stream `stats_key` from
`POST /api/v1/streams` — unrelated to NOALBS's own `key` field above, which
must always be the literal string `stream`.

Opening `/stats-nginx?key=<stats_key>` directly in a browser renders as an
HTML table (dark-themed) instead of raw XML, via an `<?xml-stylesheet?>`
processing instruction pointing at `/stat.xsl` — the same mechanism
`nginx-rtmp-module`'s classic `stat.xsl` uses, just restyled.

### Native NOALBS provider (`OpenRTMP`)

NOALBS also ships a dedicated `OpenRTMP` stream server provider
([nginx-obs-automatic-low-bitrate-switching#224](https://github.com/NOALBS/nginx-obs-automatic-low-bitrate-switching/pull/224))
that talks to `/stats` (the JSON endpoint) directly instead of the
`nginx-rtmp`-compatible XML shim above, so you don't need the fixed
`live`/`stream` placeholders:

```json
{
  "type": "OpenRTMP",
  "statsUrl": "http://<host>:8080/stats?key=<stats_key>",
  "triggers": {
    "low": 2500,
    "offline": 500,
    "rtt": 200,
    "rtt_offline": 2000
  }
}
```

`statsUrl` uses your real per-stream `stats_key` from
`POST /api/v1/streams` — same key as `/stats`, no app/stream redaction
involved. The provider reads `bitrate_kbps` and `rtt_ms` from the JSON
response to drive NOALBS's `low`/`offline` bitrate and RTT triggers.

---

## Docker

```bash
docker compose up -d
```

---

## Project Structure

```text
librtmp2-server/
├── src/
│   ├── main.rs           CLI entry point & arg parsing (clap)
│   ├── server.rs         App lifecycle, HTTP+RTMP(S) wiring
│   ├── config.rs         .env config loader
│   ├── db.rs             SQLite persistence (rusqlite)
│   ├── http.rs           HTTP API (axum)
│   ├── rtmp_bridge.rs    RTMP protocol ↔ DB integration seam
│   ├── keygen.rs         Stream key generation
│   └── logger.rs         Logging
├── Cargo.toml
├── Dockerfile
├── docker-compose.yml
└── .env.example
```

---

## License

MIT — see [LICENSE](LICENSE)
