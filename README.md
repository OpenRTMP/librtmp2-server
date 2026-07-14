# librtmp2-server

RTMP / E-RTMP media server built on [librtmp2](https://github.com/OpenRTMP/librtmp2).

Focused on RTMP/E-RTMP only. SQLite-backed. JSON stats. Nginx-compatible XML.

[![License](https://img.shields.io/github/license/OpenRTMP/librtmp2-server)](LICENSE)
![GitHub Release](https://img.shields.io/github/v/release/OpenRTMP/librtmp2-server)
![Language](https://img.shields.io/badge/language-Rust-orange)

---

## Project Status

`librtmp2-server` is **alpha** software: the HTTP/API/DB layer is usable, but RTMP behaviour comes entirely from the embedded [`librtmp2`](https://github.com/OpenRTMP/librtmp2) crate — see that repo's [implementation status](https://github.com/OpenRTMP/librtmp2#implementation-status) for protocol details.

### Implemented in this repository

- **RTMP listener** on `RTMP_BIND` via `librtmp2`
- **RTMPS listener** on `RTMPS_BIND` when `TLS_ENABLED=true` (alongside plaintext RTMP)
- **OBS / FFmpeg publish path** with DB-backed `publish_key` validation
- **Play authentication** via `play_key`
- **Publisher → player relay** (same `(app, stream)` route)
- **SQLite persistence** — streams, publishers, players, stats
- **JSON stats** — `/stats?key=***`
- **Nginx-RTMP XML** — `/stats-nginx?key=***`
- **REST API** — stream CRUD, Bearer token auth
- **Docker** — Alpine-based images on GHCR

### Protocol behaviour inherited from librtmp2 (not reimplemented here)

- Live H.264/AAC and Enhanced-RTMP **passthrough** ingest (HEVC/AV1/Opus, multitrack when publishers send it)
- Late player join gets cached codec sequence headers (legacy + Enhanced-RTMP) and last keyframe; `onMetaData` is replayed to late joiners
- Legacy RTMP commands (`pause`, `seek`, `receiveAudio`/`receiveVideo`, `closeStream`) are handled in the protocol layer
- E-RTMP v2 connect capability negotiation and multitrack relay live in `librtmp2`; this server does not expose per-track IDs in the HTTP API yet
- No nginx-rtmp feature parity (HLS, exec, push relay, recording)

Test your OBS/FFmpeg workflow before using this for critical streams. It is not a drop-in replacement for `nginx-rtmp`.

---

## Features

Everything below is implemented **in this repo**. Wire-protocol limits are defined by `librtmp2` (see link above).

- **Integrated RTMP listener** — built on the `librtmp2` crate
- **RTMP + RTMPS at the same time** — RTMPS is an additional listener, not a mode switch
- **SQLite persistence** — streams, publishers, players, stats
- **Unique keys per stream** — `publish_key`, `play_key`, `stats_key`
- **Privacy by design** — no public stream list without keys
- **JSON + Nginx-compatible XML stats**
- **REST API** — stream CRUD, Bearer token auth
- **Docker-ready** — lightweight Alpine container

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

Both `librtmp2-server` and `librtmp2` are alpha; API and protocol details may still change.

```text
OBS / FFmpeg / App
        │
        ▼
  librtmp2-server (Rust)
  ├── RTMP Listener (port 1935)      ← librtmp2, always on
  ├── RTMPS Listener (port 1936)     ← librtmp2, when TLS_ENABLED=true
  ├── SQLite (streams, publishers, players, stats)
  ├── HTTP API     (port 8080, axum)
  │   ├── /api/v1/streams    CRUD
  │   ├── /stats             JSON stats (key-protected)
  │   └── /stats-nginx       XML stats (nginx-compatible)
  └── Config
        │
        ▼
      librtmp2 (Rust)
      ├── Live publish/play relay
      ├── Legacy RTMP session core
      └── E-RTMP passthrough + parser modules (see librtmp2 README)
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
  "publish_key": "live_a1b2c3d4e5f6789012345678abcdef01",
  "play_key": "play_fedcba0987654321fedcba0987654321",
  "stats_key": "sts_0123456789abcdef0123456789abcdef",
  "enabled": true
}
```

### Publish with OBS

- Server: `rtmp://your-server/live`
- Stream Key: use the `publish_key` returned by `POST /api/v1/streams`

### View stats (JSON)

```bash
curl "http://localhost:8080/stats?key=sts_0123456789abcdef0123456789abcdef"
```

```json
{
  "streams": [{
    "id": "mystream",
    "name": "My Live Stream",
    "app": "live",
    "uptime": 12345,
    "bitrate_kbps": 2450.5,
    "rtt_ms": 24.0,
    "bytes_in": 1234567,
    "video": {"codec": "h264", "width": 1920, "height": 1080, "fps": 30.0},
    "audio": {"codec": "aac"}
  }],
  "players": [],
  "summary": {"publishers": 1, "players": 0, "total_clients": 1}
}
```

Each entry in `players` (one per connected viewer, once any are watching) looks like:

```json
{
  "id": "play_...",
  "stream_name": "My Live Stream",
  "app": "live",
  "uptime": 12345,
  "bitrate_kbps": 2450.5,
  "rtt_ms": 24.0,
  "bytes_out": 987654
}
```

### Stats nginx-rtmp XML

```bash
curl "http://localhost:8080/stats-nginx?key=sts_0123456789abcdef0123456789abcdef"
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

NOALBS is adding a dedicated `OpenRTMP` stream server provider (a draft
pull request against `NOALBS/nginx-obs-automatic-low-bitrate-switching`
at the time of writing) that talks to `/stats` (the JSON endpoint) directly
instead of the `nginx-rtmp`-compatible XML shim above, so you don't need
the fixed `live`/`stream` placeholders. End-to-end setup once that PR
lands:

**1. Create a stream** (see [Example: Create a stream](#example-create-a-stream) above)
to get its keys. If you'd rather not hand-craft `curl` calls,
[librtmp2-server-panel](https://github.com/OpenRTMP/librtmp2-server-panel)
is a web UI for this server — it can create/delete streams, one-click-copy
the publish/play/stats URLs, and show the same live stats — so you can grab
the `stats_key` for NOALBS straight from its overview instead:

```bash
curl -X POST http://localhost:8080/api/v1/streams \
  -H "Authorization: Bearer <generated-api-token>" \
  -H "Content-Type: application/json" \
  -d '{"id":"mystream","name":"My Live Stream","app":"live"}'
```

```json
{
  "id": "mystream",
  "app": "live",
  "publish_key": "live_a1b2c3d4e5f6789012345678abcdef01",
  "play_key": "play_fedcba0987654321fedcba0987654321",
  "stats_key": "sts_0123456789abcdef0123456789abcdef"
}
```

**2. Publish with OBS** using the `publish_key` as the stream key (server
`rtmp://your-server/live`) — see [Publish with OBS](#publish-with-obs) above.
`play_key` is what viewers/players use to pull the stream; it's unrelated to
NOALBS.

**3. Point NOALBS at `/stats?key=<stats_key>`** using the `stats_key` from
step 1 — this is the only key NOALBS needs. The provider entry itself only
needs `statsUrl`; the bitrate/RTT thresholds live in NOALBS's top-level
`switcher.triggers`, not inside the provider block:

```json
{
  "switcher": {
    "triggers": {
      "low": 2500,
      "offline": 500,
      "rtt": 200,
      "rttOffline": 2000
    },
    "streamServers": [
      {
        "name": "openrtmp",
        "priority": 0,
        "enabled": true,
        "streamServer": {
          "type": "OpenRTMP",
          "statsUrl": "http://<host>:8080/stats?key=sts_0123456789abcdef0123456789abcdef"
        }
      }
    ]
  }
}
```

Unlike the `Nginx` provider above, there's no separate `application`/`key`
pair to fill in — the `stats_key` embedded in `statsUrl` is all the
addressing NOALBS needs, since `/stats` returns the real app/stream name
instead of the redacted `live`/`stream` placeholders. The provider reads
`bitrate_kbps` and `rtt_ms` from the JSON response (see
[View stats (JSON)](#view-stats-json) above) to drive those triggers:
`low`/`offline` are `bitrate_kbps` floors (in kbps) — NOALBS switches once
the stream drops to or below them — and `rtt`/`rttOffline` are `rtt_ms`
ceilings (in milliseconds) that trigger once the RTT reaches or exceeds
them.

---

## Docker

Prebuilt multi-arch images (`amd64`/`arm64`/`riscv64`) are published to
`ghcr.io/openrtmp/librtmp2-server` on every release — no local build needed:

```bash
docker run -d \
  --name librtmp2-server \
  -p 1935:1935 \
  -p 8080:8080 \
  -v librtmp2-server-data:/data \
  ghcr.io/openrtmp/librtmp2-server:latest
docker logs librtmp2-server   # copy the generated API token
```

Available tags: `latest`, `beta`, `alpha`, and pinned versions (e.g. `0.1.4` /
`v0.1.4`).

To build from source instead:

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
