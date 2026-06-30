# librtmp2-server

RTMP / E-RTMP media server built on [librtmp2](https://github.com/OpenRTMP/librtmp2).

Focused on RTMP/E-RTMP only. SQLite-backed. JSON stats. Nginx-compatible XML.

[![License](https://img.shields.io/github/license/OpenRTMP/librtmp2-server)](LICENSE)
![Version](https://img.shields.io/badge/version-alpha-red)
![Language](https://img.shields.io/badge/language-Rust-orange)

---

## Features

- **SQLite persistence** — Streams, publishers, players, stats all in a DB
- **Unique keys per stream** — `publish_key`, `play_key`, `stats_key`
- **Privacy by design** — No one can see streams/stats without the exact key
- **JSON stats** — `/stats?key=***` clean modern JSON
- **Nginx-RTMP XML** — `/stats-nginx?key=***` for existing tools
- **REST API** — Stream CRUD, Bearer token auth
- **Docker-ready** — Lightweight Alpine container

---

## Architecture

`librtmp2-server` is written in Rust (axum + rusqlite). It owns everything
*around* the RTMP protocol — config, persistence, the HTTP/REST API, CLI,
logging, and key generation — and exposes an [`RtmpEventHandler`
trait](src/rtmp_bridge.rs) (`on_connect` / `on_publish` / `on_play` /
`on_frame` / `on_close`) as the integration seam for the actual RTMP/E-RTMP
protocol implementation.

The RTMP protocol layer itself lives in a separate
[librtmp2](https://github.com/OpenRTMP/librtmp2) crate (also being rewritten
in Rust) and is not yet wired into this server's listener — see
[`src/server.rs`](src/server.rs) for where that integration will land.

```text
OBS / FFmpeg / App
        │
        ▼
  librtmp2-server (Rust)
  ├── RTMP Listener (port 1935)      ← not yet wired up; pending librtmp2 (Rust)
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
./target/release/librtmp2-server -c config.example.env
```

Or with CLI flags:

```bash
./target/release/librtmp2-server -p 1935 -w 8080 -t my-secret-token -v
```

---

## Configuration

```env
# RTMP listener address
RTMP_BIND=0.0.0.0:1935

# Maximum concurrent RTMP connections
RTMP_MAX_CONNECTIONS=100

# RTMPS (TLS) - disabled by default
TLS_ENABLED=false
TLS_CERT_FILE=/etc/librtmp2-server/fullchain.pem
TLS_KEY_FILE=/etc/librtmp2-server/privkey.pem

# HTTP API and UI listener address
HTTP_BIND=0.0.0.0:8080

# Log level: 0=error 1=warn 2=info 3=debug
LOG_LEVEL=2

# Log file path (empty = stderr only)
LOG_FILE=
```

---

## RTMPS (TLS)

RTMPS termination will be provided by the RTMP listener once the Rust
`librtmp2` crate is wired in. The `tls` config block and
`LRTMP2_TLS_ENABLED` / `LRTMP2_TLS_CERT_FILE` / `LRTMP2_TLS_KEY_FILE`
environment variables are already validated by this server at startup —
enabling TLS without both a cert and key file configured is refused with a
clear error — but no listener currently terminates connections.

```env
TLS_ENABLED=true
TLS_CERT_FILE=/path/fullchain.pem
TLS_KEY_FILE=/path/privkey.pem
```

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
  -H "Authorization: Bearer $(openssl rand -hex 32)" \
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
- Stream Key: `pub_mystream_1719480000`

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

Returns the same XML format as `nginx-rtmp-module`.

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
│   ├── server.rs         App lifecycle, HTTP+RTMP wiring
│   ├── config.rs         .env config loader
│   ├── db.rs             SQLite persistence (rusqlite)
│   ├── http.rs           HTTP API (axum)
│   ├── rtmp_bridge.rs    RTMP protocol ↔ DB integration seam
│   ├── keygen.rs         Stream key generation
│   └── logger.rs         Logging
├── Cargo.toml
├── Dockerfile
├── docker-compose.yml
└── config.example.env
```

---

## License

MIT — see [LICENSE](LICENSE)
