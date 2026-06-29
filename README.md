# librtmp2-server

RTMP / E-RTMP media server built on [librtmp2](https://github.com/OpenRTMP/librtmp2).

Focused on RTMP/E-RTMP only. SQLite-backed. JSON stats. Nginx-compatible XML.

[![License](https://img.shields.io/github/license/OpenRTMP/librtmp2-server)](LICENSE)
[![Version](https://img.shields.io/badge/version-0.1.0-blue)]()
[![Language](https://img.shields.io/badge/language-C-blue)]()

---

## Features

- **RTMP / E-RTMP ingest** — Legacy RTMP + Enhanced RTMP v1/v2 (HEVC, AV1, VP9)
- **SQLite persistence** — Streams, publishers, players, stats all in a DB
- **Unique keys per stream** — `publish_key`, `play_key`, `stats_key`
- **Privacy by design** — No one can see streams/stats without the exact key
- **JSON stats** — `/stats?key=***` clean modern JSON
- **Nginx-RTMP XML** — `/stats-nginx?key=***` for existing tools
- **REST API** — Stream CRUD, Bearer token auth
- **Docker-ready** — Lightweight Alpine container

---

## Architecture

```text
OBS / FFmpeg / App
        │
        ▼
  librtmp2-server
  ├── RTMP Listener (port 1935)
  ├── SQLite (streams, publishers, players, stats)
  ├── HTTP API     (port 8080)
  │   ├── /api/v1/streams    CRUD
  │   ├── /stats             JSON stats (key-protected)
  │   └── /stats-nginx       XML stats (nginx-compatible)
  └── Config
        │
        ▼
      librtmp2
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

- C11 compiler (gcc / clang)
- CMake >= 3.16
- pthread + SQLite3 dev
- OpenSSL dev (for RTMPS; optional — see below)
- [librtmp2](https://github.com/OpenRTMP/librtmp2)

### Compile

```bash
# Build librtmp2 first
git clone https://github.com/OpenRTMP/librtmp2.git
cd librtmp2 && make release && cd ..

# Build the server
git clone https://github.com/OpenRTMP/librtmp2-server.git
cd librtmp2-server
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release -DLRTMP2_DIR=../../librtmp2
make -j$(nproc)
```

### Run

```bash
./librtmp2-server -c config.example.json
```

Or with CLI flags:

```bash
./librtmp2-server -p 1935 -w 8080 -t my-secret-token -v
```

---

## Configuration

```json
{
  "rtmp": {
    "bind": "0.0.0.0:1935",
    "max_connections": 100,
    "chunk_size": 4096
  },
  "tls": {
    "enabled": false,
    "cert_file": "/etc/librtmp2-server/fullchain.pem",
    "key_file": "/etc/librtmp2-server/privkey.pem"
  },
  "http": {
    "bind": "0.0.0.0:8080"
  },
  "auth": {
    "api_token": "<replace-with-random-token>"
  },
  "log_level": 2,
  "log_file": ""
}
```

---

## RTMPS (TLS)

RTMPS termination is provided by librtmp2 (OpenSSL) and is **built in by
default**. To enable it, set `tls.enabled` and point at a PEM certificate chain
and private key:

```json
"tls": { "enabled": true, "cert_file": "/path/fullchain.pem", "key_file": "/path/privkey.pem" }
```

Equivalent environment variables: `LRTMP2_TLS_ENABLED=1`,
`LRTMP2_TLS_CERT_FILE`, `LRTMP2_TLS_KEY_FILE`. When `tls.enabled` is `false`
(the default) the listener speaks plaintext RTMP.

To build **without** TLS (no OpenSSL dependency), disable it in both projects:

```bash
make TLS=0                      # Makefile build
cmake .. -DENABLE_TLS=OFF       # CMake build
```

Enabling TLS in the config while the library was built without it is refused at
startup with a clear error.

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

### Stats nginx-rtmp XML (für externe Tools)

```bash
curl "http://localhost:8080/stats-nginx?key=st_mystream_1719480002"
```

Gibt das gleiche XML-Format wie `nginx-rtmp-module` zurück.

---

## Docker

```bash
docker compose up -d
```

---

## Project Structure

```text
librtmp2-server/
├── include/librtmp2-server/   Public headers
├── src/                       Source files
│   ├── cli.c                  Entry point & arg parsing
│   ├── server.c               Main app context
│   ├── config.c               JSON config loader
│   ├── db.c                   SQLite persistence
│   ├── http.c                 HTTP server (Mongoose)
│   ├── rtmp_callbacks.c       librtmp2 → DB bridge
│   └── logger.c               Logging
├── tests/                     Unit tests
├── CMakeLists.txt
├── Dockerfile
├── docker-compose.yml
└── config.example.json
```

---

## License

MIT — see [LICENSE](LICENSE)
