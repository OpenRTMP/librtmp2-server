# librtmp2-server

RTMP / E-RTMP media server built on [librtmp2](https://github.com/AlexanderWagnerDev/librtmp2).

Focused on RTMP/E-RTMP only. SQLite-backed. Nginx-RTMP-compatible stats.

[![License](https://img.shields.io/github/license/AlexanderWagnerDev/librtmp2-server)](LICENSE)
[![Status](https://img.shields.io/badge/status-alpha-orange)]()
[![Language](https://img.shields.io/badge/language-C-blue)]()

---

## Features

- **RTMP / E-RTMP ingest** — Legacy RTMP + Enhanced RTMP v1/v2 (HEVC, AV1, VP9)
- **SQLite persistence** — Streams, publishers, players, stats all in a DB
- **Unique keys per stream** — Each stream gets a unique `publish_key`, `play_key`, and `stats_key`
- **Privacy by design** — No one can see your streams/stats without knowing the exact key
- **JSON stats** — `/stats?key=***` returns clean modern JSON
- **Nginx-RTMP-compatible XML** — `/stats-nginx?key=***` for existing tools
- **REST API** — Create/manage streams, query sessions
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
  │   ├── /api/v1/streams     CRUD
  │   ├── /api/v1/streams/:id/stats
  │   ├── /stats              Nginx-RTMP XML (key-protected)
  │   └── /stats-nginx        Alias (identical)
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
- pthread
- SQLite3 dev
- [librtmp2](https://github.com/AlexanderWagnerDev/librtmp2) (workspace sibling)

### Compile

```bash
# Clone both repos side by side
git clone https://github.com/AlexanderWagnerDev/librtmp2.git
git clone https://github.com/AlexanderWagnerDev/librtmp2-server.git

# Build librtmp2 first
cd librtmp2 && make release && cd ..

# Build the server
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
| GET | `/api/v1/streams/:id/stats?key=<stats_key>` | Per-stream stats |

### Stats (key-protected via query param)

| Endpoint | Description |
|----------|-------------|
| `/stats?key=<stats_key>` | JSON stats (modern) |
| `/stats-nginx?key=<stats_key>` | XML (nginx-rtmp compatible) |

---

## Example: Create a stream

```bash
curl -X POST http://localhost:8080/api/v1/streams \
  -H "Authorization: Bearer change-me-to-a-secure-token" \
  -H "Content-Type: application/json" \
  -d '{"id":"mystream","name":"My Live Stream","app":"live"}'
```

Response:
```json
{
  "id": "mystream",
  "name": "My Live Stream",
  "app": "live",
  "publish_key": "pub_mystream_1719480000",
  "play_key": "pl_mystream_1719480001",
  "stats_key": "st_mystream_1719480002",
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
│   ├── rtmp_callbacks.c      librtmp2 → DB bridge
│   └── logger.c               Logging
├── tests/                     Unit tests
├── CMakeLists.txt
├── Dockerfile
├── docker-compose.yml
└── config.example.json
```

---

## Roadmap

| Feature | Status |
|---------|--------|
| RTMP/E-RTMP ingest | Planned |
| SQLite persistence | Planned |
| Unique keys per stream | Planned |
| Nginx-RTMP-compatible /stats | Planned |
| REST API | Planned |
| Per-stream stats | Planned |
| Persistent stats history | Planned |
| Multi-node | Future |

---

## License

MIT — see [LICENSE](LICENSE)
