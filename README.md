# librtmp2-server

RTMP / E-RTMP media server built on [librtmp2](https://github.com/AlexanderWagnerDev/librtmp2).

Ingest, stream registry, HTTP API, and a modern stats UI — focused on RTMP/E-RTMP only.

[![License](https://img.shields.io/github/license/AlexanderWagnerDev/librtmp2-server)](LICENSE)
[![Status](https://img.shields.io/badge/status-alpha-orange)]()
[![Language](https://img.shields.io/badge/language-C-blue)]()

---

## Features

- **RTMP / E-RTMP ingest** — Legacy RTMP + Enhanced RTMP v1/v2 (HEVC, AV1, VP9)
- **Stream registry** — Create and manage streams via API or config
- **HTTP API** — RESTful control plane (`/api/v1/streams`, `/api/v1/sessions`, `/api/v1/stats`)
- **Stats UI** — Modern dashboard with live stream status, bitrate, codec info
- **Session management** — Track publishers and players, disconnect control
- **Config file** — JSON-based configuration
- **Docker-ready** — Lightweight Alpine container image

---

## Architecture

```text
OBS / FFmpeg / App
        │
        ▼
  librtmp2-server
  ├── RTMP Listener (port 1935)
  ├── Stream Registry
  ├── HTTP API       (port 8080)
  ├── Stats Collector
  ├── Web UI
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
- [librtmp2](https://github.com/AlexanderWagnerDev/librtmp2) (workspace sibling or clone)

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

## Configuration

```json
{
  "rtmp": {
    "bind": "0.0.0.0:1935",
    "max_connections": 100,
    "chunk_size": 4096
  },
  "http": {
    "bind": "0.0.0.0:8080"
  },
  "auth": {
    "api_token": "change-me",
    "require_stream_key": true
  },
  "log_level": 2,
  "log_file": "",
  "web_root": "./web"
}
```

---

## HTTP API

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/v1/health` | Health check (no auth) |
| GET | `/api/v1/stats/overview` | Server stats |
| GET | `/api/v1/streams` | List streams |
| POST | `/api/v1/streams` | Create stream |
| GET | `/api/v1/streams/:id` | Get stream |
| PATCH | `/api/v1/streams/:id` | Update stream |
| DELETE | `/api/v1/streams/:id` | Delete stream |
| GET | `/api/v1/sessions` | List sessions |
| GET | `/api/v1/sessions/:id` | Get session |
| POST | `/api/v1/sessions/:id/disconnect` | Disconnect session |

All endpoints except `/health` require `Authorization: Bearer <token>`.

---

## Stats UI

Open `http://localhost:8080/` in your browser for the live dashboard.

---

## Docker

```bash
docker compose up -d
```

Or manually:

```bash
docker build -t librtmp2-server .
docker run -d \
  --name rtmp-server \
  -p 1935:1935 \
  -p 8080:8080 \
  librtmp2-server
```

---

## Project Structure

```text
librtmp2-server/
├── include/librtmp2-server/   Public headers
├── src/                       Source files
│   ├── cli.c                  Entry point
│   ├── server.c               Main app context
│   ├── config.c               JSON config parser
│   ├── stream_registry.c      Stream CRUD
│   ├── session_manager.c      Active session tracking
│   ├── stats_collector.c      Aggregated stats
│   ├── http_api.c             HTTP server (Mongoose)
│   ├── http_static.c          Static file helpers
│   ├── rtmp_callbacks.c      librtmp2 → server bridge
│   └── logger.c               Logging
├── web/                       Static web UI
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
| Stream registry | Planned |
| HTTP API | Planned |
| Stats UI | Planned |
| Session management | Planned |
| Persistent stats | Planned |
| Multi-node | Future |

---

## License

MIT — see [LICENSE](LICENSE)
