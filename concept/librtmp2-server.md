# librtmp2 Server — Product Concept

## Purpose

`librtmp2-server` is the future product **on top of** the `librtmp2` core library. It is a standalone RTMP / E-RTMP server with an **API**, a **stats page**, and a lean operational interface. Product logic is strictly separated from the protocol implementation so that the core can be reused by other projects. [1]

The goal is **not** an overloaded multi-protocol monolith like MediaMTX, but a focused, modern RTMP/E-RTMP server with clean UX, an API-first mindset, and good observability. [1]

***

## Product Vision

The server should become what was originally described:

- Create streams dynamically via API
- Have a proper stats page
- Stay focused on RTMP / E-RTMP
- Require no third-party push targets
- Be lightweight, container-friendly, and self-hostable

The library does the protocol work; the server is the product.

***

## Technical Layering

```text
OBS / FFmpeg / App
        │
        ▼
  librtmp2-server
  ├── RTMP Listener
  ├── Stream Registry
  ├── HTTP API
  ├── Stats Collector
  ├── Web UI
  └── Persistence
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

***

## Language and Component Choices

### Core
- `librtmp2` in **C**

### Server Layer
Recommended options:

| Component | Recommendation | Reason |
|---|---|---|
| API/HTTP | Go or Python | good productivity, clean web stacks |
| Web UI | plain HTML/JS or small frontend | low footprint |
| Persistence | SQLite to start, optionally PostgreSQL | simple initial deployments |
| Containerization | Docker | fits your workflow |

The language for the server layer can be decided later, since it **no longer** defines the protocol foundation.

***

## Feature Set v1

### 1. RTMP/E-RTMP Ingest

- Accept incoming publisher connections
- Validate stream key
- Extract app name and stream key
- Detect codec/FourCC
- Persist session status

### 2. HTTP API

Example endpoints:

```text
POST   /api/v1/streams
GET    /api/v1/streams
GET    /api/v1/streams/:id
PATCH  /api/v1/streams/:id
DELETE /api/v1/streams/:id

GET    /api/v1/sessions
GET    /api/v1/sessions/:id
POST   /api/v1/sessions/:id/disconnect
POST   /api/v1/sessions/:id/reconnect

GET    /api/v1/stats/overview
GET    /api/v1/stats/streams/:id
GET    /api/v1/health
```

### 3. Stats Page

The UI should be modern, clear, and operationally useful:

- Active streams
- Status: online/offline
- Uptime
- Incoming bitrate
- Frame rate
- Video codec / FourCC
- Audio codec
- Number of sessions
- Error rate / recent errors
- Optional history charts

### 4. Stream Registry

Each stream is an object:

```json
{
  "id": "stream_123",
  "name": "Main Stage",
  "app": "live",
  "stream_key": "abc123",
  "enabled": true,
  "require_auth": true,
  "allowed_codecs": ["avc1", "hvc1", "av01"],
  "created_at": "2026-06-26T00:00:00Z"
}
```

### 5. Authentication

At minimum:
- Static API tokens
- Stream-key-based publish auth
- Optional basic auth for stats page

***

## Architecture Modules

### Ingest Worker

Handles incoming RTMP connections and uses `librtmp2` internally. Translates library callbacks into application events.

### Session Manager

Maintains active connections, stream lifecycle, online status, and disconnect reason.

### Stats Collector

Aggregates metrics per stream and session:
- Bytes in/out
- Bitrate
- FPS (rough estimate)
- Codec
- Duration
- Error counters

### REST API

Operational control and data retrieval.

### Web UI

Simple frontend for overview and detail pages.

### Persistence Layer

At minimum, tables/collections for:
- streams
- api_tokens
- session_history
- optional stats_samples

***

## Data Model

### streams

| Field | Type | Purpose |
|---|---|---|
| id | string | Primary key |
| name | string | Display name |
| app | string | RTMP app |
| stream_key | string | Publish key |
| enabled | bool | Active/inactive |
| require_auth | bool | Auth required |
| allowed_codecs | json/text | Allowed codecs |
| created_at | timestamp | Creation time |
| updated_at | timestamp | Last updated |

### sessions

| Field | Type | Purpose |
|---|---|---|
| id | string | Session ID |
| stream_id | string | Reference to stream |
| remote_addr | string | IP/port |
| started_at | timestamp | Connection start |
| ended_at | timestamp | Connection end |
| status | string | active/closed/error |
| video_codec | string | Codec/FourCC |
| audio_codec | string | Audio format |
| bytes_in | bigint | Bytes received |
| last_error | text | Last error |

### stats_samples

| Field | Type | Purpose |
|---|---|---|
| id | integer | PK |
| session_id | string | Reference |
| ts | timestamp | Timestamp |
| bitrate_in | integer | Current bitrate |
| fps | float | Current FPS |
| keyframe_interval | float | Estimated |

***

## API Design Principles

- JSON only
- Stable versioning: `/api/v1`
- Machine-readable error objects
- Clear status codes
- Idempotent GET/DELETE
- Auditability for administrative actions

Error example:

```json
{
  "error": {
    "code": "STREAM_NOT_FOUND",
    "message": "The requested stream does not exist."
  }
}
```

***

## Stats UI Principles

The UI should **not** look like SRS. It should be modern, minimal, and useful.

Views:
- Dashboard
- Streams list
- Stream detail page
- Session detail page
- System health

Key elements:
- Search and filter functionality
- Color-coded status indicators
- Live-updated values
- History over the last minutes/hours
- Usable on mobile, optimized for desktop

***

## Deployment Concept

Recommended structure:

```text
services:
  librtmp2-server:
    image: alexanderwagnerdev/librtmp2-server:latest
    ports:
      - "1935:1935"
      - "8080:8080"
    volumes:
      - ./data:/data
      - ./config:/config
```

Ports:
- `1935/tcp` RTMP
- `8080/tcp` HTTP API + UI

***

## Roadmap

### Phase A — Tech Demo

- Integrate `librtmp2` minimally
- 1 stream key hardcoded
- 1 API route `GET /health`
- 1 web page showing active sessions

### Phase B — MVP

- Stream registry
- CRUD API
- Login / API token
- Dashboard
- Session history

### Phase C — Production Readiness

- Persistent metrics
- Reconnect control
- Finer codec policies
- Better charts
- Multi-node capability

### Phase D — Cluster / Enterprise (optional)

- Multiple ingest nodes
- Central control plane
- Node health
- Scheduling / drain mode

***

## Differentiation from Other Tools

### vs. SRS

- Better product experience
- API-first
- Better UX
- Clear focus on RTMP/E-RTMP
- Own core rather than just a wrapper in the long run

### vs. MediaMTX

- Less protocol overhead
- More targeted use case
- More modern stats and API surface
- Not a generic catch-all router

### vs. nginx-rtmp

- Modern codecs / E-RTMP
- Active product architecture
- API and observability

***

## Success Criteria

`librtmp2-server` is successful when:

- a user can start an RTMP/E-RTMP server within minutes,
- streams can be created via API,
- the stats page is clearer and more useful than SRS,
- the product stays small and focused,
- and the server is built entirely on `librtmp2`, not on SRS.

***

## GitHub Strategy

Recommended repositories under `AlexanderWagnerDev`:

- `AlexanderWagnerDev/librtmp2`
- `AlexanderWagnerDev/librtmp2-server`

Optionally later:
- `AlexanderWagnerDev/librtmp2-python`
- `AlexanderWagnerDev/librtmp2-go`
- `AlexanderWagnerDev/librtmp2-obs-plugin`
