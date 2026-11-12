# librtmp2-server

RTMP / E-RTMP media server with a bundled SQLite-backed HTTP API, built on
[librtmp2](https://github.com/OpenRTMP/librtmp2).

Source, full documentation and issue tracker:
**[github.com/OpenRTMP/librtmp2-server](https://github.com/OpenRTMP/librtmp2-server)**
· Web UI: [librtmp2-server-panel](https://hub.docker.com/r/openrtmp/librtmp2-server-panel)

> **Alpha software.** The HTTP/API/DB layer is usable, but the RTMP behaviour
> is still evolving. Test before relying on it in production.

## Quick start

```bash
docker run -d \
  --name librtmp2-server \
  -p 1935:1935 \
  -p 8080:8080 \
  -v librtmp2-data:/data \
  openrtmp/librtmp2-server:latest

docker logs librtmp2-server   # copy the generated API token
```

The server generates an API token on first start and prints it to stderr. To
pin the token instead, pass it as `LRTMP2_API_TOKEN` on the **first** start:

```bash
docker run -d \
  --name librtmp2-server \
  -p 1935:1935 -p 8080:8080 \
  -e LRTMP2_API_TOKEN="$(openssl rand -hex 32)" \
  -v librtmp2-data:/data \
  openrtmp/librtmp2-server:latest
```

The token is stored in SQLite, so later changes to the environment variable
have no effect until the database is re-seeded (delete `/data/server.db*`).

## Supported tags and architectures

- `latest`, `beta`, `alpha` — moving channels
- `0.6.9`, `v0.6.9` — pinned releases (both spellings are published)
- `linux/amd64`, `linux/arm64`, `linux/riscv64`

Mirrors: `ghcr.io/openrtmp/librtmp2-server` and `quay.io/openrtmp/librtmp2-server`.

## Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 1935 | RTMP | Publish / play (always on) |
| 8080 | HTTP | REST API, stats, HLS, health |
| 1936 | RTMPS | Only when `LRTMP2_TLS_ENABLED=true` |
| 1940 | TCP | Cluster control — trusted peers only |
| 1941 | TCP | Cluster media — trusted peers only |

## Volumes

| Path | Purpose |
|------|---------|
| `/data` | SQLite database (and recordings/HLS if enabled) |

The image runs as the unprivileged user `openrtmp`. Make a bind-mounted `/data`
writable for that user (e.g. `chown 1000:1000` or an image-compatible UID).

## Configuration

Configuration is read from `/etc/librtmp2-server/.env` (seeded from
`.env.example`) and from process environment variables with the `LRTMP2_`
prefix, which take precedence:

| Variable | Default | Purpose |
|----------|---------|---------|
| `LRTMP2_RTMP_BIND` | `0.0.0.0:1935` | RTMP listener |
| `LRTMP2_HTTP_BIND` | `0.0.0.0:8080` | HTTP API listener |
| `LRTMP2_DB` | `/data/server.db` | SQLite path |
| `LRTMP2_API_TOKEN` | generated | Fixed API token (first start only) |
| `LRTMP2_LOG_LEVEL` | `2` | `0`=error … `3`=debug |
| `LRTMP2_TLS_ENABLED` + `LRTMP2_TLS_CERT_FILE` / `LRTMP2_TLS_KEY_FILE` | `false` | Adds RTMPS on 1936 |
| `LRTMP2_HTTP_TRUSTED_PROXIES` | empty | Proxies allowed to set `X-Forwarded-For` |
| `LRTMP2_CLUSTER_*` | off | Optional HA cluster (ports 1940/1941) |

Media outputs are opt-in and documented in the repository:

- **Recording** — native FLV (`LRTMP2_MEDIA_RECORDING_ENABLED`)
- **HLS** — FFmpeg-generated, served under `/hls/<stream_id>/` (`LRTMP2_MEDIA_HLS_ENABLED`)
- **Push relay** — RTMP(S) forward to upstream targets (`LRTMP2_MEDIA_PUSH_TARGETS`)
- **Hooks** — trusted commands on publish start/end (`LRTMP2_MEDIA_EXEC_*`)

FFmpeg is included in the image, so HLS, push relay and transcoding work without
extra packages. Recording and hooks do not depend on it.

Full variable reference:
[`.env.example`](https://github.com/OpenRTMP/librtmp2-server/blob/main/.env.example)
· [media outputs](https://github.com/OpenRTMP/librtmp2-server/blob/main/docs/media-outputs.md)
· [clustering](https://github.com/OpenRTMP/librtmp2-server/blob/main/docs/clustering.md)

## Docker Compose

```yaml
services:
  server:
    image: openrtmp/librtmp2-server:latest
    restart: unless-stopped
    environment:
      LRTMP2_HTTP_BIND: 0.0.0.0:8080
      LRTMP2_RTMP_BIND: 0.0.0.0:1935
      LRTMP2_API_TOKEN: ${LRTMP2_API_TOKEN:?set in .env}
    volumes:
      - server-data:/data
    ports:
      - "1935:1935"
      - "8080:8080"

volumes:
  server-data:
```

For the full server + web panel stack, use the panel's
[`compose.quickstart.yml`](https://github.com/OpenRTMP/librtmp2-server-panel/blob/main/compose.quickstart.yml).

## Health and safety

- Built-in healthcheck: `GET http://localhost:8080/api/v1/health`
- Non-root user, no shell login
- Rust binary built against musl, linked against vendored OpenSSL
- API token required for all admin endpoints; public routes are rate-limited

## License

MIT — see [LICENSE](https://github.com/OpenRTMP/librtmp2-server/blob/main/LICENSE).
