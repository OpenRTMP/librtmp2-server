# Build stage (official Rust toolchain — matches package rust-version / OpenRaft)
# Standalone (default `docker compose` context `.`):
#   docker build -t librtmp2-server .
# Monorepo (parent OpenRTMP with sibling librtmp2/):
#   docker build -f librtmp2-server/Dockerfile .
FROM rust:1.97-bookworm AS builder

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        pkg-config libssl-dev ca-certificates git \
    && rm -rf /var/lib/apt/lists/*

ARG LIBRTMP2_GIT=https://github.com/OpenRTMP/librtmp2.git
# Empty = pin to the `rev` in Cargo.toml (standalone reproducible builds).
# Override for monorepo/testing (branch name or commit SHA).
ARG LIBRTMP2_REF=

WORKDIR /build

# Monorepo context provides librtmp2/ + librtmp2-server/; otherwise clone.
COPY . /build/context/
RUN set -eu; \
    if [ -f /build/context/librtmp2-server/Cargo.toml ] && [ -f /build/context/librtmp2/Cargo.toml ]; then \
      mv /build/context/librtmp2 /build/librtmp2; \
      mv /build/context/librtmp2-server /build/librtmp2-server; \
    elif [ -f /build/context/Cargo.toml ] && grep -q 'name = "librtmp2-server"' /build/context/Cargo.toml; then \
      mv /build/context /build/librtmp2-server; \
      ref="${LIBRTMP2_REF}"; \
      if [ -z "$ref" ]; then \
        ref="$(sed -n 's/.*rev = "\([^"]*\)".*/\1/p' /build/librtmp2-server/Cargo.toml | head -n1)"; \
      fi; \
      if [ -z "$ref" ]; then \
        echo "LIBRTMP2_REF empty and no rev= in Cargo.toml" >&2; \
        exit 1; \
      fi; \
      git clone "${LIBRTMP2_GIT}" /build/librtmp2; \
      git -C /build/librtmp2 checkout --detach "$ref"; \
    else \
      echo "Unrecognized build context; expected OpenRTMP parent or librtmp2-server root" >&2; \
      exit 1; \
    fi

WORKDIR /build/librtmp2-server
# Monorepo builds must resolve the sibling checkout, not the pinned git rev.
RUN set -eu; \
    if [ -f /build/librtmp2/Cargo.toml ]; then \
      printf '\n[patch."https://github.com/OpenRTMP/librtmp2"]\nlibrtmp2 = { path = "../librtmp2" }\n' >> Cargo.toml; \
    fi; \
    cargo build --release --features cluster

ARG APP_VERSION=""
RUN version="$APP_VERSION" && \
    if [ -z "$version" ]; then \
        version="$(awk -F '"' '/^version = "/ { print $2; exit }' Cargo.toml)"; \
    fi && \
    test -n "$version" && \
    printf '%s\n' "$version" > /build/VERSION

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ca-certificates libssl3 wget \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --no-create-home --shell /usr/sbin/nologin openrtmp \
    && mkdir -p /data \
    && chown openrtmp:openrtmp /data

COPY --from=builder /build/librtmp2-server/target/release/librtmp2-server /usr/local/bin/librtmp2-server
COPY --from=builder /build/librtmp2-server/.env.example /etc/librtmp2-server/.env
COPY --from=builder /build/VERSION /usr/local/share/openrtmp/VERSION
COPY --from=builder /build/librtmp2-server/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod 0755 /usr/local/bin/entrypoint.sh

ENV LRTMP2_DB=/data/server.db \
    OPENRTMP_VERSION_FILE=/usr/local/share/openrtmp/VERSION

WORKDIR /etc/librtmp2-server

USER openrtmp

# 1935 RTMP, 8080 HTTP; 1940/1941 cluster control/media (optional HA)
EXPOSE 1935 8080 1940 1941

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -qO- http://localhost:8080/api/v1/health || exit 1

ENTRYPOINT ["entrypoint.sh"]
CMD ["librtmp2-server"]
