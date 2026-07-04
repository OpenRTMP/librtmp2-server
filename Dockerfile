# Build stage
FROM rust:latest AS builder

WORKDIR /build
COPY . .
RUN cargo build --release

# Runtime stage (glibc — matches the rust:latest builder; Alpine/musl cannot run this binary)
FROM debian:bookworm-slim

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 wget \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/librtmp2-server /usr/local/bin/
COPY --from=builder /build/config.example.env /etc/librtmp2-server/config.env

# Create non-root user and data directory
RUN useradd -r -s /usr/sbin/nologin -d /nonexistent -M openrtmp \
    && mkdir -p /data \
    && chown openrtmp:openrtmp /data

ENV LRTMP2_DB=/data/server.db

# Drop privileges
USER openrtmp

EXPOSE 1935 8080

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -qO- http://localhost:8080/api/v1/health || exit 1

ENTRYPOINT ["librtmp2-server"]
CMD ["-c", "/etc/librtmp2-server/config.env"]
