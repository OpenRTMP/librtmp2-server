# Build stage
FROM rust:alpine AS builder

RUN apk add --no-cache build-base

WORKDIR /build
COPY . .
RUN cargo build --release

# Runtime stage
FROM alpine:latest

RUN apk add --no-cache libgcc

COPY --from=builder /build/target/release/librtmp2-server /usr/local/bin/
COPY --from=builder /build/config.example.json /etc/librtmp2-server/config.json

# Create non-root user and data directory
RUN adduser -D -H -s /sbin/nologin appuser && \
    mkdir -p /data && \
    chown appuser:appuser /data

ENV LRTMP2_DB=/data/server.db

# Drop privileges
USER appuser

EXPOSE 1935 8080

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -qO- http://localhost:8080/api/v1/health || exit 1

ENTRYPOINT ["librtmp2-server"]
CMD ["-c", "/etc/librtmp2-server/config.json"]
