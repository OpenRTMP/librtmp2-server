# Build stage
FROM rust:alpine AS builder

WORKDIR /build
COPY . .
RUN cargo build --release

# Runtime stage
FROM alpine:latest

RUN apk add --no-cache libgcc

COPY --from=builder /build/target/release/librtmp2-server /usr/local/bin/
COPY --from=builder /build/.env.example /etc/librtmp2-server/.env

# Create non-root user and data directory
RUN adduser -D -H -s /sbin/nologin openrtmp && \
    mkdir -p /data && \
    chown openrtmp:openrtmp /data

ENV LRTMP2_DB=/data/server.db

# Run from the config directory so the server loads ./.env by default
WORKDIR /etc/librtmp2-server

# Drop privileges
USER openrtmp

EXPOSE 1935 8080

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -qO- http://localhost:8080/api/v1/health || exit 1

ENTRYPOINT ["librtmp2-server"]
