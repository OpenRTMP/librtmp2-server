# Build stage
FROM alpine:latest AS builder

RUN apk add --no-cache \
    build-base \
    cmake \
    git \
    linux-headers \
    sqlite \
    pkgconf \
    openssl-dev

WORKDIR /build

# Clone and build librtmp2 (static library)
RUN git clone --depth 1 https://github.com/OpenRTMP/librtmp2.git /build/librtmp2 && \
    cd /build/librtmp2 && make release

# Copy and build server
COPY . /build/server
WORKDIR /build/server

RUN mkdir build && cd build && \
    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DLRTMP2_DIR=/build/librtmp2 \
        -DENABLE_TESTS=OFF && \
    make -j$(nproc)

# Runtime stage
FROM alpine:latest

RUN apk add --no-cache libstdc++ libgcc openssl

COPY --from=builder /build/server/build/librtmp2-server /usr/local/bin/
COPY --from=builder /build/server/config.example.json /etc/librtmp2-server/config.json

# Create non-root user and data directory
RUN adduser -D -H -s /sbin/nologin appuser && \
    mkdir -p /data && \
    chown appuser:appuser /data

ENV LRTMP2_DB_PATH=/data/server.db

# Drop privileges
USER appuser

EXPOSE 1935 8080

HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD wget -qO- http://localhost:8080/api/v1/health || exit 1

ENTRYPOINT ["librtmp2-server"]
CMD ["-c", "/etc/librtmp2-server/config.json"]
