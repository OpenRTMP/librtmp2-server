# Build stage
FROM alpine:latest AS builder

RUN apk add --no-cache \
    build-base \
    cmake \
    git \
    linux-headers \
    pkgconf

WORKDIR /build

RUN git clone https://github.com/AlexanderWagnerDev/librtmp2.git /build/librtmp2 && \
    cd /build/librtmp2 && make release

# Copy server source
COPY . /build/librtmp2-server
WORKDIR /build/librtmp2-server

RUN mkdir build && cd build && \
    cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DLRTMP2_DIR=/build/librtmp2 && \
    make -j$(nproc)

# Runtime stage
FROM alpine:latest

RUN apk add --no-cache libstdc++ libgcc

COPY --from=builder /build/librtmp2-server/build/librtmp2-server /usr/local/bin/
COPY --from=builder /build/librtmp2-server/web /usr/local/share/librtmp2-server/web
COPY --from=builder /build/librtmp2-server/config.example.json /etc/librtmp2-server/config.json

EXPOSE 1935 8080

HEALTHCHECK --interval=30s --timeout=3s \
    CMD wget -qO- http://localhost:8080/api/v1/health || exit 1

ENTRYPOINT ["librtmp2-server"]
CMD ["-c", "/etc/librtmp2-server/config.json"]
