# librtmp2-server Makefile
# Quick build without CMake. For full builds use CMake.

CC ?= gcc

# librtmp2 dependency location. Local development keeps it as a sibling
# checkout (../librtmp2); CI clones it into a subdirectory of the workspace
# (./librtmp2). Auto-detect whichever exists (falling back to the sibling
# path), and allow an explicit override, e.g. `make LRTMP2_DIR=/path/to/librtmp2`.
LRTMP2_DIR ?= $(firstword $(wildcard ../librtmp2 librtmp2) ../librtmp2)
LRTMP2_A = $(LRTMP2_DIR)/liblibrtmp2.a

CFLAGS = -Wall -Wextra -Wpedantic -Wshadow -Wstrict-prototypes
CFLAGS += -D_GNU_SOURCE -D_POSIX_C_SOURCE=200809L
CFLAGS += -Iinclude -I$(LRTMP2_DIR)/include -I$(LRTMP2_DIR)/src

# Mongoose
MONGOOSE_DIR = build/mongoose
CFLAGS += -I$(MONGOOSE_DIR)

# SQLite
SQLITE_CFLAGS := $(shell pkg-config --cflags sqlite3 2>/dev/null)
SQLITE_LIBS := $(shell pkg-config --libs sqlite3 2>/dev/null)
CFLAGS += $(SQLITE_CFLAGS)

# Linuxbrew fallback for sqlite3
ifeq ($(SQLITE_CFLAGS),)
  ifneq ($(wildcard /home/linuxbrew/.linuxbrew/include/sqlite3.h),)
    CFLAGS += -I/home/linuxbrew/.linuxbrew/include
    SQLITE_LIBS = -L/home/linuxbrew/.linuxbrew/lib -lsqlite3
  endif
endif

# TLS / RTMPS. librtmp2 builds TLS in by default, so the server links OpenSSL by
# default too. `make TLS=0` builds a plaintext-only librtmp2 and drops OpenSSL.
TLS ?= 1
ifeq ($(TLS),1)
  OPENSSL_LIBS ?= $(shell pkg-config --libs openssl 2>/dev/null || echo -lssl -lcrypto)
endif

ifdef DEBUG
  CFLAGS += -g -O0 -DDEBUG
else
  CFLAGS += -O2 -DNDEBUG
endif

ifdef ASAN
  CFLAGS += -fsanitize=address -fno-omit-frame-pointer
  LDFLAGS += -fsanitize=address
endif

ifdef UBSAN
  CFLAGS += -fsanitize=undefined
  LDFLAGS += -fsanitize=undefined
endif

# Source files (excluding cli.c which has main())
LIB_SRCS = src/config.c src/db.c src/http.c src/rtmp_callbacks.c src/server.c src/logger.c
LIB_OBJS = $(LIB_SRCS:.c=.o)

# Mongoose object
MONGOOSE_SRC = $(MONGOOSE_DIR)/mongoose.c
MONGOOSE_HDR = $(MONGOOSE_DIR)/mongoose.h
MONGOOSE_OBJ = $(MONGOOSE_DIR)/mongoose.o
MONGOOSE_BASE = https://raw.githubusercontent.com/cesanta/mongoose/7.14

STATIC_LIB = liblibrtmp2-server.a
SERVER_BIN = librtmp2-server

# Test files
TEST_SRCS = tests/unit/main.c tests/unit/test_db.c tests/unit/test_config.c tests/unit/test_http_stats.c tests/unit/test_stream_id.c tests/unit/test_keygen.c
TEST_OBJS = $(TEST_SRCS:.c=.o)
TEST_BIN = tests/run_tests
TEST_BIN_ASAN = tests/run_tests_asan

# Interop test files — end-to-end tests that spawn the real server binary
INTEROP_SRCS = tests/integration/main.c tests/integration/test_interop_obs.c tests/integration/test_interop_ffmpeg.c tests/integration/test_interop_haishinkkit.c tests/integration/test_interop_concurrent.c
INTEROP_OBJS = $(INTEROP_SRCS:.c=.o)
INTEROP_BIN = tests/run_interop_tests

# Interop tests need curl
CURL_CFLAGS := $(shell pkg-config --cflags libcurl 2>/dev/null)
CURL_LIBS := $(shell pkg-config --libs libcurl 2>/dev/null)
ifeq ($(CURL_LIBS),)
  CURL_LIBS = -lcurl
endif

.PHONY: debug release test clean asan ubsan all

all: $(STATIC_LIB) $(SERVER_BIN)

debug:
	$(MAKE) DEBUG=1 all

release:
	$(MAKE) all

# Ensure librtmp2 is built (matching this build's TLS setting)
$(LRTMP2_A):
	$(MAKE) -C $(LRTMP2_DIR) release TLS=$(TLS)

# Mongoose — fetch both the amalgamated source and its header.
$(MONGOOSE_HDR):
	mkdir -p $(MONGOOSE_DIR)
	curl -fsSL $(MONGOOSE_BASE)/mongoose.h -o $(MONGOOSE_HDR)

$(MONGOOSE_SRC): $(MONGOOSE_HDR)
	curl -fsSL $(MONGOOSE_BASE)/mongoose.c -o $(MONGOOSE_SRC)

$(MONGOOSE_OBJ): $(MONGOOSE_SRC)
	$(CC) $(CFLAGS) -c $< -o $@

# Library objects — the server sources #include "mongoose.h", so make sure the
# header is fetched (order-only) before any of them compile.
src/%.o: src/%.c | $(MONGOOSE_HDR)
	$(CC) $(CFLAGS) -c $< -o $@

# Static library — server objects + mongoose. librtmp2 is a separate archive
# that consumers link alongside this one (see SERVER_BIN below).
$(STATIC_LIB): $(LIB_OBJS) $(MONGOOSE_OBJ)
	ar rcs $@ $(LIB_OBJS) $(MONGOOSE_OBJ)

# Server binary
src/cli.o: src/cli.c | $(MONGOOSE_HDR)
	$(CC) $(CFLAGS) -c $< -o $@

$(SERVER_BIN): src/cli.o $(LIB_OBJS) $(MONGOOSE_OBJ) $(LRTMP2_A)
	$(CC) $(LDFLAGS) -o $@ src/cli.o $(LIB_OBJS) $(MONGOOSE_OBJ) $(LRTMP2_A) $(SQLITE_LIBS) $(OPENSSL_LIBS) -lpthread -lm

# Tests
tests/unit/%.o: tests/unit/%.c
	$(CC) $(CFLAGS) -c $< -o $@

$(TEST_BIN): $(TEST_OBJS) src/config.o src/db.o src/logger.o $(LRTMP2_A)
	$(CC) $(LDFLAGS) -o $@ $(TEST_OBJS) src/config.o src/db.o src/logger.o $(SQLITE_LIBS) $(OPENSSL_LIBS) -lpthread -lm

$(TEST_BIN_ASAN): $(TEST_SRCS) src/config.c src/db.c src/logger.c
	$(CC) $(CFLAGS) -fsanitize=address -fno-omit-frame-pointer -g -O0 -DDEBUG \
	    -o $@ $(TEST_SRCS) src/config.c src/db.c src/logger.c \
	    $(SQLITE_LIBS) -lpthread -lm

test: $(TEST_BIN)
	./$(TEST_BIN)

asan: $(TEST_BIN_ASAN)
	./$(TEST_BIN_ASAN)

ubsan:
	$(MAKE) DEBUG=1 UBSAN=1 $(TEST_BIN)
	./$(TEST_BIN)

# Interop tests — build and run (requires server binary + librtmp2)
tests/integration/%.o: tests/integration/%.c | $(MONGOOSE_HDR)
	$(CC) $(CFLAGS) $(CURL_CFLAGS) -c $< -o $@

$(INTEROP_BIN): $(INTEROP_OBJS) src/config.o src/db.o src/logger.o $(LRTMP2_A)
	$(CC) $(LDFLAGS) -o $@ $(INTEROP_OBJS) src/config.o src/db.o src/logger.o $(LRTMP2_A) $(SQLITE_LIBS) $(OPENSSL_LIBS) $(CURL_LIBS) -lpthread -lm

# Run interop tests — server binary must be built first, then we
# spawn it, drive it, and tear it down.
interop: $(SERVER_BIN) $(INTEROP_BIN)
	@echo "Running interop tests (spawns server binary)..."
	LRTMP2_DB_PATH=$(INTEROP_DB_PATH) $(INTEROP_BIN)
	@echo "Interop tests complete."

clean:
	rm -f $(LIB_OBJS) $(MONGOOSE_OBJ) src/cli.o $(STATIC_LIB) $(SERVER_BIN)
	rm -f $(TEST_OBJS) $(TEST_BIN) $(TEST_BIN_ASAN)
	rm -f $(INTEROP_OBJS) $(INTEROP_BIN)
	rm -rf $(MONGOOSE_DIR)
