# librtmp2-server Makefile
# Quick build without CMake. For full builds use CMake.

CC ?= gcc
CFLAGS = -Wall -Wextra -Wpedantic -Wshadow -Wstrict-prototypes
CFLAGS += -Iinclude -I../librtmp2/include -I../librtmp2/src

# Mongoose
MONGOOSE_DIR = build/mongoose
CFLAGS += -I$(MONGOOSE_DIR)

# SQLite
SQLITE_CFLAGS := $(shell pkg-config --cflags sqlite3 2>/dev/null)
SQLITE_LIBS := $(shell pkg-config --libs sqlite3 2>/dev/null)
CFLAGS += $(SQLITE_CFLAGS)

# librtmp2
LRTMP2_DIR = librtmp2
LRTMP2_A = $(LRTMP2_DIR)/liblibrtmp2.a

ifdef DEBUG
  CFLAGS += -g -O0 -DDEBUG
else
  CFLAGS += -O2 -NDEBUG
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
MONGOOSE_OBJ = $(MONGOOSE_DIR)/mongoose.o

STATIC_LIB = liblibrtmp2-server.a
SERVER_BIN = librtmp2-server

# Test files
TEST_SRCS = tests/unit/main.c tests/unit/test_db.c tests/unit/test_config.c tests/unit/test_http_stats.c
TEST_OBJS = $(TEST_SRCS:.c=.o)
TEST_BIN = tests/run_tests
TEST_BIN_ASAN = tests/run_tests_asan

.PHONY: debug release test clean asan ubsan all

all: $(STATIC_LIB) $(SERVER_BIN)

debug:
	$(MAKE) DEBUG=1 all

release:
	$(MAKE) all

# Ensure librtmp2 is built
$(LRTMP2_A):
	$(MAKE) -C $(LRTMP2_DIR) release

# Mongoose
$(MONGOOSE_SRC):
	mkdir -p $(MONGOOSE_DIR)
	curl -fsSL https://raw.githubusercontent.com/cesanta/mongoose/7.14/mongoose.c -o $(MONGOOSE_SRC)

$(MONGOOSE_OBJ): $(MONGOOSE_SRC)
	$(CC) $(CFLAGS) -c $< -o $@

# Library objects
src/%.o: src/%.c
	$(CC) $(CFLAGS) -c $< -o $@

# Static library
$(STATIC_LIB): $(LIB_OBJS) $(MONGOOSE_OBJ)
	ar rcs $@ $(LIB_OBJS) $(MONGOOSE_OBJ) $(LRTMP2_A)

# Server binary
src/cli.o: src/cli.c
	$(CC) $(CFLAGS) -c $< -o $@

$(SERVER_BIN): src/cli.o $(LIB_OBJS) $(MONGOOSE_OBJ) $(LRTMP2_A)
	$(CC) $(LDFLAGS) -o $@ src/cli.o $(LIB_OBJS) $(MONGOOSE_OBJ) -L$(LRTMP2_DIR) -llibrtmp2 $(SQLITE_LIBS) -lpthread -lm

# Tests
tests/unit/%.o: tests/unit/%.c
	$(CC) $(CFLAGS) -c $< -o $@

$(TEST_BIN): $(TEST_OBJS) src/config.o src/db.o src/logger.o $(LRTMP2_A)
	$(CC) $(LDFLAGS) -o $@ $(TEST_OBJS) src/config.o src/db.o src/logger.o $(SQLITE_LIBS) -lpthread -lm

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

clean:
	rm -f $(LIB_OBJS) $(MONGOOSE_OBJ) src/cli.o $(STATIC_LIB) $(SERVER_BIN)
	rm -f $(TEST_OBJS) $(TEST_BIN) $(TEST_BIN_ASAN)
	rm -rf $(MONGOOSE_DIR)
