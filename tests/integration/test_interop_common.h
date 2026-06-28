/**
 * test_interop_common.h — Shared helpers for interop tests
 *
 * Each interop test spawns the real librtmp2-server in-process (fork),
 * drives it with a librtmp2 client, and verifies end-to-end behaviour.
 * These are not unit tests — they verify real-world tool compatibility
 * patterns against the actual server binary.
 */
#ifndef TEST_INTEROP_COMMON_H
#define TEST_INTEROP_COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <time.h>
#include <errno.h>

/* ---- Configuration ---- */
#define INTEROP_RTMP_PORT   1939
#define INTEROP_HTTP_PORT   8089
#define INTEROP_TOKEN       "interop-test-token"
#define INTEROP_TIMEOUT_MS  5000
#define INTEROP_DB_PATH     "/tmp/librtmp2_interop_test.db"

/* ---- Helpers ---- */

/* Generate a short random suffix for unique stream names */
static void interop_random_suffix(char *buf, size_t len)
{
    static int seeded = 0;
    if (!seeded) {
        srand((unsigned)time(NULL) ^ (unsigned)getpid());
        seeded = 1;
    }
    for (size_t i = 0; i < len - 1; i++) {
        buf[i] = 'a' + (rand() % 26);
    }
    buf[len - 1] = '\0';
}

/* Compose a full RTMP URL, e.g. "rtmp://127.0.0.1:1939/live/stream_abc" */
static void interop_rtmp_url(char *buf, size_t bufsz,
                              const char *stream_name)
{
    snprintf(buf, bufsz, "rtmp://127.0.0.1:%d/live/%s",
             INTEROP_RTMP_PORT, stream_name);
}

/* Compose an HTTP API URL */
static void interop_http_url(char *buf, size_t bufsz,
                              const char *path)
{
    snprintf(buf, bufsz, "http://127.0.0.1:%d%s",
             INTEROP_HTTP_PORT, path);
}

/* Remove stale DB before a test to get a clean server start */
static void interop_cleanup_db(void)
{
    unlink(INTEROP_DB_PATH);
    /* Also try WAL/SHM companions */
    char tmp[512];
    snprintf(tmp, sizeof(tmp), "%s-wal", INTEROP_DB_PATH);
    unlink(tmp);
    snprintf(tmp, sizeof(tmp), "%s-shm", INTEROP_DB_PATH);
    unlink(tmp);
}

/* Reap a child server process gracefully */
static int interop_reap_server(pid_t pid)
{
    int status = -1;
    /* Send SIGTERM and wait */
    kill(pid, SIGTERM);
    struct timespec req = {0, 100 * 1000 * 1000}; /* 100ms */
    int waited = 0;
    while (waited < 50) { /* 5s max */
        int ret = waitpid(pid, &status, WNOHANG);
        if (ret == pid) return status;
        if (ret == -1) return -1;
        nanosleep(&req, NULL);
        waited++;
    }
    /* Force kill */
    kill(pid, SIGKILL);
    waitpid(pid, &status, 0);
    return status;
}

/* ---- Result tracking ---- */
typedef struct {
    int  total;
    int  passed;
    char current_test[128];
} interop_result_t;

static void interop_result_init(interop_result_t *r)
{
    r->total = 0;
    r->passed = 0;
}

static void interop_result_record(interop_result_t *r, int ok)
{
    r->total++;
    if (ok) r->passed++;
}

#endif /* TEST_INTEROP_COMMON_H */
