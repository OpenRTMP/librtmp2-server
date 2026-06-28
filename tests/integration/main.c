/**
 * main.c — Interop test runner for librtmp2-server
 *
 * Spawns the actual server binary in-process (fork + exec), waits for it
 * to be ready, then drives each interop scenario against it via librtmp2
 * client API + libcurl HTTP calls. The server is torn down at the end.
 *
 * Exit code: 0 = all tests passed, 1 = any failure.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/wait.h>
#include <signal.h>
#include <time.h>
#include <curl/curl.h>

#include "test_interop_common.h"

extern int test_interop_obs_main(void);
extern int test_interop_ffmpeg_main(void);
extern int test_interop_haishinkkit_main(void);
extern int test_interop_concurrent_main(void);

static pid_t g_server_pid = -1;

/* Size for config + template templates */
#define CONFIG_SIZE 1024

/* Write a test-specific config file and spawn the server */
static int spawn_server(void)
{
    interop_cleanup_db();

    char config_path[256];
    snprintf(config_path, sizeof(config_path),
             "/tmp/librtmp2_interop_config_%d.json", getpid());

    FILE *f = fopen(config_path, "w");
    if (!f) {
        fprintf(stderr, "FAIL: Cannot create test config: %s\n", config_path);
        return -1;
    }

    fprintf(f,
        "{\n"
        "  \"rtmp\": {\n"
        "    \"bind\": \"127.0.0.1:%d\",\n"
        "    \"max_connections\": 64,\n"
        "    \"chunk_size\": 4096\n"
        "  },\n"
        "  \"tls\": { \"enabled\": false },\n"
        "  \"http\": {\n"
        "    \"bind\": \"127.0.0.1:%d\"\n"
        "  },\n"
        "  \"auth\": {\n"
        "    \"api_token\": \"%s\"\n"
        "  },\n"
        "  \"log_level\": 1,\n"
        "  \"log_file\": \"\"\n"
        "}\n",
        INTEROP_RTMP_PORT, INTEROP_HTTP_PORT, INTEROP_TOKEN);
    fclose(f);

    pid_t pid = fork();
    if (pid < 0) {
        perror("fork");
        return -1;
    }

    if (pid == 0) {
        /* Child: exec the server binary */
        /* Set DB path via env var (server.c reads LRTMP2_DB) */
        setenv("LRTMP2_DB", INTEROP_DB_PATH, 1);
        execl("./librtmp2-server", "librtmp2-server",
              "-c", config_path,
              NULL);
        /* If we get here, exec failed */
        perror("execl librtmp2-server");
        _exit(127);
    }

    g_server_pid = pid;
    printf("[interop] Server spawned (pid=%d), waiting for readiness...\n", pid);

    /* Wait for HTTP port to become available (up to 5 seconds) */
    CURL *curl = curl_easy_init();
    if (!curl) return -1;

    char health_url[256];
    interop_http_url(health_url, sizeof(health_url), "/api/v1/health");

    int ready = 0;
    for (int i = 0; i < 50; i++) {
        usleep(100000); /* 100ms between tries */

        /* Also check if the process died */
        int status;
        pid_t ret = waitpid(pid, &status, WNOHANG);
        if (ret == pid) {
            fprintf(stderr, "FAIL: Server process exited before becoming ready\n");
            curl_easy_cleanup(curl);
            return -1;
        }

        curl_easy_setopt(curl, CURLOPT_URL, health_url);
        curl_easy_setopt(curl, CURLOPT_NOBODY, 1L); /* HEAD request */
        curl_easy_setopt(curl, CURLOPT_TIMEOUT, 2L);
        CURLcode rc = curl_easy_perform(curl);
        if (rc == CURLE_OK) {
            ready = 1;
            break;
        }
    }
    curl_easy_cleanup(curl);

    if (!ready) {
        fprintf(stderr, "FAIL: Server did not become ready within 5 seconds\n");
        kill(g_server_pid, SIGTERM);
        waitpid(g_server_pid, NULL, 0);
        g_server_pid = -1;
        return -1;
    }

    printf("[interop] Server ready.\n");
    return 0;
}

static void teardown_server(void)
{
    if (g_server_pid > 0) {
        printf("[interop] Tearing down server (pid=%d)...\n", g_server_pid);
        interop_reap_server(g_server_pid);
        g_server_pid = -1;
    }
    interop_cleanup_db();
}

int main(void)
{
    printf("=== librtmp2-server interop tests ===\n\n");

    if (spawn_server() != 0) {
        printf("\n=== FAIL: could not start server ===\n");
        return 1;
    }

    int total_passed = 0;
    int total_tests = 4;

    printf("\nRunning interop test scenarios...\n\n");

    printf("=== [1/4] OBS-style publish/play ===\n");
    total_passed += (test_interop_obs_main() == 0) ? 1 : 0;

    printf("=== [2/4] FFmpeg-style ingestion ===\n");
    total_passed += (test_interop_ffmpeg_main() == 0) ? 1 : 0;

    printf("=== [3/4] HaishinKit mobile publish ===\n");
    total_passed += (test_interop_haishinkkit_main() == 0) ? 1 : 0;

    printf("=== [4/4] Concurrent streams stress ===\n");
    total_passed += (test_interop_concurrent_main() == 0) ? 1 : 0;

    teardown_server();

    printf("\n=== Interop Results: %d/%d suites passed ===\n",
           total_passed, total_tests);

    return (total_passed == total_tests) ? 0 : 1;
}
