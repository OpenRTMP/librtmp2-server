/**
 * server.c — Main server application context
 *
 * Wires together:
 * - librtmp2 (RTMP protocol)
 * - SQLite (persistence)
 * - Mongoose (HTTP API + nginx-compatible stats)
 */
#include "librtmp2-server/server.h"
#include "librtmp2-server/config.h"
#include "librtmp2-server/db.h"
#include "librtmp2-server/http.h"
#include "librtmp2-server/logger.h"
#include "librtmp2/librtmp2.h"

#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <stdlib.h>

/* Defined in rtmp_callbacks.c — opaque here, only ever held by pointer/value
 * inside lrtmp2_server_app_t and passed straight through to rtmp_bridge_setup. */
typedef struct conn_state conn_state_t;
typedef struct {
    db_context_t *db;
    conn_state_t *conns;
} rtmp_bridge_t;

extern void rtmp_bridge_setup(lrtmp2_server_config_t *config, rtmp_bridge_t *bridge,
                               db_context_t *db);

struct lrtmp2_server_app {
    server_config_t     config;
    lrtmp2_server_t    *rtmp_server;
    db_context_t       *db;
    http_server_t      *http;
    rtmp_bridge_t       bridge;
    int                 running;
};

static lrtmp2_server_app_t *g_app = NULL;

static void signal_handler(int sig)
{
    (void)sig;
    if (g_app) g_app->running = 0;
}

lrtmp2_server_app_t *server_app_create(const server_config_t *config)
{
    lrtmp2_server_app_t *app = calloc(1, sizeof(lrtmp2_server_app_t));
    if (!app) return NULL;

    memcpy(&app->config, config, sizeof(*config));

    /* Open SQLite database */
    const char *db_path = getenv("LRTMP2_DB");
    if (!db_path || !db_path[0]) db_path = "/tmp/librtmp2-server.db";

    app->db = db_open(db_path);
    if (!app->db) {
        log_error("Failed to open database: %s", db_path);
        free(app);
        return NULL;
    }

    /* Create HTTP server */
    app->http = http_server_create(config);
    if (!app->http) {
        log_error("Failed to create HTTP server");
        db_close(app->db);
        free(app);
        return NULL;
    }
    http_server_set_db(app->http, app->db);

    g_app = app;
    signal(SIGINT, signal_handler);
    signal(SIGTERM, signal_handler);

    return app;
}

void server_app_destroy(lrtmp2_server_app_t *app)
{
    if (!app) return;

    if (app->rtmp_server) {
        lrtmp2_server_stop(app->rtmp_server);
        lrtmp2_server_destroy(app->rtmp_server);
    }
    if (app->http)  http_server_destroy(app->http);
    if (app->db)    db_close(app->db);

    if (g_app == app) g_app = NULL;
    free(app);
}

int server_app_run(lrtmp2_server_app_t *app)
{
    int rc;

    log_info("librtmp2-server v0.1.0 starting...");
    log_info("librtmp2 library v%s", lrtmp2_version_string());

    /* Start HTTP server */
    rc = http_server_start(app->http);
    if (rc != 0) return rc;

    /* Create RTMP server with bridge to DB */
    lrtmp2_server_config_t rtmp_config;
    rtmp_bridge_setup(&rtmp_config, &app->bridge, app->db);

    /* RTMPS: enable TLS termination if the operator configured it. librtmp2
     * builds TLS in by default but it can be compiled out, so refuse to start
     * with a clear message rather than silently serving plaintext. */
    if (app->config.tls_enabled) {
        if (!lrtmp2_tls_supported()) {
            log_error("TLS enabled in config but librtmp2 was built without TLS "
                      "support (rebuild librtmp2 with TLS, or disable tls in config)");
            return -1;
        }
        if (app->config.tls_cert_file[0] == '\0' || app->config.tls_key_file[0] == '\0') {
            log_error("TLS enabled but tls.cert_file / tls.key_file not configured");
            return -1;
        }
        rtmp_config.tls_enabled   = 1;
        rtmp_config.tls_cert_file = app->config.tls_cert_file;
        rtmp_config.tls_key_file  = app->config.tls_key_file;
        log_info("RTMPS enabled (cert=%s)", app->config.tls_cert_file);
    } else {
        log_info("RTMPS disabled (plaintext RTMP only)");
    }

    app->rtmp_server = lrtmp2_server_create(&rtmp_config);
    if (!app->rtmp_server) {
        log_error("Failed to create RTMP server");
        return -1;
    }

    rc = lrtmp2_server_listen(app->rtmp_server, app->config.rtmp_bind);
    if (rc != 0) {
        log_error("Failed to listen on %s", app->config.rtmp_bind);
        return -1;
    }

    log_info("RTMP server listening on %s", app->config.rtmp_bind);
    log_info("Server ready — RTMP: %s | HTTP: %s", app->config.rtmp_bind, app->config.http_bind);

    /* Main loop */
    app->running = 1;
    while (app->running) {
        http_server_poll(app->http, 10);
        lrtmp2_server_poll(app->rtmp_server, 10);
    }

    log_info("Shutting down...");
    return 0;
}

void server_app_stop(lrtmp2_server_app_t *app)
{
    app->running = 0;
}
