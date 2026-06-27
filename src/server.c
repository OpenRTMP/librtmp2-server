/**
 * server.c — Main server application context
 */
#include "librtmp2-server/server.h"
#include "librtmp2-server/config.h"
#include "librtmp2-server/stream_registry.h"
#include "librtmp2-server/session_manager.h"
#include "librtmp2-server/stats_collector.h"
#include "librtmp2-server/http_api.h"
#include "librtmp2-server/logger.h"
#include "librtmp2/librtmp2.h"

#include <stdio.h>
#include <string.h>
#include <signal.h>
#include <stdlib.h>

/* Include mongoose for polling */
#include "mongoose.h"

struct lrtmp2_server_app {
    server_config_t     config;
    lrtmp2_server_t    *rtmp_server;
    stream_registry_t  *registry;
    session_manager_t  *sessions;
    stats_collector_t  *stats;
    http_server_t      *http;
    int                 running;
};

static lrtmp2_server_app_t *g_app = NULL;  /* for signal handler */

static void signal_handler(int sig)
{
    (void)sig;
    if (g_app) {
        g_app->running = 0;
    }
}

lrtmp2_server_app_t *server_app_create(const server_config_t *config)
{
    lrtmp2_server_app_t *app = calloc(1, sizeof(lrtmp2_server_app_t));
    if (!app) return NULL;

    memcpy(&app->config, config, sizeof(*config));

    /* Create subsystems */
    app->registry = stream_registry_create();
    app->sessions = session_manager_create();
    app->stats    = stats_collector_create();

    if (!app->registry || !app->sessions || !app->stats) {
        log_error("Failed to create server subsystems");
        server_app_destroy(app);
        return NULL;
    }

    /* Create HTTP server */
    app->http = http_server_create(config);
    if (!app->http) {
        log_error("Failed to create HTTP server");
        server_app_destroy(app);
        return NULL;
    }
    http_server_set_stream_registry(app->http, app->registry);
    http_server_set_session_manager(app->http, app->sessions);
    http_server_set_stats_collector(app->http, app->stats);

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
    if (app->http)      http_server_destroy(app->http);
    if (app->stats)     stats_collector_destroy(app->stats);
    if (app->sessions)  session_manager_destroy(app->sessions);
    if (app->registry)  stream_registry_destroy(app->registry);

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

    /* Create and start RTMP server */
    /* Note: rtmp_bridge setup will be called from rtmp bridge */
    /* For now, we create directly here with a minimal config */
    lrtmp2_server_config_t rtmp_config;
    memset(&rtmp_config, 0, sizeof(rtmp_config));
    rtmp_config.max_connections = app->config.rtmp_max_conn;
    rtmp_config.chunk_size      = app->config.rtmp_chunk_size;
    rtmp_config.userdata        = app; /* will be passed to bridge */

    /* TODO: set callbacks via bridge setup */
    /* rtmp_bridge_setup(&rtmp_config, &bridge, app->registry, app->sessions, app->stats); */

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
        /* Pump Mongoose HTTP events */
        http_server_poll(app->http, 10);
        /* Pump RTMP events */
        lrtmp2_server_poll(app->rtmp_server, 10);
    }

    log_info("Shutting down...");
    return 0;
}

void server_app_stop(lrtmp2_server_app_t *app)
{
    app->running = 0;
}
