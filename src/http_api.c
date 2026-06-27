/**
 * http_api.c — Embedded HTTP server using Mongoose
 *
 * Provides:
 * - REST API on /api/v1/*
 * - Static file serving for web UI on /
 * - Health check endpoint
 */
#include "librtmp2-server/http_api.h"
#include "librtmp2-server/stream_registry.h"
#include "librtmp2-server/session_manager.h"
#include "librtmp2-server/stats_collector.h"
#include "librtmp2-server/logger.h"

#include "mongoose.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

struct http_server {
    struct mg_mgr   mgr;
    server_config_t config;
    char            listen_addr[128];
    int             running;

    /* Subsystem refs */
    stream_registry_t  *registry;
    session_manager_t  *sessions;
    stats_collector_t  *stats;
};

/* --- JSON helper --- */

static void json_escape(struct mg_str dst, const char *src)
{
    (void)dst; (void)src;
    /* simple — real impl would escape quotes/backslashes */
}

static void http_send_json(struct mg_connection *c, int status, const char *json)
{
    mg_http_printf_head(c, status,
        "Content-Type: application/json\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n\r\n%s",
        strlen(json), json);
}

static void http_send_error(struct mg_connection *c, int status, const char *code, const char *msg)
{
    char buf[512];
    snprintf(buf, sizeof(buf),
        "{\"error\":{\"code\":\"%s\",\"message\":\"%s\"}}",
        code, msg);
    http_send_json(c, status, buf);
}

/* --- Auth check --- */

static bool check_auth(struct http_server *http, struct mg_http_message *hm)
{
    if (!http->config.api_token[0]) return true; /* no auth configured */

    char token[256] = {0};
    struct mg_str *auth = mg_http_get_header(hm, "Authorization");
    if (auth && auth->len > 7 && strncmp(auth->ptr, "Bearer ", 7) == 0) {
        size_t tlen = auth->len - 7;
        if (tlen >= sizeof(token)) tlen = sizeof(token) - 1;
        memcpy(token, auth->ptr + 7, tlen);
        token[tlen] = '\0';
        /* Trim trailing whitespace/newline */
        for (int i = (int)tlen - 1; i >= 0; i--) {
            if (token[i] == '\r' || token[i] == '\n' || token[i] == ' ')
                token[i] = '\0';
            else break;
        }
    }
    return strcmp(token, http->config.api_token) == 0;
}

/* --- API Handlers --- */

static void handle_health(struct mg_connection *c, struct http_server *http)
{
    (void)http;
    time_t now = time(NULL);
    char buf[256];
    snprintf(buf, sizeof(buf),
        "{\"status\":\"ok\",\"uptime\":%ld,\"timestamp\":%ld}",
        (long)(now - http->stats->started), (long)now);
    http_send_json(c, 200, buf);
}

static void handle_stats_overview(struct mg_connection *c, struct http_server *http)
{
    if (!check_auth(http, NULL)) {
        http_send_error(c, 401, "UNAUTHORIZED", "Missing or invalid token");
        return;
    }
    stats_overview_t ov;
    stats_get_overview(http->stats, &ov);
    char buf[1024];
    snprintf(buf, sizeof(buf),
        "{"
        "\"uptime\":%lu,"
        "\"active_sessions\":%d,"
        "\"active_publishers\":%d,"
        "\"active_players\":%d,"
        "\"total_bytes_in\":%llu,"
        "\"total_bytes_out\":%llu,"
        "\"total_bitrate_in\":%.0f,"
        "\"total_streams\":%d,"
        "\"error_count\":%d"
        "}",
        (unsigned long)ov.uptime_seconds,
        ov.active_sessions,
        ov.active_publishers,
        ov.active_players,
        (unsigned long long)ov.total_bytes_in,
        (unsigned long long)ov.total_bytes_in,
        ov.total_bitrate_in,
        ov.total_streams,
        ov.error_count);
    http_send_json(c, 200, buf);
}

static void handle_streams_list(struct mg_connection *c, struct http_server *http)
{
    if (!check_auth(http, NULL)) {
        http_send_error(c, 401, "UNAUTHORIZED", "Missing or invalid token");
        return;
    }

    char *buf = malloc(65536);
    if (!buf) { http_send_error(c, 500, "INTERNAL", "Out of memory"); return; }

    int off = 0;
    off += snprintf(buf + off, 65536 - off, "[");
    (void)http;

    /* TODO: iterate registry — stored callback based */
    off += snprintf(buf + off, 65536 - off, "]");

    http_send_json(c, 200, buf);
    free(buf);
}

static void handle_sessions_list(struct mg_connection *c, struct http_server *http)
{
    if (!check_auth(http, NULL)) {
        http_send_error(c, 401, "UNAUTHORIZED", "Missing or invalid token");
        return;
    }

    char *buf = malloc(65536);
    if (!buf) { http_send_error(c, 500, "INTERNAL", "Out of memory"); return; }

    int off = 0;
    off += snprintf(buf + off, 65536 - off, "[");
    /* TODO: iterate sessions */
    off += snprintf(buf + off, 65536 - off, "]");

    http_send_json(c, 200, buf);
    free(buf);
}

/* --- Mongoose event handler --- */

static void http_handler(struct mg_connection *c, int ev, void *ev_data, void *fn_data)
{
    if (ev != MG_EV_HTTP_MSG) return;

    struct http_server *http = (struct http_server *)fn_data;
    struct mg_http_message *hm = (struct mg_http_message *)ev_data;
    struct mg_str uri = hm->uri;

    /* API routes */
    if (mg_http_match_uri(hm, "/api/v1/health")) {
        handle_health(c, http);
        return;
    }

    if (mg_http_match_uri(hm, "/api/v1/stats/overview")) {
        handle_stats_overview(c, http);
        return;
    }

    if (mg_http_match_uri(hm, "/api/v1/streams")) {
        handle_streams_list(c, http);
        return;
    }

    if (mg_http_match_uri(hm, "/api/v1/sessions")) {
        handle_sessions_list(c, http);
        return;
    }

    /* Default: static file serving */
    struct mg_serve_http_opts opts;
    memset(&opts, 0, sizeof(opts));
    opts.document_root = http->config.web_root;
    mg_http_serve_dir(c, hm, &opts);
}

/* --- Public API --- */

http_server_t *http_server_create(const server_config_t *config)
{
    http_server_t *http = calloc(1, sizeof(http_server_t));
    if (!http) return NULL;
    memcpy(&http->config, config, sizeof(*config));
    return http;
}

void http_server_destroy(http_server_t *http)
{
    if (!http) return;
    mg_mgr_free(&http->mgr);
    free(http);
}

int http_server_start(http_server_t *http)
{
    snprintf(http->listen_addr, sizeof(http->listen_addr), "http://%s", http->config.http_bind);
    mg_mgr_init(&http->mgr);
    struct mg_connection *c = mg_http_listen(&http->mgr, http->listen_addr, http_handler, http);
    if (!c) {
        log_error("Failed to start HTTP server on %s", http->config.http_bind);
        return -1;
    }
    log_info("HTTP server listening on %s", http->config.http_bind);
    return 0;
}

void http_server_stop(http_server_t *http)
{
    http->running = 0;
}

void http_server_set_stream_registry(http_server_t *http, void *reg)
{
    http->registry = (stream_registry_t *)reg;
}

void http_server_set_session_manager(http_server_t *http, void *mgr)
{
    http->sessions = (session_manager_t *)mgr;
}

void http_server_set_stats_collector(http_server_t *http, void *stats)
{
    http->stats = (stats_collector_t *)stats;
}

void http_server_poll(http_server_t *http, int timeout_ms)
{
    mg_mgr_poll(&http->mgr, timeout_ms);
}
