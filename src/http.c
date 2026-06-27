/**
 * http.c — HTTP server using Mongoose
 *
 * Endpoints:
 *   GET  /api/v1/health                       no auth
 *   GET  /api/v1/streams                      Bearer token
 *   POST /api/v1/streams                      Bearer token, returns keys
 *   DELETE /api/v1/streams/:id                Bearer token
 *
 *   GET  /stats?key=<stats_key>               JSON stats (modern)
 *   GET  /api/v1/streams/:id/stats?key=<sk>   JSON per-stream stats
 *   GET  /stats-nginx?key=<stats_key>         XML (nginx-rtmp compatible)
 */
#include "librtmp2-server/http.h"
#include "librtmp2-server/db.h"
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
    db_context_t   *db;
};

/* ---------- helpers ---------- */

static void send_json(struct mg_connection *c, int status, const char *body, size_t len)
{
    mg_http_printf_head(c, status,
        "Content-Type: application/json; charset=utf-8\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n\r\n", len);
    mg_write(c, body, len);
}

static void send_xml(struct mg_connection *c, int status, const char *body, size_t len)
{
    mg_http_printf_head(c, status,
        "Content-Type: application/xml; charset=utf-8\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n\r\n", len);
    mg_write(c, body, len);
}

static void err_json(struct mg_connection *c, int status, const char *code, const char *msg)
{
    char buf[512];
    int n = snprintf(buf, sizeof(buf),
        "{\"error\":{\"code\":\"%s\",\"message\":\"%s\"}}", code, msg);
    send_json(c, status, buf, (size_t)n);
}

static void err_xml(struct mg_connection *c, int status, const char *msg)
{
    char buf[512];
    int n = snprintf(buf, sizeof(buf),
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
        "<rtmp><error>%s</error></rtmp>\n", msg);
    send_xml(c, status, buf, (size_t)n);
}

static int query_var(const struct mg_http_message *hm, const char *name, char *out, size_t outlen)
{
    char tmp[512];
    int n = mg_http_get_var(&hm->query, name, tmp, sizeof(tmp));
    if (n > 0) {
        size_t cpy = (size_t)n < outlen - 1 ? (size_t)n : outlen - 1;
        memcpy(out, tmp, cpy);
        out[cpy] = '\0';
        return (int)cpy;
    }
    out[0] = '\0';
    return 0;
}

/* ---------- auth ---------- */

static bool bearer_ok(struct http_server *http, const struct mg_http_message *hm)
{
    if (!http->config.api_token[0]) return true;
    const struct mg_str *hdr = mg_http_get_header(hm, "Authorization");
    if (!hdr || hdr->len < 8 || strncmp(hdr->ptr, "Bearer ", 7) != 0)
        return false;
    char tok[256];
    size_t tlen = hdr->len - 7;
    if (tlen >= sizeof(tok)) tlen = sizeof(tok) - 1;
    memcpy(tok, hdr->ptr + 7, tlen);
    tok[tlen] = '\0';
    for (int i = (int)tlen - 1; i >= 0; i--) {
        if (tok[i] == '\r' || tok[i] == '\n' || tok[i] == ' ') tok[i] = '\0';
        else break;
    }
    return strcmp(tok, http->config.api_token) == 0;
}

static bool stats_key_ok(struct http_server *http, const char *key, const char *stream_id)
{
    if (!key || !key[0]) return false;
    db_stream_t s;
    if (!db_stream_find_by_stats_key(http->db, key, &s)) return false;
    if (stream_id && strcmp(s.id, stream_id) != 0) return false;
    return true;
}

/* ---------- JSON stats builder ---------- */

static void build_json_stats(struct http_server *http, const char *stream_id,
                              char **out, size_t *outlen)
{
    size_t cap = 65536;
    char *buf = malloc(cap);
    int off = 0;

    db_publisher_t *pubs = NULL; int pub_cnt = 0;
    db_player_t *players = NULL; int player_cnt = 0;

    if (stream_id) {
        db_publisher_list(http->db, stream_id, &pubs, &pub_cnt);
        db_player_list(http->db, stream_id, &players, &player_cnt);
    } else {
        db_publisher_list_all(http->db, &pubs, &pub_cnt);
        db_player_list_all(http->db, &players, &player_cnt);
    }

    off += snprintf(buf + off, cap - (size_t)off, "{\"streams\":[");

    for (int i = 0; i < pub_cnt; i++) {
        db_publisher_t *p = &pubs[i];
        time_t now = time(NULL);
        int uptime_s = (int)(now - p->connected_at);

        off += snprintf(buf + off, cap - (size_t)off,
            "%s{"
            "\"id\":\"%s\","
            "\"name\":\"%s\","
            "\"app\":\"%s\","
            "\"uptime\":%d,"
            "\"bitrate_kbps\":%.1f,"
            "\"bytes_in\":%llu,"
            "\"video\":{\"codec\":\"%s\",\"width\":%u,\"height\":%u,\"fps\":%.1f},"
            "\"audio\":{\"codec\":\"%s\"},"
            "\"client\":{\"address\":\"%s\",\"publisher\":true}"
            "}",
            i > 0 ? "," : "",
            p->stream_name, p->stream_name, p->app,
            uptime_s, p->bitrate_kbps,
            (unsigned long long)p->bytes_in,
            p->video_codec, p->video_width, p->video_height, p->fps,
            p->audio_codec,
            p->remote_addr);
    }

    /* Players grouped by stream */
    off += snprintf(buf + off, cap - (size_t)off, "],\"players\":[");
    for (int i = 0; i < player_cnt; i++) {
        db_player_t *pl = &players[i];
        time_t now = time(NULL);
        int uptime_s = (int)(now - pl->connected_at);

        off += snprintf(buf + off, cap - (size_t)off,
            "%s{"
            "\"id\":\"%s\","
            "\"stream_name\":\"%s\","
            "\"app\":\"%s\","
            "\"uptime\":%d,"
            "\"bitrate_kbps\":%.1f,"
            "\"bytes_out\":%llu,"
            "\"client\":{\"address\":\"%s\"}"
            "}",
            i > 0 ? "," : "",
            pl->id, pl->stream_name, pl->app,
            uptime_s, pl->bitrate_kbps,
            (unsigned long long)pl->bytes_out,
            pl->remote_addr);
    }

    off += snprintf(buf + off, cap - (size_t)off,
        "],\"summary\":{\"publishers\":%d,\"players\":%d,\"total_clients\":%d}}",
        pub_cnt, player_cnt, pub_cnt + player_cnt);

    db_publisher_free_list(pubs);
    db_player_free_list(players);

    *out = buf;
    *outlen = (size_t)off;
}

/* ---------- XML stats (nginx-rtmp compatible) ---------- */

static void build_nginx_xml(struct http_server *http, const char *stream_id,
                             char **out, size_t *outlen)
{
    size_t cap = 65536;
    char *buf = malloc(cap);
    int off = 0;

    db_publisher_t *pubs = NULL; int pub_cnt = 0;
    db_player_t *players = NULL; int player_cnt = 0;

    if (stream_id) {
        db_publisher_list(http->db, stream_id, &pubs, &pub_cnt);
        db_player_list(http->db, stream_id, &players, &player_cnt);
    } else {
        db_publisher_list_all(http->db, &pubs, &pub_cnt);
        db_player_list_all(http->db, &players, &player_cnt);
    }

    off += snprintf(buf + off, cap - (size_t)off,
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<rtmp>\n  <server>\n"
        "    <application>\n      <name>live</name>\n      <live>\n");

    for (int i = 0; i < pub_cnt; i++) {
        db_publisher_t *p = &pubs[i];
        time_t now = time(NULL);
        int uptime_ms = (int)(now - p->connected_at) * 1000;
        int bw_in = (int)(p->bitrate_kbps * 1000);

        off += snprintf(buf + off, cap - (size_t)off,
            "        <stream>\n"
            "          <name>%s</name>\n"
            "          <time>%d</time>\n"
            "          <bw_in>%d</bw_in>\n"
            "          <bytes_in>%llu</bytes_in>\n"
            "          <bw_out>0</bw_out>\n"
            "          <bytes_out>0</bytes_out>\n",
            p->stream_name, uptime_ms, bw_in,
            (unsigned long long)p->bytes_in);

        if (p->video_codec[0]) {
            off += snprintf(buf + off, cap - (size_t)off,
                "          <video>\n"
                "            <width>%u</width>\n"
                "            <height>%u</height>\n"
                "            <frame_rate>%.1f</frame_rate>\n"
                "            <codec>%s</codec>\n"
                "            <profile>baseline</profile>\n"
                "            <level>3.1</level>\n"
                "          </video>\n",
                p->video_width, p->video_height, p->fps, p->video_codec);
        }

        if (p->audio_codec[0]) {
            off += snprintf(buf + off, cap - (size_t)off,
                "          <audio>\n"
                "            <codec>%s</codec>\n"
                "            <sample_rate>44100</sample_rate>\n"
                "            <channels>2</channels>\n"
                "          </audio>\n",
                p->audio_codec);
        }

        off += snprintf(buf + off, cap - (size_t)off,
            "          <client>\n"
            "            <address>%s</address>\n"
            "            <time>%d</time>\n"
            "            <flashver>FMLE/3.0</flashver>\n"
            "            <dropped>0</dropped>\n"
            "            <avsync>0</avsync>\n"
            "            <timestamp>%d</timestamp>\n"
            "            <active>1</active>\n"
            "            <publisher>1</publisher>\n"
            "          </client>\n"
            "        </stream>\n",
            p->remote_addr, uptime_ms, uptime_ms);
    }

    for (int i = 0; i < player_cnt; i++) {
        db_player_t *pl = &players[i];
        time_t now = time(NULL);
        int uptime_ms = (int)(now - pl->connected_at) * 1000;

        off += snprintf(buf + off, cap - (size_t)off,
            "        <stream>\n"
            "          <name>%s</name>\n"
            "          <time>%d</time>\n"
            "          <bw_in>0</bw_in>\n"
            "          <bytes_in>0</bytes_in>\n"
            "          <bw_out>%d</bw_out>\n"
            "          <bytes_out>%llu</bytes_out>\n"
            "          <client>\n"
            "            <address>%s</address>\n"
            "            <time>%d</time>\n"
            "            <flashver>FMLE/3.0</flashver>\n"
            "            <dropped>0</dropped>\n"
            "            <avsync>0</avsync>\n"
            "            <timestamp>%d</timestamp>\n"
            "            <active>1</active>\n"
            "            <publisher>0</publisher>\n"
            "          </client>\n"
            "        </stream>\n",
            pl->stream_name, uptime_ms,
            (int)(pl->bitrate_kbps * 1000),
            (unsigned long long)pl->bytes_out,
            pl->remote_addr, uptime_ms, uptime_ms);
    }

    off += snprintf(buf + off, cap - (size_t)off,
        "        <nclients>%d</nclients>\n"
        "      </live>\n    </application>\n  </server>\n</rtmp>\n",
        pub_cnt + player_cnt);

    db_publisher_free_list(pubs);
    db_player_free_list(players);

    *out = buf;
    *outlen = (size_t)off;
}

/* ---------- handlers ---------- */

static void handle_health(struct mg_connection *c, struct http_server *http)
{
    (void)http;
    char buf[256];
    int n = snprintf(buf, sizeof(buf),
        "{\"status\":\"ok\",\"timestamp\":%ld}", (long)time(NULL));
    send_json(c, 200, buf, (size_t)n);
}

/* /stats?key=***  → JSON */
static void handle_stats_json(struct mg_connection *c, struct http_server *http,
                               const struct mg_http_message *hm)
{
    char key[256];
    query_var(hm, "key", key, sizeof(key));
    if (!key[0]) { err_json(c, 401, "MISSING_KEY", "stats_key required"); return; }

    db_stream_t s;
    if (!db_stream_find_by_stats_key(http->db, key, &s)) {
        err_json(c, 403, "INVALID_KEY", "Invalid stats key");
        return;
    }

    char *json = NULL; size_t jlen = 0;
    build_json_stats(http, s.id, &json, &jlen);
    send_json(c, 200, json, jlen);
    free(json);
}

/* /stats-nginx?key=***  → XML */
static void handle_stats_nginx(struct mg_connection *c, struct http_server *http,
                                const struct mg_http_message *hm)
{
    char key[256];
    query_var(hm, "key", key, sizeof(key));
    if (!key[0]) { err_xml(c, 401, "Missing stats key"); return; }

    db_stream_t s;
    if (!db_stream_find_by_stats_key(http->db, key, &s)) {
        err_xml(c, 403, "Invalid stats key");
        return;
    }

    char *xml = NULL; size_t xlen = 0;
    build_nginx_xml(http, s.id, &xml, &xlen);
    send_xml(c, 200, xml, xlen);
    free(xml);
}

/* GET /api/v1/streams */
static void handle_streams_list(struct mg_connection *c, struct http_server *http,
                                 const struct mg_http_message *hm)
{
    if (!bearer_ok(http, hm)) { err_json(c, 401, "UNAUTHORIZED", "Missing or invalid token"); return; }

    db_stream_t *streams = NULL; int count = 0;
    db_stream_list(http->db, &streams, &count);

    size_t cap = 65536;
    char *buf = malloc(cap);
    int off = snprintf(buf, cap, "[");
    for (int i = 0; i < count; i++) {
        /* Never expose keys in list view */
        off += snprintf(buf + off, cap - (size_t)off,
            "%s{\"id\":\"%s\",\"name\":\"%s\",\"app\":\"%s\",\"enabled\":%s,\"created_at\":%ld}",
            i > 0 ? "," : "",
            streams[i].id, streams[i].name, streams[i].app,
            streams[i].enabled ? "true" : "false",
            (long)streams[i].created_at);
    }
    off += snprintf(buf + off, cap - (size_t)off, "]");

    db_stream_free_list(streams);
    send_json(c, 200, buf, (size_t)off);
    free(buf);
}

/* POST /api/v1/streams  → creates stream, returns keys */
static void handle_stream_create(struct mg_connection *c, struct http_server *http,
                                  const struct mg_http_message *hm)
{
    if (!bearer_ok(http, hm)) { err_json(c, 401, "UNAUTHORIZED", "Missing or invalid token"); return; }

    char body[4096];
    size_t blen = hm->body.len < sizeof(body) - 1 ? hm->body.len : sizeof(body) - 1;
    memcpy(body, hm->body.ptr, blen);
    body[blen] = '\0';

    db_stream_t s;
    memset(&s, 0, sizeof(s));

    /* minimal JSON extract */
    #define EXTRACT(key, dst, dstlen) do { \
        char nk[128]; snprintf(nk, sizeof(nk), "\"%s\"", key); \
        const char *_p = strstr(body, nk); \
        if (_p) { _p += strlen(nk); while (*_p == ' ' || *_p == '\t' || *_p == ':') _p++; \
        if (*_p == '"') { _p++; size_t _i = 0; while (*_p && *_p != '"' && _i < (dstlen)-1) (dst)[_i++] = *_p++; (dst)[_i] = '\0'; } \
        else (dst)[0] = '\0'; } else (dst)[0] = '\0'; } while(0)

    EXTRACT("id", s.id, sizeof(s.id));
    EXTRACT("name", s.name, sizeof(s.name));
    EXTRACT("app", s.app, sizeof(s.app));
    EXTRACT("allowed_codecs", s.allowed_codecs, sizeof(s.allowed_codecs));
    #undef EXTRACT

    if (!s.id[0]) { err_json(c, 400, "BAD_REQUEST", "Missing 'id' field"); return; }
    if (!s.app[0]) strncpy(s.app, "live", sizeof(s.app) - 1);
    if (!s.name[0]) strncpy(s.name, s.id, sizeof(s.name) - 1);
    if (!s.allowed_codecs[0]) strncpy(s.allowed_codecs, "avc1,hvc1,av01", sizeof(s.allowed_codecs) - 1);

    /* Generate unique keys */
    long t = (long)time(NULL);
    snprintf(s.publish_key, sizeof(s.publish_key), "pub_%s_%ld", s.id, t);
    snprintf(s.play_key, sizeof(s.play_key), "pl_%s_%ld", s.id, t + 1);
    snprintf(s.stats_key, sizeof(s.stats_key), "st_%s_%ld", s.id, t + 2);
    s.enabled = true;
    s.created_at = time(NULL);

    if (!db_stream_add(http->db, &s)) {
        err_json(c, 409, "CONFLICT", "Stream ID already exists");
        return;
    }

    log_info("Stream created: id=%s app=%s", s.id, s.app);

    char resp[1024];
    int n = snprintf(resp, sizeof(resp),
        "{\"id\":\"%s\",\"name\":\"%s\",\"app\":\"%s\","
        "\"publish_key\":\"%s\",\"play_key\":\"%s\",\"stats_key\":\"%s\","
        "\"enabled\":true,\"created_at\":%ld}",
        s.id, s.name, s.app,
        s.publish_key, s.play_key, s.stats_key,
        (long)s.created_at);
    send_json(c, 201, resp, (size_t)n);
}

/* DELETE /api/v1/streams/:id */
static void handle_stream_delete(struct mg_connection *c, struct http_server *http,
                                  const struct mg_http_message *hm, const char *id)
{
    if (!bearer_ok(http, hm)) { err_json(c, 401, "UNAUTHORIZED", "Missing or invalid token"); return; }

    db_stream_t s;
    if (!db_stream_get(http->db, id, &s)) {
        err_json(c, 404, "NOT_FOUND", "Stream not found");
        return;
    }
    db_stream_delete(http->db, id);
    log_info("Stream deleted: %s", id);
    send_json(c, 200, "{\"status\":\"deleted\"}", 20);
}

/* GET /api/v1/streams/:id/stats?key=***  → JSON per-stream */
static void handle_stream_stats(struct mg_connection *c, struct http_server *http,
                                 const struct mg_http_message *hm, const char *id)
{
    char key[256];
    query_var(hm, "key", key, sizeof(key));
    if (!stats_key_ok(http, key, id)) {
        err_json(c, 403, "FORBIDDEN", "Invalid stats key");
        return;
    }

    db_stream_t s;
    if (!db_stream_get(http->db, id, &s)) {
        err_json(c, 404, "NOT_FOUND", "Stream not found");
        return;
    }

    char *json = NULL; size_t jlen = 0;
    build_json_stats(http, id, &json, &jlen);
    send_json(c, 200, json, jlen);
    free(json);
}

/* ---------- mongoose event handler ---------- */

static void http_handler(struct mg_connection *c, int ev, void *ev_data, void *fn_data)
{
    if (ev != MG_EV_HTTP_MSG) return;

    struct http_server *http = (struct http_server *)fn_data;
    struct mg_http_message *hm = (struct mg_http_message *)ev_data;
    struct mg_str uri = hm->uri;

    /* Health — no auth */
    if (mg_http_match_uri(hm, "/api/v1/health")) {
        handle_health(c, http);
        return;
    }

    /* /stats?key=*** → JSON */
    if (mg_http_match_uri(hm, "/stats")) {
        handle_stats_json(c, http, hm);
        return;
    }

    /* /stats-nginx?key=*** → XML */
    if (mg_http_match_uri(hm, "/stats-nginx")) {
        handle_stats_nginx(c, http, hm);
        return;
    }

    /* /api/v1/streams */
    if (mg_http_match_uri(hm, "/api/v1/streams")) {
        if (hm->body.len == 0)
            handle_streams_list(c, http, hm);
        else
            handle_stream_create(c, http, hm);
        return;
    }

    /* /api/v1/streams/*  (DELETE or /stats) */
    if (mg_http_match_uri(hm, "/api/v1/streams/*")) {
        /* Check for /stats sub-path */
        const char *after = strstr(uri.ptr, "/api/v1/streams/");
        if (after) {
            after += strlen("/api/v1/streams/");
            const char *slash = strchr(after, '/');
            if (slash && strncmp(slash, "/stats", 6) == 0) {
                char sid[64] = {0};
                size_t len = (size_t)(slash - after);
                if (len < sizeof(sid) - 1) {
                    memcpy(sid, after, len);
                    sid[len] = '\0';
                    handle_stream_stats(c, http, hm, sid);
                    return;
                }
            }

            /* DELETE /api/v1/streams/:id */
            char sid[64] = {0};
            strncpy(sid, after, sizeof(sid) - 1);
            for (int i = 0; sid[i]; i++) {
                if (sid[i] == '/' || sid[i] == '?') { sid[i] = '\0'; break; }
            }
            if (sid[0]) {
                handle_stream_delete(c, http, hm, sid);
                return;
            }
        }
    }

    err_json(c, 404, "NOT_FOUND", "Unknown endpoint");
}

/* ---------- public API ---------- */

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
    if (!mg_http_listen(&http->mgr, http->listen_addr, http_handler, http)) {
        log_error("Failed to start HTTP on %s", http->config.http_bind);
        return -1;
    }
    log_info("HTTP listening on %s", http->config.http_bind);
    return 0;
}

void http_server_stop(http_server_t *http) { http->running = 0; }
void http_server_poll(http_server_t *http, int ms) { mg_mgr_poll(&http->mgr, ms); }
void http_server_set_db(http_server_t *http, void *db) { http->db = (db_context_t *)db; }
