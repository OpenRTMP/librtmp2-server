/**
 * http.c — HTTP server using Mongoose
 *
 * Provides:
 * - REST API on /api/v1/* (JSON, auth via Bearer token)
 * - /stats — Nginx-RTMP-compatible XML stats (key-protected)
 * - /stats-nginx — alias for /stats (identical output)
 * - /api/v1/streams/:id/stats — per-stream stats (key-protected)
 *
 * All stats endpoints require the stream's unique stats_key as query param:
 *   /stats?key=<stats_key>
 *   /stream/:id/stats?key=<stats_key>
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

/* --- helpers --- */

static void http_send_json(struct mg_connection *c, int status, const char *body, size_t len)
{
    mg_http_printf_head(c, status,
        "Content-Type: application/json; charset=utf-8\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n\r\n", len);
    mg_write(c, body, len);
}

static void http_send_xml(struct mg_connection *c, int status, const char *body, size_t len)
{
    mg_http_printf_head(c, status,
        "Content-Type: application/xml; charset=utf-8\r\n"
        "Content-Length: %zu\r\n"
        "Connection: close\r\n\r\n", len);
    mg_write(c, body, len);
}

static void http_send_error_json(struct mg_connection *c, int status, const char *code, const char *msg)
{
    char buf[512];
    int len = snprintf(buf, sizeof(buf),
        "{\"error\":{\"code\":\"%s\",\"message\":\"%s\"}}", code, msg);
    http_send_json(c, status, buf, (size_t)len);
}

static void http_send_error_xml(struct mg_connection *c, int status, const char *msg)
{
    char buf[512];
    int len = snprintf(buf, sizeof(buf),
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n"
        "<rtmp><error>%s</error></rtmp>\n", msg);
    http_send_xml(c, status, buf, (size_t)len);
}

/* Extract query parameter from Mongoose HTTP message */
static int get_query_var(const struct mg_http_message *hm, const char *name, char *out, size_t outlen)
{
    char buf[512];
    int len = mg_http_get_var(&hm->query, name, buf, sizeof(buf));
    if (len > 0) {
        size_t copy = (size_t)len < outlen - 1 ? (size_t)len : outlen - 1;
        memcpy(out, buf, copy);
        out[copy] = '\0';
        return (int)copy;
    }
    out[0] = '\0';
    return 0;
}

/* Check Bearer token for API endpoints */
static bool check_api_auth(struct http_server *http, struct mg_http_message *hm)
{
    if (!http->config.api_token[0]) return true;
    struct mg_str *auth = mg_http_get_header(hm, "Authorization");
    if (!auth || auth->len < 8 || strncmp(auth->ptr, "Bearer ", 7) != 0)
        return false;
    char token[256];
    size_t tlen = auth->len - 7;
    if (tlen >= sizeof(token)) tlen = sizeof(token) - 1;
    memcpy(token, auth->ptr + 7, tlen);
    token[tlen] = '\0';
    for (int i = (int)tlen - 1; i >= 0; i--) {
        if (token[i] == '\r' || token[i] == '\n' || token[i] == ' ')
            token[i] = '\0';
        else break;
    }
    return strcmp(token, http->config.api_token) == 0;
}

/* Validate stats_key query param against a stream */
static bool check_stats_key(struct http_server *http, const char *key, const char *stream_id)
{
    if (!key || !key[0]) return false;
    db_stream_t s;
    if (db_stream_find_by_stats_key(http->db, key, &s)) {
        if (stream_id && strcmp(s.id, stream_id) != 0) return false;
        return true;
    }
    return false;
}

/* ==================== Nginx-RTMP-compatible XML ==================== */

static void generate_nginx_xml(struct http_server *http, const char *stream_id,
                                char **out_buf, size_t *out_len)
{
    /* Nginx-rtmp stat format:
     * <?xml version="1.0" encoding="utf-8"?>
     * <rtmp>
     *   <server>
     *     <application>
     *       <name>live</name>
     *       <live>
     *         <stream>
     *           <name>stream_name</name>
     *           <time>12345</time>          <!-- ms since start -->
     *           <bw_in>123456</bw_in>        <!-- bytes/sec -->
     *           <bytes_in>1234567</bytes_in>
     *           <bw_out>123456</bw_out>
     *           <bytes_out>1234567</bytes_out>
     *           <video>
     *             <width>1920</width>
     *             <height>1080</height>
     *             <frame_rate>30</frame_rate>
     *             <codec>h264</codec>
     *             <profile>High</profile>
     *             <compat>0</compat>
     *             <level>4.1</level>
     *           </video>
     *           <audio>
     *             <codec>aac</codec>
     *             <sample_rate>44100</sample_rate>
     *             <channels>2</channels>
     *           </audio>
     *           <client>
     *             <address>1.2.3.4</address>
     *             <time>12345</time>
     *             <flashver>FMLE/3.0</flashver>
     *             <dropped>0</dropped>
     *             <avsync>0</avsync>
     *             <timestamp>12345</timestamp>
     *             <active>1</active>
     *           </client>
     *           <meta>...</meta>
     *         </stream>
     *         <nclients>5</nclients>
     *       </live>
     *     </application>
     *   </server>
     * </rtmp>
     */

    size_t cap = 65536;
    char *buf = malloc(cap);
    int off = 0;

    off += snprintf(buf + off, cap - (size_t)off,
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<rtmp>\n  <server>\n");

    /* Get all active publishers */
    db_publisher_t *pubs = NULL;
    int pub_count = 0;
    if (stream_id)
        db_publisher_list(http->db, stream_id, &pubs, &pub_count);
    else
        db_publisher_list_all(http->db, &pubs, &pub_count);

    /* Get all active players */
    db_player_t *players = NULL;
    int player_count = 0;
    if (stream_id)
        db_player_list(http->db, stream_id, &players, &player_count);
    else
        db_player_list_all(http->db, &players, &player_count);

    /* Group by app — for simplicity, use the publisher's app */
    /* For now, output all publishers under "live" app */
    off += snprintf(buf + off, cap - (size_t)off,
        "    <application>\n      <name>live</name>\n      <live>\n");

    for (int i = 0; i < pub_count; i++) {
        db_publisher_t *p = &pubs[i];
        time_t now = time(NULL);
        int uptime_ms = (int)(now - p->connected_at) * 1000;
        int bw_in = (int)(p->bitrate_kbps * 1000); /* bps */

        off += snprintf(buf + off, cap - (size_t)off,
            "        <stream>\n"
            "          <name>%s</name>\n"
            "          <time>%d</time>\n"
            "          <bw_in>%d</bw_in>\n"
            "          <bytes_in>%llu</bytes_in>\n"
            "          <bw_out>0</bw_out>\n"
            "          <bytes_out>0</bytes_out>\n",
            p->stream_name,
            uptime_ms,
            bw_in,
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

        /* Publisher as client */
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
            "          </client>\n",
            p->remote_addr, uptime_ms, uptime_ms);

        off += snprintf(buf + off, cap - (size_t)off, "        </stream>\n");
    }

    /* Players as clients under the first matching stream, or standalone */
    for (int i = 0; i < player_count; i++) {
        db_player_t *pl = &players[i];
        time_t now = time(NULL);
        int uptime_ms = (int)(now - pl->connected_at) * 1000;

        /* Find matching publisher to nest under */
        db_publisher_t *matched_pub = NULL;
        for (int j = 0; j < pub_count; j++) {
            if (strcmp(pubs[j].stream_id, pl->stream_id) == 0) {
                matched_pub = &pubs[j];
                break;
            }
        }

        if (matched_pub) {
            /* Already nested — add as additional client */
            /* For simplicity, add as separate stream entry with player client */
        }

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
            pl->stream_name,
            uptime_ms,
            (int)(pl->bitrate_kbps * 1000),
            (unsigned long long)pl->bytes_out,
            pl->remote_addr,
            uptime_ms,
            uptime_ms);
    }

    off += snprintf(buf + off, cap - (size_t)off,
        "        <nclients>%d</nclients>\n"
        "      </live>\n"
        "    </application>\n"
        "  </server>\n"
        "</rtmp>\n",
        pub_count + player_count);

    db_publisher_free_list(pubs);
    db_player_free_list(players);

    *out_buf = buf;
    *out_len = (size_t)off;
}

/* ==================== API Handlers ==================== */

static void handle_health(struct mg_connection *c, struct http_server *http)
{
    (void)http;
    time_t now = time(NULL);
    char buf[256];
    int len = snprintf(buf, sizeof(buf),
        "{\"status\":\"ok\",\"timestamp\":%ld}", (long)now);
    http_send_json(c, 200, buf, (size_t)len);
}

static void handle_stats_xml(struct mg_connection *c, struct http_server *http,
                              struct mg_http_message *hm)
{
    char key[256];
    get_query_var(hm, "key", key, sizeof(key));

    if (!key[0]) {
        http_send_error_xml(c, 401, "Missing stats key");
        return;
    }

    db_stream_t s;
    if (!db_stream_find_by_stats_key(http->db, key, &s)) {
        http_send_error_xml(c, 403, "Invalid stats key");
        return;
    }

    char *xml = NULL;
    size_t xml_len = 0;
    generate_nginx_xml(http, s.id, &xml, &xml_len);
    http_send_xml(c, 200, xml, xml_len);
    free(xml);
}

static void handle_api_streams_list(struct mg_connection *c, struct http_server *http)
{
    if (!check_api_auth(http, NULL)) {
        /* Try to read auth from query for convenience */
        if (!check_api_auth(http, (struct mg_http_message *)c)) {
            http_send_error_json(c, 401, "UNAUTHORIZED", "Missing or invalid token");
            return;
        }
    }

    db_stream_t *streams = NULL;
    int count = 0;
    db_stream_list(http->db, &streams, &count);

    size_t cap = 65536;
    char *buf = malloc(cap);
    int off = 0;
    off += snprintf(buf + off, cap - (size_t)off, "[");
    for (int i = 0; i < count; i++) {
        db_stream_t *s = &streams[i];
        /* Don't expose keys in list view */
        off += snprintf(buf + off, cap - (size_t)off,
            "%s{\"id\":\"%s\",\"name\":\"%s\",\"app\":\"%s\",\"enabled\":%s,\"created_at\":%ld}",
            i > 0 ? "," : "",
            s->id, s->name, s->app,
            s->enabled ? "true" : "false",
            (long)s->created_at);
    }
    off += snprintf(buf + off, cap - (size_t)off, "]");

    db_stream_free_list(streams);
    http_send_json(c, 200, buf, (size_t)off);
    free(buf);
}

static void handle_api_stream_create(struct mg_connection *c, struct http_server *http,
                                      struct mg_http_message *hm)
{
    if (!check_api_auth(http, hm)) {
        http_send_error_json(c, 401, "UNAUTHORIZED", "Missing or invalid token");
        return;
    }

    char body[4096];
    size_t body_len = hm->body.len < sizeof(body) - 1 ? hm->body.len : sizeof(body) - 1;
    memcpy(body, hm->body.ptr, body_len);
    body[body_len] = '\0';

    /* Minimal JSON parse — extract fields */
    db_stream_t s;
    memset(&s, 0, sizeof(s));

    /* Helper to extract string from JSON */
    const char *extract_str(const char *json, const char *key, char *out, size_t outlen) {
        char needle[128];
        snprintf(needle, sizeof(needle), "\"%s\"", key);
        const char *p = strstr(json, needle);
        if (!p) { out[0] = '\0'; return NULL; }
        p += strlen(needle);
        while (*p == ' ' || *p == '\t' || *p == ':') p++;
        if (*p != '"') { out[0] = '\0'; return NULL; }
        p++;
        size_t i = 0;
        while (*p && *p != '"' && i < outlen - 1) out[i++] = *p++;
        out[i] = '\0';
        return out;
    }

    extract_str(body, "id", s.id, sizeof(s.id));
    extract_str(body, "name", s.name, sizeof(s.name));
    extract_str(body, "app", s.app, sizeof(s.app));

    if (!s.id[0]) {
        http_send_error_json(c, 400, "BAD_REQUEST", "Missing 'id' field");
        return;
    }
    if (!s.app[0]) strncpy(s.app, "live", sizeof(s.app) - 1);
    if (!s.name[0]) strncpy(s.name, s.id, sizeof(s.name) - 1);

    /* Generate unique keys */
    snprintf(s.publish_key, sizeof(s.publish_key), "pub_%s_%ld", s.id, (long)time(NULL));
    snprintf(s.play_key, sizeof(s.play_key), "pl_%s_%ld", s.id, (long)time(NULL) + 1);
    snprintf(s.stats_key, sizeof(s.stats_key), "st_%s_%ld", s.id, (long)time(NULL) + 2);
    s.enabled = true;
    strncpy(s.allowed_codecs, "avc1,hvc1,av01", sizeof(s.allowed_codecs) - 1);
    s.created_at = time(NULL);

    if (!db_stream_add(http->db, &s)) {
        http_send_error_json(c, 409, "CONFLICT", "Stream ID already exists");
        return;
    }

    log_info("Stream created via API: id=%s app=%s", s.id, s.app);

    /* Return stream with keys */
    char resp[1024];
    int len = snprintf(resp, sizeof(resp),
        "{\"id\":\"%s\",\"name\":\"%s\",\"app\":\"%s\","
        "\"publish_key\":\"%s\",\"play_key\":\"%s\",\"stats_key\":\"%s\","
        "\"enabled\":true,\"created_at\":%ld}",
        s.id, s.name, s.app,
        s.publish_key, s.play_key, s.stats_key,
        (long)s.created_at);
    http_send_json(c, 201, resp, (size_t)len);
}

static void handle_api_stream_delete(struct mg_connection *c, struct http_server *http,
                                      struct mg_http_message *hm, const char *id)
{
    if (!check_api_auth(http, hm)) {
        http_send_error_json(c, 401, "UNAUTHORIZED", "Missing or invalid token");
        return;
    }

    db_stream_t s;
    if (!db_stream_get(http->db, id, &s)) {
        http_send_error_json(c, 404, "NOT_FOUND", "Stream not found");
        return;
    }

    db_stream_delete(http->db, id);
    log_info("Stream deleted via API: %s", id);

    http_send_json(c, 200, "{\"status\":\"deleted\"}", 20);
}

static void handle_api_stream_stats(struct mg_connection *c, struct http_server *http,
                                     struct mg_http_message *hm, const char *id)
{
    char key[256];
    get_query_var(hm, "key", key, sizeof(key));

    if (!check_stats_key(http, key, id)) {
        http_send_error_json(c, 403, "FORBIDDEN", "Invalid stats key");
        return;
    }

    db_stream_t s;
    if (!db_stream_get(http->db, id, &s)) {
        http_send_error_json(c, 404, "NOT_FOUND", "Stream not found");
        return;
    }

    /* Get publishers */
    db_publisher_t *pubs = NULL;
    int pub_count = 0;
    db_publisher_list(http->db, id, &pubs, &pub_count);

    /* Get players */
    db_player_t *players = NULL;
    int player_count = 0;
    db_player_list(http->db, id, &players, &player_count);

    /* Build JSON response */
    size_t cap = 65536;
    char *buf = malloc(cap);
    int off = 0;

    off += snprintf(buf + off, cap - (size_t)off,
        "{\"stream\":{\"id\":\"%s\",\"name\":\"%s\",\"app\":\"%s\"},", s.id, s.name, s.app);

    /* Publishers */
    off += snprintf(buf + off, cap - (size_t)off, "\"publishers\":[");
    for (int i = 0; i < pub_count; i++) {
        db_publisher_t *p = &pubs[i];
        off += snprintf(buf + off, cap - (size_t)off,
            "%s{\"id\":\"%s\",\"remote_addr\":\"%s\",\"stream_name\":\"%s\","
            "\"video_codec\":\"%s\",\"audio_codec\":\"%s\","
            "\"video_width\":%u,\"video_height\":%u,\"fps\":%.1f,"
            "\"bytes_in\":%llu,\"bitrate_kbps\":%.1f,\"connected_at\":%ld}",
            i > 0 ? "," : "",
            p->id, p->remote_addr, p->stream_name,
            p->video_codec, p->audio_codec,
            p->video_width, p->video_height, p->fps,
            (unsigned long long)p->bytes_in, p->bitrate_kbps,
            (long)p->connected_at);
    }
    off += snprintf(buf + off, cap - (size_t)off, "],");

    /* Players */
    off += snprintf(buf + off, cap - (size_t)off, "\"players\":[");
    for (int i = 0; i < player_count; i++) {
        db_player_t *pl = &players[i];
        off += snprintf(buf + off, cap - (size_t)off,
            "%s{\"id\":\"%s\",\"remote_addr\":\"%s\",\"stream_name\":\"%s\","
            "\"bytes_out\":%llu,\"bitrate_kbps\":%.1f,\"connected_at\":%ld}",
            i > 0 ? "," : "",
            pl->id, pl->remote_addr, pl->stream_name,
            (unsigned long long)pl->bytes_out, pl->bitrate_kbps,
            (long)pl->connected_at);
    }
    off += snprintf(buf + off, cap - (size_t)off, "]}");

    db_publisher_free_list(pubs);
    db_player_free_list(players);

    http_send_json(c, 200, buf, (size_t)off);
    free(buf);
}

/* ==================== Mongoose event handler ==================== */

static void http_handler(struct mg_connection *c, int ev, void *ev_data, void *fn_data)
{
    if (ev != MG_EV_HTTP_MSG) return;

    struct http_server *http = (struct http_server *)fn_data;
    struct mg_http_message *hm = (struct mg_http_message *)ev_data;
    struct mg_str uri = hm->uri;

    /* Health check — no auth */
    if (mg_http_match_uri(hm, "/api/v1/health")) {
        handle_health(c, http);
        return;
    }

    /* Nginx-RTMP-compatible stats XML — key-protected via query param */
    if (mg_http_match_uri(hm, "/stats") || mg_http_match_uri(hm, "/stats-nginx")) {
        handle_stats_xml(c, http, hm);
        return;
    }

    /* API: GET /api/v1/streams */
    if (mg_http_match_uri(hm, "/api/v1/streams") && hm->body.len == 0) {
        handle_api_streams_list(c, http);
        return;
    }

    /* API: POST /api/v1/streams */
    if (mg_http_match_uri(hm, "/api/v1/streams") && hm->body.len > 0) {
        handle_api_stream_create(c, http, hm);
        return;
    }

    /* API: DELETE /api/v1/streams/:id — extract id from uri */
    if (mg_http_match_uri(hm, "/api/v1/streams/*")) {
        /* Check if it's a stats sub-path */
        if (strstr(uri.ptr, "/stats")) {
            /* Extract stream id from /api/v1/streams/:id/stats */
            char stream_id[64] = {0};
            const char *p = strstr(uri.ptr, "/api/v1/streams/");
            if (p) {
                p += strlen("/api/v1/streams/");
                const char *end = strstr(p, "/stats");
                if (end && end - p < (int)sizeof(stream_id) - 1) {
                    size_t len = (size_t)(end - p);
                    memcpy(stream_id, p, len);
                    stream_id[len] = '\0';
                }
            }
            if (stream_id[0]) {
                handle_api_stream_stats(c, http, hm, stream_id);
                return;
            }
        }

        /* DELETE /api/v1/streams/:id */
        char stream_id[64] = {0};
        const char *p = strstr(uri.ptr, "/api/v1/streams/");
        if (p) {
            p += strlen("/api/v1/streams/");
            strncpy(stream_id, p, sizeof(stream_id) - 1);
            /* Trim trailing slash or query */
            for (int i = 0; stream_id[i]; i++) {
                if (stream_id[i] == '/' || stream_id[i] == '?') { stream_id[i] = '\0'; break; }
            }
        }
        if (stream_id[0]) {
            handle_api_stream_delete(c, http, hm, stream_id);
            return;
        }
    }

    /* 404 */
    http_send_error_json(c, 404, "NOT_FOUND", "Unknown endpoint");
}

/* ==================== Public API ==================== */

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

void http_server_poll(http_server_t *http, int timeout_ms)
{
    mg_mgr_poll(&http->mgr, timeout_ms);
}

void http_server_set_db(http_server_t *http, void *db)
{
    http->db = (db_context_t *)db;
}
