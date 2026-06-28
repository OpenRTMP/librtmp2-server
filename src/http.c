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
#include <stdarg.h>
#include <time.h>

struct http_server {
    struct mg_mgr   mgr;
    server_config_t config;
    char            listen_addr[128];
    int             running;
    db_context_t   *db;
};

/* ---------- helpers ---------- */

/* mongoose 7.14 dropped mg_http_match_uri(); mg_match() is the replacement. */
static bool match_uri(const struct mg_http_message *hm, const char *glob)
{
    return mg_match(hm->uri, mg_str(glob), NULL);
}

static void send_json(struct mg_connection *c, int status, const char *body, size_t len)
{
    /* mg_http_reply() emits the status line and Content-Length automatically. */
    mg_http_reply(c, status,
        "Content-Type: application/json; charset=utf-8\r\n"
        "Connection: close\r\n",
        "%.*s", (int)len, body);
}

static void send_xml(struct mg_connection *c, int status, const char *body, size_t len)
{
    mg_http_reply(c, status,
        "Content-Type: application/xml; charset=utf-8\r\n"
        "Connection: close\r\n",
        "%.*s", (int)len, body);
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

/* ---------- dynamic string buffer (grows; never overflows) ----------
 *
 * The stats builders previously accumulated into a fixed 64 KB buffer with
 * `off += snprintf(buf + off, cap - off, ...)`. Once the cumulative output
 * exceeded the buffer, `cap - off` underflowed to a huge size_t and the next
 * snprintf wrote out of bounds (heap overflow). This grows on demand instead. */
typedef struct {
    char  *buf;
    size_t len;
    size_t cap;
} dynbuf_t;

static int dyn_init(dynbuf_t *d, size_t initial)
{
    d->buf = malloc(initial);
    if (!d->buf) { d->len = d->cap = 0; return -1; }
    d->len = 0;
    d->cap = initial;
    d->buf[0] = '\0';
    return 0;
}

/* Append printf-style, growing as needed. On OOM the buffer is left valid and
 * unchanged; callers detect failure via the final length / a NULL result. */
static void dyn_appendf(dynbuf_t *d, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    int need = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (need < 0) return;

    if (d->len + (size_t)need + 1 > d->cap) {
        size_t ncap = d->cap ? d->cap : 1024;
        while (ncap < d->len + (size_t)need + 1) ncap *= 2;
        char *nb = realloc(d->buf, ncap);
        if (!nb) return;
        d->buf = nb;
        d->cap = ncap;
    }

    va_start(ap, fmt);
    vsnprintf(d->buf + d->len, d->cap - d->len, fmt, ap);
    va_end(ap);
    d->len += (size_t)need;
}

/* ---------- output escaping (prevents JSON/XML injection) ----------
 *
 * Stream/app/codec/address strings are attacker- or operator-supplied (the
 * RTMP `app` is set by the publishing client). Emitting them raw lets a peer
 * inject structure into the stats JSON/XML. Both helpers always NUL-terminate
 * and truncate safely if `out` is too small. */
static void json_escape(const char *in, char *out, size_t outcap)
{
    size_t o = 0;
    if (outcap == 0) return;
    for (size_t i = 0; in && in[i] && o + 7 < outcap; i++) {
        unsigned char c = (unsigned char)in[i];
        switch (c) {
            case '"':  out[o++] = '\\'; out[o++] = '"';  break;
            case '\\': out[o++] = '\\'; out[o++] = '\\'; break;
            case '\n': out[o++] = '\\'; out[o++] = 'n';  break;
            case '\r': out[o++] = '\\'; out[o++] = 'r';  break;
            case '\t': out[o++] = '\\'; out[o++] = 't';  break;
            default:
                if (c < 0x20) o += (size_t)snprintf(out + o, outcap - o, "\\u%04x", c);
                else          out[o++] = (char)c;
        }
    }
    out[o] = '\0';
}

static void xml_escape(const char *in, char *out, size_t outcap)
{
    size_t o = 0;
    if (outcap == 0) return;
    for (size_t i = 0; in && in[i] && o + 7 < outcap; i++) {
        unsigned char c = (unsigned char)in[i];
        switch (c) {
            case '&':  memcpy(out + o, "&amp;", 5);  o += 5; break;
            case '<':  memcpy(out + o, "&lt;", 4);   o += 4; break;
            case '>':  memcpy(out + o, "&gt;", 4);   o += 4; break;
            case '"':  memcpy(out + o, "&quot;", 6); o += 6; break;
            case '\'': memcpy(out + o, "&apos;", 6); o += 6; break;
            default:
                /* Drop control chars XML 1.0 forbids; pass the rest through. */
                if (c >= 0x20 || c == '\t' || c == '\n' || c == '\r') out[o++] = (char)c;
        }
    }
    out[o] = '\0';
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

/* Length-padded, content-constant-time string equality, so token validation
 * does not leak the secret one byte at a time via response timing. (Length
 * equality may still be observable; the token's entropy is in its bytes.) */
static bool ct_str_eq(const char *a, const char *b)
{
    size_t la = strlen(a), lb = strlen(b);
    size_t n = la > lb ? la : lb;
    unsigned char diff = (unsigned char)(la ^ lb);
    for (size_t i = 0; i < n; i++) {
        unsigned char ca = i < la ? (unsigned char)a[i] : 0;
        unsigned char cb = i < lb ? (unsigned char)b[i] : 0;
        diff |= (unsigned char)(ca ^ cb);
    }
    return diff == 0;
}

static bool bearer_ok(struct http_server *http, const struct mg_http_message *hm)
{
    if (!http->config.api_token[0]) return true;
    const struct mg_str *hdr = mg_http_get_header((struct mg_http_message *)hm, "Authorization");
    if (!hdr || hdr->len < 8 || strncmp(hdr->buf, "Bearer ", 7) != 0)
        return false;
    char tok[256];
    size_t tlen = hdr->len - 7;
    if (tlen >= sizeof(tok)) tlen = sizeof(tok) - 1;
    memcpy(tok, hdr->buf + 7, tlen);
    tok[tlen] = '\0';
    for (int i = (int)tlen - 1; i >= 0; i--) {
        if (tok[i] == '\r' || tok[i] == '\n' || tok[i] == ' ') tok[i] = '\0';
        else break;
    }
    return ct_str_eq(tok, http->config.api_token);
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
    *out = NULL;
    *outlen = 0;

    dynbuf_t d;
    if (dyn_init(&d, 8192) != 0) return;

    db_publisher_t *pubs = NULL; int pub_cnt = 0;
    db_player_t *players = NULL; int player_cnt = 0;

    if (stream_id) {
        db_publisher_list(http->db, stream_id, &pubs, &pub_cnt);
        db_player_list(http->db, stream_id, &players, &player_cnt);
    } else {
        db_publisher_list_all(http->db, &pubs, &pub_cnt);
        db_player_list_all(http->db, &players, &player_cnt);
    }

    dyn_appendf(&d, "{\"streams\":[");

    for (int i = 0; i < pub_cnt; i++) {
        db_publisher_t *p = &pubs[i];
        int uptime_s = (int)(time(NULL) - p->connected_at);
        char e_id[1024], e_name[1024], e_app[1024], e_vcodec[256], e_acodec[256], e_addr[512];
        json_escape(p->stream_id, e_id, sizeof(e_id));
        json_escape(p->stream_name, e_name, sizeof(e_name));
        json_escape(p->app, e_app, sizeof(e_app));
        json_escape(p->video_codec, e_vcodec, sizeof(e_vcodec));
        json_escape(p->audio_codec, e_acodec, sizeof(e_acodec));
        json_escape(p->remote_addr, e_addr, sizeof(e_addr));

        dyn_appendf(&d,
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
            e_id, e_name, e_app,
            uptime_s, p->bitrate_kbps,
            (unsigned long long)p->bytes_in,
            e_vcodec, p->video_width, p->video_height, p->fps,
            e_acodec,
            e_addr);
    }

    /* Players grouped by stream */
    dyn_appendf(&d, "],\"players\":[");
    for (int i = 0; i < player_cnt; i++) {
        db_player_t *pl = &players[i];
        int uptime_s = (int)(time(NULL) - pl->connected_at);
        char e_id[512], e_name[1024], e_app[1024], e_addr[512];
        json_escape(pl->id, e_id, sizeof(e_id));
        json_escape(pl->stream_name, e_name, sizeof(e_name));
        json_escape(pl->app, e_app, sizeof(e_app));
        json_escape(pl->remote_addr, e_addr, sizeof(e_addr));

        dyn_appendf(&d,
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
            e_id, e_name, e_app,
            uptime_s, pl->bitrate_kbps,
            (unsigned long long)pl->bytes_out,
            e_addr);
    }

    dyn_appendf(&d,
        "],\"summary\":{\"publishers\":%d,\"players\":%d,\"total_clients\":%d}}",
        pub_cnt, player_cnt, pub_cnt + player_cnt);

    db_publisher_free_list(pubs);
    db_player_free_list(players);

    *out = d.buf;
    *outlen = d.len;
}

/* ---------- XML stats (nginx-rtmp compatible) ---------- */

static void build_nginx_xml(struct http_server *http, const char *stream_id,
                             char **out, size_t *outlen)
{
    *out = NULL;
    *outlen = 0;

    dynbuf_t d;
    if (dyn_init(&d, 8192) != 0) return;

    db_publisher_t *pubs = NULL; int pub_cnt = 0;
    db_player_t *players = NULL; int player_cnt = 0;

    if (stream_id) {
        db_publisher_list(http->db, stream_id, &pubs, &pub_cnt);
        db_player_list(http->db, stream_id, &players, &player_cnt);
    } else {
        db_publisher_list_all(http->db, &pubs, &pub_cnt);
        db_player_list_all(http->db, &players, &player_cnt);
    }

    dyn_appendf(&d,
        "<?xml version=\"1.0\" encoding=\"utf-8\"?>\n<rtmp>\n  <server>\n"
        "    <application>\n      <name>live</name>\n      <live>\n");

    for (int i = 0; i < pub_cnt; i++) {
        db_publisher_t *p = &pubs[i];
        int uptime_ms = (int)(time(NULL) - p->connected_at) * 1000;
        int bw_in = (int)(p->bitrate_kbps * 1000);
        char e_name[1024], e_vcodec[256], e_acodec[256], e_addr[512];
        xml_escape(p->stream_name, e_name, sizeof(e_name));
        xml_escape(p->video_codec, e_vcodec, sizeof(e_vcodec));
        xml_escape(p->audio_codec, e_acodec, sizeof(e_acodec));
        xml_escape(p->remote_addr, e_addr, sizeof(e_addr));

        dyn_appendf(&d,
            "        <stream>\n"
            "          <name>%s</name>\n"
            "          <time>%d</time>\n"
            "          <bw_in>%d</bw_in>\n"
            "          <bytes_in>%llu</bytes_in>\n"
            "          <bw_out>0</bw_out>\n"
            "          <bytes_out>0</bytes_out>\n",
            e_name, uptime_ms, bw_in,
            (unsigned long long)p->bytes_in);

        if (p->video_codec[0]) {
            dyn_appendf(&d,
                "          <video>\n"
                "            <width>%u</width>\n"
                "            <height>%u</height>\n"
                "            <frame_rate>%.1f</frame_rate>\n"
                "            <codec>%s</codec>\n"
                "            <profile>baseline</profile>\n"
                "            <level>3.1</level>\n"
                "          </video>\n",
                p->video_width, p->video_height, p->fps, e_vcodec);
        }

        if (p->audio_codec[0]) {
            dyn_appendf(&d,
                "          <audio>\n"
                "            <codec>%s</codec>\n"
                "            <sample_rate>44100</sample_rate>\n"
                "            <channels>2</channels>\n"
                "          </audio>\n",
                e_acodec);
        }

        dyn_appendf(&d,
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
            e_addr, uptime_ms, uptime_ms);
    }

    for (int i = 0; i < player_cnt; i++) {
        db_player_t *pl = &players[i];
        int uptime_ms = (int)(time(NULL) - pl->connected_at) * 1000;
        char e_name[1024], e_addr[512];
        xml_escape(pl->stream_name, e_name, sizeof(e_name));
        xml_escape(pl->remote_addr, e_addr, sizeof(e_addr));

        dyn_appendf(&d,
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
            e_name, uptime_ms,
            (int)(pl->bitrate_kbps * 1000),
            (unsigned long long)pl->bytes_out,
            e_addr, uptime_ms, uptime_ms);
    }

    dyn_appendf(&d,
        "        <nclients>%d</nclients>\n"
        "      </live>\n    </application>\n  </server>\n</rtmp>\n",
        pub_cnt + player_cnt);

    db_publisher_free_list(pubs);
    db_player_free_list(players);

    *out = d.buf;
    *outlen = d.len;
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
    if (!json) { err_json(c, 500, "INTERNAL", "Out of memory"); return; }
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
    if (!xml) { err_xml(c, 500, "Out of memory"); return; }
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

    dynbuf_t d;
    if (dyn_init(&d, 4096) != 0) {
        db_stream_free_list(streams);
        err_json(c, 500, "INTERNAL", "Out of memory");
        return;
    }
    dyn_appendf(&d, "[");
    for (int i = 0; i < count; i++) {
        /* Never expose keys in list view */
        char e_id[1024], e_name[1024], e_app[1024];
        json_escape(streams[i].id, e_id, sizeof(e_id));
        json_escape(streams[i].name, e_name, sizeof(e_name));
        json_escape(streams[i].app, e_app, sizeof(e_app));
        dyn_appendf(&d,
            "%s{\"id\":\"%s\",\"name\":\"%s\",\"app\":\"%s\",\"enabled\":%s,\"created_at\":%ld}",
            i > 0 ? "," : "",
            e_id, e_name, e_app,
            streams[i].enabled ? "true" : "false",
            (long)streams[i].created_at);
    }
    dyn_appendf(&d, "]");

    db_stream_free_list(streams);
    send_json(c, 200, d.buf, d.len);
    free(d.buf);
}

/* POST /api/v1/streams  → creates stream, returns keys */
static void handle_stream_create(struct mg_connection *c, struct http_server *http,
                                  const struct mg_http_message *hm)
{
    if (!bearer_ok(http, hm)) { err_json(c, 401, "UNAUTHORIZED", "Missing or invalid token"); return; }

    char body[4096];
    size_t blen = hm->body.len < sizeof(body) - 1 ? hm->body.len : sizeof(body) - 1;
    memcpy(body, hm->body.buf, blen);
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
    if (!json) { err_json(c, 500, "INTERNAL", "Out of memory"); return; }
    send_json(c, 200, json, jlen);
    free(json);
}

/* ---------- mongoose event handler ---------- */

/* Mongoose 7.x: mg_http_listen stores fn_data on the listening connection.
 * For accepted HTTP connections, we use a static pointer set at startup. */
static http_server_t *g_http_server = NULL;

static void http_handler(struct mg_connection *c, int ev, void *ev_data)
{
    (void)c;
    if (ev != MG_EV_HTTP_MSG) return;

    struct http_server *http = g_http_server;
    struct mg_http_message *hm = (struct mg_http_message *)ev_data;
    struct mg_str uri = hm->uri;

    /* Health — no auth */
    if (match_uri(hm, "/api/v1/health")) {
        handle_health(c, http);
        return;
    }

    /* /stats?key=*** → JSON */
    if (match_uri(hm, "/stats")) {
        handle_stats_json(c, http, hm);
        return;
    }

    /* /stats-nginx?key=*** → XML */
    if (match_uri(hm, "/stats-nginx")) {
        handle_stats_nginx(c, http, hm);
        return;
    }

    /* /api/v1/streams */
    if (match_uri(hm, "/api/v1/streams")) {
        /* Route by HTTP method, not body length. A well-behaved GET has no
         * body; a POST carries JSON. Routing on body length misclassifies
         * GETs with bodies (e.g. misbehaving proxies) and fails to reject
         * unsupported methods like PUT/PATCH. */
        if (hm->method.len == 3 && strncmp(hm->method.buf, "GET", 3) == 0) {
            handle_streams_list(c, http, hm);
        } else if (hm->method.len == 4 && strncmp(hm->method.buf, "POST", 4) == 0) {
            handle_stream_create(c, http, hm);
        } else {
            err_json(c, 405, "METHOD_NOT_ALLOWED", "Only GET and POST accepted");
        }
        return;
    }

    /* /api/v1/streams/X  (DELETE or /stats) */
    if (match_uri(hm, "/api/v1/streams/*")) {
        /* Check for /stats sub-path */
        const char *after = strstr(uri.buf, "/api/v1/streams/");
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

            /* DELETE /api/v1/streams/:id — only accept DELETE method */
            if (hm->method.len == 6 && strncmp(hm->method.buf, "DELETE", 6) == 0) {
                char sid[64] = {0};
                strncpy(sid, after, sizeof(sid) - 1);
                for (int i = 0; sid[i]; i++) {
                    if (sid[i] == '/' || sid[i] == '?') { sid[i] = '\0'; break; }
                }
                if (sid[0]) {
                    handle_stream_delete(c, http, hm, sid);
                    return;
                }
            } else if (hm->method.len == 3 && strncmp(hm->method.buf, "GET", 3) == 0) {
                /* GET /api/v1/streams/:id — could return single stream info later */
                err_json(c, 404, "NOT_FOUND", "Unknown endpoint");
                return;
            } else {
                err_json(c, 405, "METHOD_NOT_ALLOWED", "Only DELETE accepted");
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
    g_http_server = http;
    if (!mg_http_listen(&http->mgr, http->listen_addr, http_handler, NULL)) {
        log_error("Failed to start HTTP on %s", http->config.http_bind);
        return -1;
    }
    log_info("HTTP listening on %s", http->config.http_bind);
    return 0;
}

void http_server_stop(http_server_t *http) { http->running = 0; }
void http_server_poll(http_server_t *http, int ms) { mg_mgr_poll(&http->mgr, ms); }
void http_server_set_db(http_server_t *http, void *db) { http->db = (db_context_t *)db; }
