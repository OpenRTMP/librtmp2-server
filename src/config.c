/**
 * config.c — JSON configuration file parsing
 *
 * Minimal parser — no external dependency. Handles the flat structure
 * we need for server config. For production, swap with cJSON or similar.
 */
#include "librtmp2-server/config.h"
#include "librtmp2-server/logger.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

void config_set_defaults(server_config_t *config)
{
    memset(config, 0, sizeof(*config));
    strncpy(config->rtmp_bind, "0.0.0.0:1935", sizeof(config->rtmp_bind) - 1);
    config->rtmp_max_conn = 100;
    config->rtmp_chunk_size = 4096;
    strncpy(config->http_bind, "0.0.0.0:8080", sizeof(config->http_bind) - 1);
    /* api_token left empty by default — server will refuse to start
     * with protected endpoints unless a token is configured. */
    config->api_token[0] = '\0';
    config->require_stream_key = true;
    strncpy(config->web_root, "./web", sizeof(config->web_root) - 1);
    config->log_level = 2; /* info */
}

/* --- minimal JSON helpers --- */

static const char *skip_ws(const char *p)
{
    while (*p == ' ' || *p == '\t' || *p == '\n' || *p == '\r') p++;
    return p;
}

/* Read a JSON string value after the key has been found. Returns end of value. */
static const char *read_json_string(const char *p, char *out, size_t outlen)
{
    p = skip_ws(p);
    if (*p != '"') return NULL;
    p++;
    size_t i = 0;
    while (*p && *p != '"' && i < outlen - 1) {
        if (*p == '\\' && p[1]) {
            p++;
            switch (*p) {
                case 'n': out[i++] = '\n'; break;
                case 't': out[i++] = '\t'; break;
                case '"': out[i++] = '"'; break;
                case '\\': out[i++] = '\\'; break;
                default: out[i++] = *p; break;
            }
        } else {
            out[i++] = *p;
        }
        p++;
    }
    out[i] = '\0';
    if (*p == '"') p++;
    return p;
}

static const char *find_key(const char *json, const char *key)
{
    char needle[256];
    snprintf(needle, sizeof(needle), "\"%s\"", key);
    const char *p = json;
    while ((p = strstr(p, needle)) != NULL) {
        p += strlen(needle);
        p = skip_ws(p);
        if (*p == ':') return p + 1;
    }
    return NULL;
}

static int read_json_int(const char *p, int *out)
{
    p = skip_ws(p);
    char *end;
    long val = strtol(p, &end, 10);
    if (end == p) return -1;
    *out = (int)val;
    return 0;
}

static int read_json_bool(const char *p, bool *out)
{
    p = skip_ws(p);
    if (strncmp(p, "true", 4) == 0)  { *out = true;  return 0; }
    if (strncmp(p, "false", 5) == 0) { *out = false; return 0; }
    return -1;
}

/* Extract a string value for a given key */
static bool get_string(const char *json, const char *key, char *out, size_t outlen)
{
    const char *p = find_key(json, key);
    if (!p) return false;
    read_json_string(p, out, outlen);
    return true;
}

static bool get_int(const char *json, const char *key, int *out)
{
    const char *p = find_key(json, key);
    if (!p) return false;
    return read_json_int(p, out) == 0;
}

static bool get_bool(const char *json, const char *key, bool *out)
{
    const char *p = find_key(json, key);
    if (!p) return false;
    return read_json_bool(p, out) == 0;
}

/* Find the start of an object value for a given key: returns pointer to '{' */
static const char *find_object(const char *json, const char *key)
{
    const char *p = find_key(json, key);
    if (!p) return NULL;
    p = skip_ws(p);
    return (*p == '{') ? p : NULL;
}

/* Find array start: returns pointer to '[' */
static const char *find_array(const char *json, const char *key)
{
    const char *p = find_key(json, key);
    if (!p) return NULL;
    p = skip_ws(p);
    return (*p == '[') ? p : NULL;
}

/* Find matching closing brace. Returns pointer to the '}'. */
static const char *match_brace(const char *p)
{
    int depth = 0;
    while (*p) {
        if (*p == '{') depth++;
        else if (*p == '}') { depth--; if (depth == 0) return p; }
        else if (*p == '"') { p++; while (*p && *p != '"') { if (*p == '\\') p++; p++; } }
        p++;
    }
    return p;
}

/* Find matching closing bracket. Returns pointer to the ']'. */
static const char *match_bracket(const char *p)
{
    int depth = 0;
    while (*p) {
        if (*p == '[') depth++;
        else if (*p == ']') { depth--; if (depth == 0) return p; }
        else if (*p == '"') { p++; while (*p && *p != '"') { if (*p == '\\') p++; p++; } }
        p++;
    }
    return p;
}

void config_apply_env(server_config_t *config)
{
    const char *v;

    v = getenv("LRTMP2_API_TOKEN");
    if (v && v[0]) {
        strncpy(config->api_token, v, sizeof(config->api_token) - 1);
        config->api_token[sizeof(config->api_token) - 1] = '\0';
    }

    v = getenv("LRTMP2_RTMP_BIND");
    if (v && v[0]) {
        strncpy(config->rtmp_bind, v, sizeof(config->rtmp_bind) - 1);
        config->rtmp_bind[sizeof(config->rtmp_bind) - 1] = '\0';
    }

    v = getenv("LRTMP2_HTTP_BIND");
    if (v && v[0]) {
        strncpy(config->http_bind, v, sizeof(config->http_bind) - 1);
        config->http_bind[sizeof(config->http_bind) - 1] = '\0';
    }

    v = getenv("LRTMP2_LOG_LEVEL");
    if (v && v[0]) {
        int lvl = atoi(v);
        if (lvl >= 0 && lvl <= 5) config->log_level = lvl;
    }

    v = getenv("LRTMP2_DB_PATH");
    if (v && v[0]) {
        /* db_path is not part of server_config_t yet — stored for later use */
        (void)0;
    }
}

bool config_load(const char *path, server_config_t *config, char *error, size_t errlen)
{
    config_set_defaults(config);

    FILE *f = fopen(path, "r");
    if (!f) {
        snprintf(error, errlen, "Cannot open config file: %s", path);
        return false;
    }

    fseek(f, 0, SEEK_END);
    long len = ftell(f);
    fseek(f, 0, SEEK_SET);

    if (len < 0) {
        fclose(f);
        snprintf(error, errlen, "Cannot determine config file size: %s", path);
        return false;
    }

    char *json = malloc((size_t)len + 1);
    if (!json) {
        fclose(f);
        snprintf(error, errlen, "Out of memory");
        return false;
    }

    size_t got = fread(json, 1, (size_t)len, f);
    json[got] = '\0';
    fclose(f);

    /* Top-level keys */
    get_string(json, "api_token", config->api_token, sizeof(config->api_token));
    get_bool(json, "require_stream_key", &config->require_stream_key);
    get_int(json, "log_level", &config->log_level);
    get_string(json, "log_file", config->log_file, sizeof(config->log_file));
    get_string(json, "web_root", config->web_root, sizeof(config->web_root));

    /* rtmp object */
    const char *rtmp = find_object(json, "rtmp");
    if (rtmp) {
        const char *end = match_brace(rtmp);
        char buf[1024];
        size_t blen = end - rtmp + 1;
        if (blen > sizeof(buf) - 1) blen = sizeof(buf) - 1;
        memcpy(buf, rtmp, blen);
        buf[blen] = '\0';
        get_string(buf, "bind", config->rtmp_bind, sizeof(config->rtmp_bind));
        get_int(buf, "max_connections", &config->rtmp_max_conn);
        get_int(buf, "chunk_size", &config->rtmp_chunk_size);
    }

    /* http object */
    const char *http = find_object(json, "http");
    if (http) {
        const char *end = match_brace(http);
        char buf[1024];
        size_t blen = end - http + 1;
        if (blen > sizeof(buf) - 1) blen = sizeof(buf) - 1;
        memcpy(buf, http, blen);
        buf[blen] = '\0';
        get_string(buf, "bind", config->http_bind, sizeof(config->http_bind));
    }

    /* auth object */
    const char *auth = find_object(json, "auth");
    if (auth) {
        const char *end = match_brace(auth);
        char buf[1024];
        size_t blen = end - auth + 1;
        if (blen > sizeof(buf) - 1) blen = sizeof(buf) - 1;
        memcpy(buf, auth, blen);
        buf[blen] = '\0';
        get_string(buf, "api_token", config->api_token, sizeof(config->api_token));
        get_bool(buf, "require_stream_key", &config->require_stream_key);
    }

    log_info("Config loaded from %s", path);
    log_debug("RTMP bind=%s, HTTP bind=%s", config->rtmp_bind, config->http_bind);

    free(json);
    return true;
}
