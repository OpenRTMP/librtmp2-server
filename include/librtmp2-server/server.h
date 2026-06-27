/**
 * server.h — Main server context and lifecycle
 */
#ifndef LRTMP2_SERVER_APP_H
#define LRTMP2_SERVER_APP_H

#include <stdint.h>
#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct lrtmp2_server_app lrtmp2_server_app_t;

typedef struct {
    /* RTMP listener */
    char rtmp_bind[64];       /* e.g. "0.0.0.0:1935" */
    int  rtmp_max_conn;
    int  rtmp_chunk_size;

    /* HTTP API + UI */
    char http_bind[64];       /* e.g. "0.0.0.0:8080" */

    /* Auth */
    char api_token[128];
    bool require_stream_key;

    /* Paths */
    char web_root[256];       /* static web UI files */
    char config_file[256];    /* path to config JSON */

    /* Logging */
    int  log_level;           /* 0=error, 1=warn, 2=info, 3=debug */
    char log_file[256];       /* optional file path, empty = stderr */
} server_config_t;

lrtmp2_server_app_t *server_app_create(const server_config_t *config);
void                 server_app_destroy(lrtmp2_server_app_t *app);
int                  server_app_run(lrtmp2_server_app_t *app);  /* blocks until stopped */
void                 server_app_stop(lrtmp2_server_app_t *app);

#ifdef __cplusplus
}
#endif

#endif /* LRTMP2_SERVER_APP_H */
