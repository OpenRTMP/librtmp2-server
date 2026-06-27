/**
 * http.h — HTTP server: REST API + Nginx-RTMP-compatible /stats XML
 */
#ifndef LRTMP2_SERVER_HTTP_H
#define LRTMP2_SERVER_HTTP_H

#include "server.h"
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct http_server http_server_t;

http_server_t *http_server_create(const server_config_t *config);
void           http_server_destroy(http_server_t *http);
int            http_server_start(http_server_t *http);
void           http_server_stop(http_server_t *http);
void           http_server_poll(http_server_t *http, int timeout_ms);

/* Set DB reference for API endpoints */
void http_server_set_db(http_server_t *http, void *db);

#ifdef __cplusplus
}
#endif

#endif /* LRTMP2_SERVER_HTTP_H */
