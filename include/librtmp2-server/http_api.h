/**
 * http_api.h — HTTP server with REST API + static file serving
 */
#ifndef LRTMP2_SERVER_HTTP_API_H
#define LRTMP2_SERVER_HTTP_API_H

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

/* Set references to subsystems for API endpoints */
void http_server_set_stream_registry(http_server_t *http, void *reg);
void http_server_set_session_manager(http_server_t *http, void *mgr);
void http_server_set_stats_collector(http_server_t *http, void *stats);

#ifdef __cplusplus
}
#endif

#endif /* LRTMP2_SERVER_HTTP_API_H */
