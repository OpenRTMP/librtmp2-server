/**
 * http_static.c — Static file serving helpers
 *
 * Currently handled by mongoose's built-in mg_http_serve_dir().
 * This file is for future customizations (e.g. caching headers,
 * gzip, SPA routing).
 */
#include "librtmp2-server/http_api.h"
/* Intentionally minimal — mongoose handles static files inline */
