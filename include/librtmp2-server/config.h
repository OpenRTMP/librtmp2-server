/**
 * config.h — JSON configuration parsing
 */
#ifndef LRTMP2_SERVER_CONFIG_H
#define LRTMP2_SERVER_CONFIG_H

#include "server.h"
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Load config from JSON file. Zero-out config first as defaults. */
bool config_load(const char *path, server_config_t *config, char *error, size_t errlen);

/* Load default config */
void config_set_defaults(server_config_t *config);

#ifdef __cplusplus
}
#endif

#endif /* LRTMP2_SERVER_CONFIG_H */
