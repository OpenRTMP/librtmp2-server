/**
 * stream_registry.h — Stream configuration and state
 */
#ifndef LRTMP2_SERVER_STREAM_REGISTRY_H
#define LRTMP2_SERVER_STREAM_REGISTRY_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    char id[64];
    char name[128];
    char app[64];
    char stream_key[128];
    bool enabled;
    bool require_auth;
    char allowed_codecs[8][8];  /* e.g. "avc1", "hvc1" */
    int  allowed_codecs_count;
    time_t created_at;
    time_t updated_at;
} stream_entry_t;

typedef struct stream_registry stream_registry_t;

stream_registry_t *stream_registry_create(void);
void               stream_registry_destroy(stream_registry_t *reg);

/* CRUD */
bool stream_registry_add(stream_registry_t *reg, const stream_entry_t *entry);
bool stream_registry_update(stream_registry_t *reg, const char *id, const stream_entry_t *entry);
bool stream_registry_remove(stream_registry_t *reg, const char *id);
const stream_entry_t *stream_registry_get(stream_registry_t *reg, const char *id);
void stream_registry_foreach(stream_registry_t *reg, void (*cb)(const stream_entry_t *entry, void *ud), void *ud);

/* Find by app + stream key (for publish auth) */
const stream_entry_t *stream_registry_find_by_key(stream_registry_t *reg, const char *app, const char *stream_key);

/* Load/save from JSON config */
bool stream_registry_load(stream_registry_t *reg, const char *json_streams);
int  stream_registry_count(stream_registry_t *reg);

#ifdef __cplusplus
}
#endif

#endif /* LRTMP2_SERVER_STREAM_REGISTRY_H */
