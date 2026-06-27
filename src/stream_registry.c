/**
 * stream_registry.c — Thread-safe stream registry
 */
#include "librtmp2-server/stream_registry.h"
#include "librtmp2-server/logger.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#define MAX_STREAMS 256

struct stream_registry {
    stream_entry_t entries[MAX_STREAMS];
    int           count;
    pthread_mutex_t lock;
};

stream_registry_t *stream_registry_create(void)
{
    stream_registry_t *reg = calloc(1, sizeof(stream_registry_t));
    if (!reg) return NULL;
    pthread_mutex_init(&reg->lock, NULL);
    return reg;
}

void stream_registry_destroy(stream_registry_t *reg)
{
    if (!reg) return;
    pthread_mutex_destroy(&reg->lock);
    free(reg);
}

bool stream_registry_add(stream_registry_t *reg, const stream_entry_t *entry)
{
    pthread_mutex_lock(&reg->lock);
    if (reg->count >= MAX_STREAMS) {
        pthread_mutex_unlock(&reg->lock);
        log_error("Stream registry full, cannot add '%s'", entry->id);
        return false;
    }
    /* Check for duplicate ID */
    for (int i = 0; i < reg->count; i++) {
        if (strcmp(reg->entries[i].id, entry->id) == 0) {
            pthread_mutex_unlock(&reg->lock);
            log_error("Stream '%s' already exists", entry->id);
            return false;
        }
    }
    stream_entry_t *e = &reg->entries[reg->count++];
    memcpy(e, entry, sizeof(*e));
    e->created_at = time(NULL);
    e->updated_at = e->created_at;
    pthread_mutex_unlock(&reg->lock);
    log_info("Stream registered: id=%s app=%s key=%s", e->id, e->app, e->stream_key);
    return true;
}

bool stream_registry_update(stream_registry_t *reg, const char *id, const stream_entry_t *entry)
{
    pthread_mutex_lock(&reg->lock);
    for (int i = 0; i < reg->count; i++) {
        if (strcmp(reg->entries[i].id, id) == 0) {
            stream_entry_t *e = &reg->entries[i];
            memcpy(e, entry, sizeof(*e));
            strncpy(e->id, id, sizeof(e->id) - 1); /* preserve id */
            e->updated_at = time(NULL);
            pthread_mutex_unlock(&reg->lock);
            log_info("Stream updated: %s", id);
            return true;
        }
    }
    pthread_mutex_unlock(&reg->lock);
    log_warn("Stream '%s' not found for update", id);
    return false;
}

bool stream_registry_remove(stream_registry_t *reg, const char *id)
{
    pthread_mutex_lock(&reg->lock);
    for (int i = 0; i < reg->count; i++) {
        if (strcmp(reg->entries[i].id, id) == 0) {
            /* shift remaining */
            memmove(&reg->entries[i], &reg->entries[i + 1],
                    (reg->count - i - 1) * sizeof(stream_entry_t));
            reg->count--;
            pthread_mutex_unlock(&reg->lock);
            log_info("Stream removed: %s", id);
            return true;
        }
    }
    pthread_mutex_unlock(&reg->lock);
    log_warn("Stream '%s' not found for removal", id);
    return false;
}

const stream_entry_t *stream_registry_get(stream_registry_t *reg, const char *id)
{
    pthread_mutex_lock(&reg->lock);
    for (int i = 0; i < reg->count; i++) {
        if (strcmp(reg->entries[i].id, id) == 0) {
            const stream_entry_t *e = &reg->entries[i];
            pthread_mutex_unlock(&reg->lock);
            return e;
        }
    }
    pthread_mutex_unlock(&reg->lock);
    return NULL;
}

void stream_registry_foreach(stream_registry_t *reg, void (*cb)(const stream_entry_t *, void *), void *ud)
{
    pthread_mutex_lock(&reg->lock);
    for (int i = 0; i < reg->count; i++) {
        cb(&reg->entries[i], ud);
    }
    pthread_mutex_unlock(&reg->lock);
}

const stream_entry_t *stream_registry_find_by_key(stream_registry_t *reg, const char *app, const char *stream_key)
{
    pthread_mutex_lock(&reg->lock);
    for (int i = 0; i < reg->count; i++) {
        stream_entry_t *e = &reg->entries[i];
        if (!e->enabled) continue;
        if (strcmp(e->app, app) == 0 && strcmp(e->stream_key, stream_key) == 0) {
            pthread_mutex_unlock(&reg->lock);
            return e;
        }
    }
    pthread_mutex_unlock(&reg->lock);
    return NULL;
}

int stream_registry_count(stream_registry_t *reg)
{
    return reg->count;
}

/* Minimal JSON array loader for streams (delegates string parsing to config.c level) */
bool stream_registry_load(stream_registry_t *reg, const char *json)
{
    /* This is a simplified loader — expects config.c to pre-parse.
     * Full implementation would parse JSON array of objects.
     * Stub for now — streams are added via HTTP API at runtime or
     * by direct calls to stream_registry_add(). */
    (void)reg; (void)json;
    log_info("Stream registry load from JSON (stub — use API or programmatic add)");
    return true;
}
