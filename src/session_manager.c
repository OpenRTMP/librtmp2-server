/**
 * session_manager.c — Thread-safe active session tracking
 */
#include "librtmp2-server/session_manager.h"
#include "librtmp2-server/logger.h"
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <stdio.h>

#define MAX_SESSIONS 1024

struct session_manager {
    session_entry_t entries[MAX_SESSIONS];
    int           count;
    int           next_id;
    pthread_mutex_t lock;
};

static int alloc_session_id(session_manager_t *mgr)
{
    return ++mgr->next_id;
}

session_manager_t *session_manager_create(void)
{
    session_manager_t *mgr = calloc(1, sizeof(session_manager_t));
    if (!mgr) return NULL;
    pthread_mutex_init(&mgr->lock, NULL);
    return mgr;
}

void session_manager_destroy(session_manager_t *mgr)
{
    if (!mgr) return;
    pthread_mutex_destroy(&mgr->lock);
    free(mgr);
}

bool session_add(session_manager_t *mgr, const session_entry_t *entry)
{
    pthread_mutex_lock(&mgr->lock);
    if (mgr->count >= MAX_SESSIONS) {
        pthread_mutex_unlock(&mgr->lock);
        log_error("Session limit reached (%d), rejecting new session", MAX_SESSIONS);
        return false;
    }
    session_entry_t *e = &mgr->entries[mgr->count++];
    memcpy(e, entry, sizeof(*e));
    e->started_at = time(NULL);
    e->status = SESSION_ACTIVE;
    pthread_mutex_unlock(&mgr->lock);
    log_info("Session started: id=%s role=%s stream=%s remote=%s",
             e->id, e->role == SESSION_PUBLISHER ? "publisher" : "player",
             e->stream_id[0] ? e->stream_id : "(unknown)", e->remote_addr);
    return true;
}

bool session_update(session_manager_t *mgr, const char *id, const session_entry_t *entry)
{
    pthread_mutex_lock(&mgr->lock);
    for (int i = 0; i < mgr->count; i++) {
        if (strcmp(mgr->entries[i].id, id) == 0) {
            /* Preserve started_at and id */
            time_t started = mgr->entries[i].started_at;
            memcpy(&mgr->entries[i], entry, sizeof(session_entry_t));
            mgr->entries[i].started_at = started;
            pthread_mutex_unlock(&mgr->lock);
            return true;
        }
    }
    pthread_mutex_unlock(&mgr->lock);
    return false;
}

bool session_remove(session_manager_t *mgr, const char *id)
{
    pthread_mutex_lock(&mgr->lock);
    for (int i = 0; i < mgr->count; i++) {
        if (strcmp(mgr->entries[i].id, id) == 0) {
            log_info("Session removed: %s", id);
            memmove(&mgr->entries[i], &mgr->entries[i + 1],
                    (mgr->count - i - 1) * sizeof(session_entry_t));
            mgr->count--;
            pthread_mutex_unlock(&mgr->lock);
            return true;
        }
    }
    pthread_mutex_unlock(&mgr->lock);
    return false;
}

bool session_disconnect(session_manager_t *mgr, const char *id)
{
    pthread_mutex_lock(&mgr->lock);
    for (int i = 0; i < mgr->count; i++) {
        if (strcmp(mgr->entries[i].id, id)) continue;
        mgr->entries[i].status = SESSION_CLOSED;
        mgr->entries[i].ended_at = time(NULL);
        log_info("Session disconnected: %s", id);
        pthread_mutex_unlock(&mgr->lock);
        return true;
    }
    pthread_mutex_unlock(&mgr->lock);
    return false;
}

session_entry_t *session_get(session_manager_t *mgr, const char *id)
{
    pthread_mutex_lock(&mgr->lock);
    for (int i = 0; i < mgr->count; i++) {
        if (strcmp(mgr->entries[i].id, id) == 0) {
            pthread_mutex_unlock(&mgr->lock);
            return &mgr->entries[i];
        }
    }
    pthread_mutex_unlock(&mgr->lock);
    return NULL;
}

void session_manager_foreach(session_manager_t *mgr, void (*cb)(const session_entry_t *, void *), void *ud)
{
    pthread_mutex_lock(&mgr->lock);
    for (int i = 0; i < mgr->count; i++) {
        cb(&mgr->entries[i], ud);
    }
    pthread_mutex_unlock(&mgr->lock);
}

int session_manager_count(session_manager_t *mgr)
{
    return mgr->count;
}

int session_manager_count_active(session_manager_t *mgr)
{
    pthread_mutex_lock(&mgr->lock);
    int n = 0;
    for (int i = 0; i < mgr->count; i++) {
        if (mgr->entries[i].status == SESSION_ACTIVE) n++;
    }
    pthread_mutex_unlock(&mgr->lock);
    return n;
}

int session_manager_count_by_stream(session_manager_t *mgr, const char *stream_id)
{
    pthread_mutex_lock(&mgr->lock);
    int n = 0;
    for (int i = 0; i < mgr->count; i++) {
        if (strcmp(mgr->entries[i].stream_id, stream_id) == 0 && mgr->entries[i].status == SESSION_ACTIVE) n++;
    }
    pthread_mutex_unlock(&mgr->lock);
    return n;
}

void session_update_bytes(session_manager_t *mgr, const char *id, uint64_t bytes_in_delta)
{
    pthread_mutex_lock(&mgr->lock);
    for (int i = 0; i < mgr->count; i++) {
        if (strcmp(mgr->entries[i].id, id) == 0) {
            mgr->entries[i].bytes_in += bytes_in_delta;
            break;
        }
    }
    pthread_mutex_unlock(&mgr->lock);
}
