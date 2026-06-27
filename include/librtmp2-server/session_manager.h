/**
 * session_manager.h — Active session tracking
 */
#ifndef LRTMP2_SERVER_SESSION_MANAGER_H
#define LRTMP2_SERVER_SESSION_MANAGER_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    SESSION_ACTIVE = 0,
    SESSION_CLOSED,
    SESSION_ERROR,
} session_status_t;

typedef enum {
    SESSION_PUBLISHER = 0,
    SESSION_PLAYER,
} session_role_t;

typedef struct {
    char id[64];
    char stream_id[64];
    char app[64];
    char stream_key[128];
    char remote_addr[64];
    session_role_t  role;
    session_status_t status;
    time_t started_at;
    time_t ended_at;

    /* Stats */
    uint64_t bytes_in;
    uint64_t bytes_out;
    double   bitrate_in;      /* bits per second, rolling */
    double   fps;             /* video FPS estimate */
    char     video_codec[16];
    char     audio_codec[16];
    char     last_error[256];
} session_entry_t;

typedef struct session_manager session_manager_t;

session_manager_t *session_manager_create(void);
void               session_manager_destroy(session_manager_t *mgr);

/* Session lifecycle */
bool session_add(session_manager_t *mgr, const session_entry_t *entry);
bool session_update(session_manager_t *mgr, const char *id, const session_entry_t *entry);
bool session_remove(session_manager_t *mgr, const char *id);
bool session_disconnect(session_manager_t *mgr, const char *id);
session_entry_t *session_get(session_manager_t *mgr, const char *id);

void session_manager_foreach(session_manager_t *mgr, void (*cb)(const session_entry_t *entry, void *ud), void *ud);
int  session_manager_count(session_manager_t *mgr);
int  session_manager_count_active(session_manager_t *mgr);
int  session_manager_count_by_stream(session_manager_t *mgr, const char *stream_id);

/* Update stats */
void session_update_bytes(session_manager_t *mgr, const char *id, uint64_t bytes_in_delta);

#ifdef __cplusplus
}
#endif

#endif /* LRTMP2_SERVER_SESSION_MANAGER_H */
