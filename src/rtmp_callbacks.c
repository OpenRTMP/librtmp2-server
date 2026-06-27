/**
 * rtmp_callbacks.c — Bridge librtmp2 callbacks to the server subsystems
 */
#include "librtmp2-server/server.h"
#include "librtmp2-server/stream_registry.h"
#include "librtmp2-server/session_manager.h"
#include "librtmp2-server/stats_collector.h"
#include "librtmp2-server/logger.h"
#include "librtmp2/librtmp2.h"
#include <stdio.h>
#include <string.h>

/* Userdata passed to all callbacks */
typedef struct {
    stream_registry_t  *registry;
    session_manager_t  *sessions;
    stats_collector_t  *stats;
    char                stream_id[64];
} rtmp_bridge_t;

static int on_connect(lrtmp2_conn_t *conn, void *userdata)
{
    rtmp_bridge_t *bridge = (rtmp_bridge_t *)userdata;
    log_debug("RTMP: new connection");
    (void)conn; (void)bridge;
    return 0; /* accept */
}

static int on_publish(lrtmp2_conn_t *conn, const char *app, const char *stream_key, void *userdata)
{
    rtmp_bridge_t *bridge = (rtmp_bridge_t *)userdata;

    log_info("RTMP: publish request app='%s' key='%s'", app, stream_key);

    /* Validate against stream registry */
    const stream_entry_t *stream = stream_registry_find_by_key(bridge->registry, app, stream_key);
    if (!stream) {
        log_warn("RTMP: publish rejected — no matching stream for app='%s'", app);
        return -1; /* reject */
    }

    /* Create session entry */
    session_entry_t sess;
    memset(&sess, 0, sizeof(sess));
    snprintf(sess.id, sizeof(sess.id), "sess_%p", (void *)conn);
    strncpy(sess.stream_id, stream->id, sizeof(sess.stream_id) - 1);
    strncpy(sess.app, app, sizeof(sess.app) - 1);
    strncpy(sess.stream_key, stream_key, sizeof(sess.stream_key) - 1);
    sess.role = SESSION_PUBLISHER;
    sess.status = SESSION_ACTIVE;

    session_add(bridge->sessions, &sess);
    bridge->stats->active_publishers++;
    bridge->stats->active_sessions++;
    strncpy(bridge->stream_id, stream->id, sizeof(bridge->stream_id) - 1);

    log_info("RTMP: publish accepted stream='%s' session=%s", stream->id, sess.id);
    return 0;
}

static int on_play(lrtmp2_conn_t *conn, const char *app, const char *stream_key, void *userdata)
{
    (void)conn; (void)userdata;
    log_info("RTMP: play request app='%s' key='%s'", app, stream_key);
    /* For now accept all play requests. Could validate against registry too. */
    return 0;
}

static int on_frame(lrtmp2_conn_t *conn, const lrtmp2_frame_t *frame, void *userdata)
{
    rtmp_bridge_t *bridge = (rtmp_bridge_t *)userdata;

    const char *type_str = "?";
    switch (frame->type) {
        case LRTMP2_FRAME_AUDIO:    type_str = "AUDIO"; break;
        case LRTMP2_FRAME_VIDEO:    type_str = "VIDEO"; break;
        case LRTMP2_FRAME_SCRIPT:   type_str = "SCRIPT"; break;
        case LRTMP2_FRAME_METADATA: type_str = "METADATA"; break;
    }

    /* Update stats */
    bridge->stats->total_bytes_in += frame->size;
    char session_id[64];
    snprintf(session_id, sizeof(session_id), "sess_%p", (void *)conn);
    session_update_bytes(bridge->sessions, session_id, frame->size);

    log_debug("RTMP: frame %s ts=%u size=%u", type_str, frame->timestamp, frame->size);

    return 0;
}

static void on_close(lrtmp2_conn_t *conn, void *userdata)
{
    rtmp_bridge_t *bridge = (rtmp_bridge_t *)userdata;

    char session_id[64];
    snprintf(session_id, sizeof(session_id), "sess_%p", (void *)conn);

    session_entry_t *sess = session_get(bridge->sessions, session_id);
    if (sess) {
        if (sess->role == SESSION_PUBLISHER) bridge->stats->active_publishers--;
        bridge->stats->active_sessions--;
        session_remove(bridge->sessions, session_id);
    }
    log_debug("RTMP: connection closed session=%s", session_id);
}

/* Fill a librtmp2_server_config with our bridge callbacks */
void rtmp_bridge_setup(lrtmp2_server_config_t *config, rtmp_bridge_t *bridge,
                       stream_registry_t *registry, session_manager_t *sessions,
                       stats_collector_t *stats)
{
    memset(config, 0, sizeof(*config));
    memset(bridge, 0, sizeof(*bridge));
    bridge->registry = registry;
    bridge->sessions = sessions;
    bridge->stats    = stats;

    config->max_connections = 100;
    config->chunk_size      = 4096;
    config->on_connect_cb   = on_connect;
    config->on_publish_cb   = on_publish;
    config->on_play_cb      = on_play;
    config->on_frame_cb     = on_frame;
    config->on_close_cb     = on_close;
    config->userdata        = bridge;
}
