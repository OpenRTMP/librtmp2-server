/**
 * rtmp_callbacks.c — Bridge librtmp2 callbacks to SQLite-backed server
 *
 * On publish: validate publish_key, insert publisher row in DB
 * On play: validate play_key, insert player row in DB
 * On frame: update publisher stats (bitrate, codec, bytes)
 * On close: mark publisher/player as inactive in DB
 */
#include "librtmp2-server/server.h"
#include "librtmp2-server/db.h"
#include "librtmp2-server/logger.h"
#include "librtmp2/librtmp2.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

/* Per-connection state, keyed by the conn pointer. The librtmp2 server
 * config carries a single userdata pointer shared across every connection
 * (lrtmp2_server_config_t has one userdata slot, not one per conn), so
 * publish/play/close state cannot live directly on rtmp_bridge_t — with two
 * connections active (e.g. a publisher and a player), each on_publish/
 * on_play call would clobber the other connection's tracked key, and
 * on_close would act on whichever connection's data happened to be there. */
typedef struct conn_state {
    lrtmp2_conn_t      *conn;
    int                 is_publisher;
    int                 is_player;
    db_publisher_t      pub;
    db_player_t         player;
    struct conn_state  *next;
} conn_state_t;

typedef struct {
    db_context_t  *db;
    conn_state_t  *conns;
} rtmp_bridge_t;

static conn_state_t *conn_state_find(rtmp_bridge_t *bridge, lrtmp2_conn_t *conn)
{
    for (conn_state_t *cs = bridge->conns; cs; cs = cs->next) {
        if (cs->conn == conn) return cs;
    }
    return NULL;
}

/* Get remote address string from connection's client_fd */
static void get_remote_addr(int fd, char *out, size_t outlen)
{
    struct sockaddr_storage addr;
    socklen_t len = sizeof(addr);
    if (getpeername(fd, (struct sockaddr *)&addr, &len) == 0) {
        if (addr.ss_family == AF_INET) {
            struct sockaddr_in *sin = (struct sockaddr_in *)&addr;
            snprintf(out, outlen, "%s:%d",
                inet_ntoa(sin->sin_addr), ntohs(sin->sin_port));
        } else if (addr.ss_family == AF_INET6) {
            struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)&addr;
            char ip[INET6_ADDRSTRLEN];
            inet_ntop(AF_INET6, &sin6->sin6_addr, ip, sizeof(ip));
            snprintf(out, outlen, "[%s]:%d", ip, ntohs(sin6->sin6_port));
        }
    } else {
        snprintf(out, outlen, "unknown");
    }
}

static int on_connect(lrtmp2_conn_t *conn, void *userdata)
{
    rtmp_bridge_t *bridge = (rtmp_bridge_t *)userdata;

    conn_state_t *cs = calloc(1, sizeof(*cs));
    if (!cs) {
        log_error("RTMP: failed to allocate per-connection state");
        return -1;
    }
    cs->conn = conn;
    cs->next = bridge->conns;
    bridge->conns = cs;

    log_debug("RTMP: new connection");
    return 0; /* accept all, auth happens on publish/play */
}

static int on_publish(lrtmp2_conn_t *conn, const char *app, const char *stream_key, void *userdata)
{
    rtmp_bridge_t *bridge = (rtmp_bridge_t *)userdata;
    conn_state_t *cs = conn_state_find(bridge, conn);

    log_info("RTMP: publish request app='%s' key='%s'", app, stream_key);

    /* Validate publish_key against DB */
    db_stream_t stream;
    if (!db_stream_find_by_publish_key(bridge->db, stream_key, &stream)) {
        log_warn("RTMP: publish rejected — invalid publish_key for app='%s'", app);
        return -1; /* reject */
    }

    /* Insert publisher into DB */
    db_publisher_t pub;
    memset(&pub, 0, sizeof(pub));
    snprintf(pub.id, sizeof(pub.id), "pub_%ld_%08x", (long)time(NULL), rand() & 0xFFFFFFFF);
    strncpy(pub.stream_id, stream.id, sizeof(pub.stream_id) - 1);
    strncpy(pub.app, app, sizeof(pub.app) - 1);
    strncpy(pub.stream_name, stream.name, sizeof(pub.stream_name) - 1);
    pub.active = true;
    pub.connected_at = time(NULL);

    /* Get remote addr from connection.
     * librtmp2 stores the client_fd in the connection struct; since we
     * don't have a public accessor, we use the client_fd via the
     * on_connect/on_publish callback's conn->client_fd. */
    get_remote_addr(lrtmp2_conn_get_fd(conn), pub.remote_addr, sizeof(pub.remote_addr));

    db_publisher_add(bridge->db, &pub);

    if (cs) {
        cs->pub = pub;
        cs->is_publisher = 1;
    }

    log_info("RTMP: publish accepted stream='%s' publisher=%s", stream.id, pub.id);
    return 0;
}

static int on_play(lrtmp2_conn_t *conn, const char *app, const char *stream_key, void *userdata)
{
    rtmp_bridge_t *bridge = (rtmp_bridge_t *)userdata;
    conn_state_t *cs = conn_state_find(bridge, conn);

    log_info("RTMP: play request app='%s' key='%s'", app, stream_key);

    /* Validate play_key against DB */
    db_stream_t stream;
    if (!db_stream_find_by_play_key(bridge->db, stream_key, &stream)) {
        log_warn("RTMP: play rejected — invalid play_key for app='%s'", app);
        return -1; /* reject */
    }

    /* Insert player into DB */
    db_player_t player;
    memset(&player, 0, sizeof(player));
    snprintf(player.id, sizeof(player.id), "pl_%ld_%08x", (long)time(NULL), rand() & 0xFFFFFFFF);
    strncpy(player.stream_id, stream.id, sizeof(player.stream_id) - 1);
    strncpy(player.app, app, sizeof(player.app) - 1);
    strncpy(player.stream_name, stream.name, sizeof(player.stream_name) - 1);
    player.active = true;
    player.connected_at = time(NULL);

    get_remote_addr(lrtmp2_conn_get_fd(conn), player.remote_addr, sizeof(player.remote_addr));

    db_player_add(bridge->db, &player);

    if (cs) {
        cs->player = player;
        cs->is_player = 1;
    }

    log_info("RTMP: play accepted stream='%s' player=%s", stream.id, player.id);
    return 0;
}

static int on_frame(lrtmp2_conn_t *conn, const lrtmp2_frame_t *frame, void *userdata)
{
    (void)conn; (void)userdata;

    if (frame->type == LRTMP2_FRAME_VIDEO) {
        log_debug("RTMP: VIDEO frame ts=%u size=%u codec=%s",
                  frame->timestamp, frame->size,
                  frame->video_fourcc.cc[0] ? frame->video_fourcc.cc : "legacy");
    } else if (frame->type == LRTMP2_FRAME_AUDIO) {
        log_debug("RTMP: AUDIO frame ts=%u size=%u codec=%s",
                  frame->timestamp, frame->size,
                  frame->audio_fourcc.cc[0] ? frame->audio_fourcc.cc : "legacy");
    }

    /* TODO: update publisher stats in DB (bitrate, codec, fps) */
    return 0;
}

static void on_close(lrtmp2_conn_t *conn, void *userdata)
{
    rtmp_bridge_t *bridge = (rtmp_bridge_t *)userdata;

    /* Unlink this connection's state from the bridge first — the row data
     * (and which role(s) it held) lives entirely in cs, captured at
     * publish/play time, so closing never touches another connection. */
    conn_state_t **link = &bridge->conns;
    conn_state_t *cs = NULL;
    while (*link) {
        if ((*link)->conn == conn) {
            cs = *link;
            *link = cs->next;
            break;
        }
        link = &(*link)->next;
    }
    if (!cs) return;

    if (cs->is_publisher) {
        cs->pub.active = false;
        db_publisher_update(bridge->db, cs->pub.id, &cs->pub);
        log_info("RTMP: publisher disconnected: stream=%s id=%s",
                 cs->pub.stream_id, cs->pub.id);
    }

    if (cs->is_player) {
        cs->player.active = false;
        db_player_update(bridge->db, cs->player.id, &cs->player);
        log_info("RTMP: player disconnected: stream=%s id=%s",
                 cs->player.stream_id, cs->player.id);
    }

    free(cs);
}

/* Setup librtmp2 server config with our bridge */
void rtmp_bridge_setup(lrtmp2_server_config_t *config, rtmp_bridge_t *bridge,
                       db_context_t *db)
{
    memset(config, 0, sizeof(*config));
    memset(bridge, 0, sizeof(*bridge));
    bridge->db = db;

    config->max_connections = 100;
    config->chunk_size      = 4096;
    config->on_connect_cb   = on_connect;
    config->on_publish_cb   = on_publish;
    config->on_play_cb      = on_play;
    config->on_frame_cb     = on_frame;
    config->on_close_cb     = on_close;
    config->userdata        = bridge;
}
