/**
 * db.h — SQLite persistence for streams, publishers, players and stats
 */
#ifndef LRTMP2_SERVER_DB_H
#define LRTMP2_SERVER_DB_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct db_context db_context_t;

/* --- Stream --- */
typedef struct {
    char id[64];
    char name[128];
    char app[64];
    char publish_key[128];   /* publisher stream key */
    char play_key[128];      /* player/viewer key */
    char stats_key[128];     /* stats page unique key */
    bool enabled;
    char allowed_codecs[256];
    time_t created_at;
} db_stream_t;

/* --- Active publisher --- */
typedef struct {
    char id[64];             /* unique publisher id = publish_key */
    char stream_id[64];
    char remote_addr[64];
    char app[64];
    char stream_name[128];   /* the stream_name from publish command */
    char video_codec[32];
    char audio_codec[32];
    uint32_t video_width;
    uint32_t video_height;
    double fps;
    uint64_t bytes_in;
    double bitrate_kbps;
    time_t connected_at;
    bool active;
} db_publisher_t;

/* --- Active player --- */
typedef struct {
    char id[64];             /* unique player id = play_key */
    char stream_id[64];
    char remote_addr[64];
    char app[64];
    char stream_name[128];
    uint64_t bytes_out;
    double bitrate_kbps;
    time_t connected_at;
    bool active;
} db_player_t;

/* --- Stats sample for history --- */
typedef struct {
    char stream_id[64];
    double bitrate_in_kbps;
    double fps;
    uint32_t width;
    uint32_t height;
    char video_codec[32];
    char audio_codec[32];
    int player_count;
    time_t ts;
} db_stat_sample_t;

/* --- Lifecycle --- */
db_context_t *db_open(const char *path);
void          db_close(db_context_t *db);

/* --- Streams --- */
bool db_stream_add(db_context_t *db, const db_stream_t *s);
bool db_stream_get(db_context_t *db, const char *id, db_stream_t *out);
bool db_stream_get_by_app(db_context_t *db, const char *app, const char *stream_name, db_stream_t *out);
bool db_stream_update(db_context_t *db, const char *id, const db_stream_t *s);
bool db_stream_delete(db_context_t *db, const char *id);
bool db_stream_list(db_context_t *db, db_stream_t **out_array, int *out_count);
bool db_stream_find_by_publish_key(db_context_t *db, const char *key, db_stream_t *out);
bool db_stream_find_by_play_key(db_context_t *db, const char *key, db_stream_t *out);
bool db_stream_find_by_stats_key(db_context_t *db, const char *key, db_stream_t *out);
void db_stream_free_list(db_stream_t *arr);

/* --- Publishers --- */
bool db_publisher_add(db_context_t *db, const db_publisher_t *p);
bool db_publisher_update(db_context_t *db, const char *id, const db_publisher_t *p);
bool db_publisher_remove(db_context_t *db, const char *id);
bool db_publisher_list(db_context_t *db, const char *stream_id, db_publisher_t **out, int *count);
bool db_publisher_list_all(db_context_t *db, db_publisher_t **out, int *count);
void db_publisher_free_list(db_publisher_t *arr);

/* --- Players --- */
bool db_player_add(db_context_t *db, const db_player_t *p);
bool db_player_update(db_context_t *db, const char *id, const db_player_t *p);
bool db_player_remove(db_context_t *db, const char *id);
bool db_player_list(db_context_t *db, const char *stream_id, db_player_t **out, int *count);
bool db_player_list_all(db_context_t *db, db_player_t **out, int *count);
void db_player_free_list(db_player_t *arr);

/* --- Stats samples --- */
bool db_stat_add(db_context_t *db, const db_stat_sample_t *s);
bool db_stat_recent(db_context_t *db, const char *stream_id, int limit, db_stat_sample_t **out, int *count);
void db_stat_free_list(db_stat_sample_t *arr);

#ifdef __cplusplus
}
#endif

#endif /* LRTMP2_SERVER_DB_H */
