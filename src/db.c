/**
 * db.c — SQLite persistence layer
 *
 * Stores streams, publishers, players and stats samples.
 * All access is thread-safe via SQLite's own locking + our own mutex.
 */
#include "librtmp2-server/db.h"
#include "librtmp2-server/logger.h"
#include <sqlite3.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <pthread.h>
#include <unistd.h>

struct db_context {
    sqlite3 *conn;
    pthread_mutex_t lock;
};

static int busy_handler(void *ud, int retries)
{
    (void)ud;
    if (retries < 10) {
        usleep(100000); /* 100ms */
        return 1; /* retry */
    }
    return 0;
}

db_context_t *db_open(const char *path)
{
    db_context_t *db = calloc(1, sizeof(db_context_t));
    if (!db) return NULL;

    pthread_mutex_init(&db->lock, NULL);

    int rc = sqlite3_open(path, &db->conn);
    if (rc != SQLITE_OK) {
        log_error("Cannot open database %s: %s", path, sqlite3_errmsg(db->conn));
        free(db);
        return NULL;
    }

    sqlite3_busy_handler(db->conn, busy_handler, NULL);
    sqlite3_exec(db->conn, "PRAGMA journal_mode=WAL;", NULL, NULL, NULL);
    sqlite3_exec(db->conn, "PRAGMA foreign_keys=ON;", NULL, NULL, NULL);

    /* Create tables */
    const char *sql =
        "CREATE TABLE IF NOT EXISTS streams ("
        "  id TEXT PRIMARY KEY,"
        "  name TEXT NOT NULL DEFAULT '',"
        "  app TEXT NOT NULL DEFAULT 'live',"
        "  publish_key TEXT UNIQUE NOT NULL,"
        "  play_key TEXT UNIQUE NOT NULL,"
        "  stats_key TEXT UNIQUE NOT NULL,"
        "  enabled INTEGER NOT NULL DEFAULT 1,"
        "  allowed_codecs TEXT NOT NULL DEFAULT 'avc1,hvc1,av01',"
        "  created_at INTEGER NOT NULL"
        ");"
        "CREATE TABLE IF NOT EXISTS publishers ("
        "  id TEXT PRIMARY KEY,"
        "  stream_id TEXT NOT NULL,"
        "  remote_addr TEXT NOT NULL DEFAULT '',"
        "  app TEXT NOT NULL DEFAULT '',"
        "  stream_name TEXT NOT NULL DEFAULT '',"
        "  video_codec TEXT NOT NULL DEFAULT '',"
        "  audio_codec TEXT NOT NULL DEFAULT '',"
        "  video_width INTEGER NOT NULL DEFAULT 0,"
        "  video_height INTEGER NOT NULL DEFAULT 0,"
        "  fps REAL NOT NULL DEFAULT 0,"
        "  bytes_in INTEGER NOT NULL DEFAULT 0,"
        "  bitrate_kbps REAL NOT NULL DEFAULT 0,"
        "  connected_at INTEGER NOT NULL,"
        "  active INTEGER NOT NULL DEFAULT 1"
        ");"
        "CREATE TABLE IF NOT EXISTS players ("
        "  id TEXT PRIMARY KEY,"
        "  stream_id TEXT NOT NULL,"
        "  remote_addr TEXT NOT NULL DEFAULT '',"
        "  app TEXT NOT NULL DEFAULT '',"
        "  stream_name TEXT NOT NULL DEFAULT '',"
        "  bytes_out INTEGER NOT NULL DEFAULT 0,"
        "  bitrate_kbps REAL NOT NULL DEFAULT 0,"
        "  connected_at INTEGER NOT NULL,"
        "  active INTEGER NOT NULL DEFAULT 1"
        ");"
        "CREATE TABLE IF NOT EXISTS stats_samples ("
        "  id INTEGER PRIMARY KEY AUTOINCREMENT,"
        "  stream_id TEXT NOT NULL,"
        "  bitrate_in_kbps REAL NOT NULL DEFAULT 0,"
        "  fps REAL NOT NULL DEFAULT 0,"
        "  width INTEGER NOT NULL DEFAULT 0,"
        "  height INTEGER NOT NULL DEFAULT 0,"
        "  video_codec TEXT NOT NULL DEFAULT '',"
        "  audio_codec TEXT NOT NULL DEFAULT '',"
        "  player_count INTEGER NOT NULL DEFAULT 0,"
        "  ts INTEGER NOT NULL"
        ");"
        "CREATE INDEX IF NOT EXISTS idx_pub_stream ON publishers(stream_id);"
        "CREATE INDEX IF NOT EXISTS idx_player_stream ON players(stream_id);"
        "CREATE INDEX IF NOT EXISTS idx_stats_stream ON stats_samples(stream_id);"
        "CREATE INDEX IF NOT EXISTS idx_pub_active ON publishers(active);"
        "CREATE INDEX IF NOT EXISTS idx_player_active ON players(active);";

    char *err = NULL;
    rc = sqlite3_exec(db->conn, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        log_error("DB schema error: %s", err);
        sqlite3_free(err);
        db_close(db);
        return NULL;
    }

    log_info("Database opened: %s", path);
    return db;
}

void db_close(db_context_t *db)
{
    if (!db) return;
    if (db->conn) sqlite3_close(db->conn);
    pthread_mutex_destroy(&db->lock);
    free(db);
}

/* --- helper: prepare & step --- */

static int exec_simple(db_context_t *db, const char *sql)
{
    pthread_mutex_lock(&db->lock);
    char *err = NULL;
    int rc = sqlite3_exec(db->conn, sql, NULL, NULL, &err);
    if (rc != SQLITE_OK) {
        log_error("DB error: %s -- %s", err, sql);
        sqlite3_free(err);
    }
    pthread_mutex_unlock(&db->lock);
    return rc;
}

/* Copy a TEXT column into a fixed buffer. Always NUL-terminates (strncpy alone
 * leaves dst unterminated when the value fills dstlen-1 bytes). Handles NULL
 * columns without passing NULL to strncpy. */
static void db_col_text(char *dst, size_t dstlen, sqlite3_stmt *stmt, int col)
{
    if (dstlen == 0) return;
    const unsigned char *text = sqlite3_column_text(stmt, col);
    if (!text) {
        dst[0] = '\0';
        return;
    }
    size_t n = dstlen - 1;
    strncpy(dst, (const char *)text, n);
    dst[n] = '\0';
}

/* ==================== STREAMS ==================== */

bool db_stream_add(db_context_t *db, const db_stream_t *s)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db->conn,
        "INSERT INTO streams (id,name,app,publish_key,play_key,stats_key,enabled,allowed_codecs,created_at) "
        "VALUES (?,?,?,?,?,?,?,?,?)", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, s->id, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, s->name, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, s->app, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, s->publish_key, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 5, s->play_key, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 6, s->stats_key, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 7, s->enabled ? 1 : 0);
    sqlite3_bind_text(stmt, 8, s->allowed_codecs, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 9, (int64_t)s->created_at);

    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&db->lock);

    if (rc == SQLITE_DONE) {
        log_info("Stream added: id=%s app=%s", s->id, s->app);
        return true;
    }
    log_error("Failed to add stream %s: %d", s->id, rc);
    return false;
}

static bool db_stream_load_row(sqlite3_stmt *stmt, db_stream_t *out)
{
    db_col_text(out->id, sizeof(out->id), stmt, 0);
    db_col_text(out->name, sizeof(out->name), stmt, 1);
    db_col_text(out->app, sizeof(out->app), stmt, 2);
    db_col_text(out->publish_key, sizeof(out->publish_key), stmt, 3);
    db_col_text(out->play_key, sizeof(out->play_key), stmt, 4);
    db_col_text(out->stats_key, sizeof(out->stats_key), stmt, 5);
    out->enabled = sqlite3_column_int(stmt, 6) != 0;
    db_col_text(out->allowed_codecs, sizeof(out->allowed_codecs), stmt, 7);
    out->created_at = (time_t)sqlite3_column_int64(stmt, 8);
    return true;
}

bool db_stream_get(db_context_t *db, const char *id, db_stream_t *out)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db->conn, "SELECT id,name,app,publish_key,play_key,stats_key,enabled,allowed_codecs,created_at FROM streams WHERE id=?", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, id, -1, SQLITE_STATIC);
    bool found = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        db_stream_load_row(stmt, out);
        found = true;
    }
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&db->lock);
    return found;
}

bool db_stream_get_by_app(db_context_t *db, const char *app, const char *stream_name, db_stream_t *out)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db->conn,
        "SELECT id,name,app,publish_key,play_key,stats_key,enabled,allowed_codecs,created_at "
        "FROM streams WHERE app=? AND name=?", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, app, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, stream_name, -1, SQLITE_STATIC);
    bool found = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        db_stream_load_row(stmt, out);
        found = true;
    }
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&db->lock);
    return found;
}

bool db_stream_find_by_publish_key(db_context_t *db, const char *key, db_stream_t *out)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db->conn,
        "SELECT id,name,app,publish_key,play_key,stats_key,enabled,allowed_codecs,created_at "
        "FROM streams WHERE publish_key=? AND enabled=1", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    bool found = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        db_stream_load_row(stmt, out);
        found = true;
    }
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&db->lock);
    return found;
}

bool db_stream_find_by_play_key(db_context_t *db, const char *key, db_stream_t *out)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db->conn,
        "SELECT id,name,app,publish_key,play_key,stats_key,enabled,allowed_codecs,created_at "
        "FROM streams WHERE play_key=? AND enabled=1", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    bool found = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        db_stream_load_row(stmt, out);
        found = true;
    }
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&db->lock);
    return found;
}

bool db_stream_find_by_stats_key(db_context_t *db, const char *key, db_stream_t *out)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db->conn,
        "SELECT id,name,app,publish_key,play_key,stats_key,enabled,allowed_codecs,created_at "
        "FROM streams WHERE stats_key=? AND enabled=1", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, key, -1, SQLITE_STATIC);
    bool found = false;
    if (sqlite3_step(stmt) == SQLITE_ROW) {
        db_stream_load_row(stmt, out);
        found = true;
    }
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&db->lock);
    return found;
}

bool db_stream_update(db_context_t *db, const char *id, const db_stream_t *s)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db->conn,
        "UPDATE streams SET name=?,app=?,publish_key=?,play_key=?,stats_key=?,enabled=?,allowed_codecs=? WHERE id=?",
        -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, s->name, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, s->app, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, s->publish_key, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, s->play_key, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 5, s->stats_key, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 6, s->enabled ? 1 : 0);
    sqlite3_bind_text(stmt, 7, s->allowed_codecs, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 8, id, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&db->lock);
    return rc == SQLITE_DONE;
}

/* Run one bound DELETE inside the already-open transaction. Returns true on
 * SQLITE_DONE, false on a prepare or step failure so the caller can roll
 * back instead of leaving a partially cascaded delete. */
static bool db_exec_delete_by_stream_id(db_context_t *db, const char *sql, const char *id)
{
    sqlite3_stmt *stmt;
    if (sqlite3_prepare_v2(db->conn, sql, -1, &stmt, NULL) != SQLITE_OK) return false;
    sqlite3_bind_text(stmt, 1, id, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    return rc == SQLITE_DONE;
}

bool db_stream_delete(db_context_t *db, const char *id)
{
    pthread_mutex_lock(&db->lock);

    /* Cascade: remove dependent rows so deleted streams cannot leave ghost
     * active publishers/players that pollute stats after stream re-creation.
     * Wrapped in a transaction so a failure partway through (e.g. SQLITE_BUSY)
     * cannot leave the cascade half-applied. */
    char *err = NULL;
    if (sqlite3_exec(db->conn, "BEGIN IMMEDIATE", NULL, NULL, &err) != SQLITE_OK) {
        log_error("DB error starting cascade delete transaction: %s", err);
        sqlite3_free(err);
        pthread_mutex_unlock(&db->lock);
        return false;
    }

    bool ok = db_exec_delete_by_stream_id(db, "DELETE FROM publishers WHERE stream_id=?", id) &&
              db_exec_delete_by_stream_id(db, "DELETE FROM players WHERE stream_id=?", id) &&
              db_exec_delete_by_stream_id(db, "DELETE FROM stats_samples WHERE stream_id=?", id) &&
              db_exec_delete_by_stream_id(db, "DELETE FROM streams WHERE id=?", id);

    if (ok) {
        if (sqlite3_exec(db->conn, "COMMIT", NULL, NULL, &err) != SQLITE_OK) {
            log_error("DB error committing cascade delete: %s", err);
            sqlite3_free(err);
            ok = false;
        }
    }
    if (!ok) {
        sqlite3_exec(db->conn, "ROLLBACK", NULL, NULL, NULL);
    }

    pthread_mutex_unlock(&db->lock);
    return ok;
}

bool db_stream_list(db_context_t *db, db_stream_t **out_array, int *out_count)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *count_stmt;
    sqlite3_prepare_v2(db->conn, "SELECT COUNT(*) FROM streams", -1, &count_stmt, NULL);
    int total = 0;
    if (sqlite3_step(count_stmt) == SQLITE_ROW) total = sqlite3_column_int(count_stmt, 0);
    sqlite3_finalize(count_stmt);

    if (total == 0) {
        *out_array = NULL;
        *out_count = 0;
        pthread_mutex_unlock(&db->lock);
        return true;
    }

    db_stream_t *arr = calloc(total, sizeof(db_stream_t));
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db->conn,
        "SELECT id,name,app,publish_key,play_key,stats_key,enabled,allowed_codecs,created_at FROM streams ORDER BY created_at",
        -1, &stmt, NULL);
    int i = 0;
    while (sqlite3_step(stmt) == SQLITE_ROW && i < total) {
        db_stream_load_row(stmt, &arr[i++]);
    }
    sqlite3_finalize(stmt);
    *out_array = arr;
    *out_count = i;
    pthread_mutex_unlock(&db->lock);
    return true;
}

void db_stream_free_list(db_stream_t *arr) { free(arr); }

/* ==================== PUBLISHERS ==================== */

bool db_publisher_add(db_context_t *db, const db_publisher_t *p)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db->conn,
        "INSERT OR REPLACE INTO publishers "
        "(id,stream_id,remote_addr,app,stream_name,video_codec,audio_codec,video_width,video_height,fps,bytes_in,bitrate_kbps,connected_at,active) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1)",
        -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, p->id, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, p->stream_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, p->remote_addr, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, p->app, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 5, p->stream_name, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 6, p->video_codec, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 7, p->audio_codec, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 8, p->video_width);
    sqlite3_bind_int(stmt, 9, p->video_height);
    sqlite3_bind_double(stmt, 10, p->fps);
    sqlite3_bind_int64(stmt, 11, p->bytes_in);
    sqlite3_bind_double(stmt, 12, p->bitrate_kbps);
    sqlite3_bind_int64(stmt, 13, (int64_t)p->connected_at);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&db->lock);
    return rc == SQLITE_DONE;
}

bool db_publisher_update(db_context_t *db, const char *id, const db_publisher_t *p)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db->conn,
        "UPDATE publishers SET stream_id=?,remote_addr=?,app=?,stream_name=?,"
        "video_codec=?,audio_codec=?,video_width=?,video_height=?,fps=?,"
        "bytes_in=?,bitrate_kbps=?,active=? WHERE id=?",
        -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, p->stream_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, p->remote_addr, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, p->app, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, p->stream_name, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 5, p->video_codec, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 6, p->audio_codec, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 7, p->video_width);
    sqlite3_bind_int(stmt, 8, p->video_height);
    sqlite3_bind_double(stmt, 9, p->fps);
    sqlite3_bind_int64(stmt, 10, p->bytes_in);
    sqlite3_bind_double(stmt, 11, p->bitrate_kbps);
    sqlite3_bind_int(stmt, 12, p->active ? 1 : 0);
    sqlite3_bind_text(stmt, 13, id, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&db->lock);
    return rc == SQLITE_DONE;
}

bool db_publisher_remove(db_context_t *db, const char *id)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db->conn, "DELETE FROM publishers WHERE id=?", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, id, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&db->lock);
    return rc == SQLITE_DONE;
}

static void load_publisher_row(sqlite3_stmt *stmt, db_publisher_t *out)
{
    db_col_text(out->id, sizeof(out->id), stmt, 0);
    db_col_text(out->stream_id, sizeof(out->stream_id), stmt, 1);
    db_col_text(out->remote_addr, sizeof(out->remote_addr), stmt, 2);
    db_col_text(out->app, sizeof(out->app), stmt, 3);
    db_col_text(out->stream_name, sizeof(out->stream_name), stmt, 4);
    db_col_text(out->video_codec, sizeof(out->video_codec), stmt, 5);
    db_col_text(out->audio_codec, sizeof(out->audio_codec), stmt, 6);
    out->video_width = (uint32_t)sqlite3_column_int(stmt, 7);
    out->video_height = (uint32_t)sqlite3_column_int(stmt, 8);
    out->fps = sqlite3_column_double(stmt, 9);
    out->bytes_in = (uint64_t)sqlite3_column_int64(stmt, 10);
    out->bitrate_kbps = sqlite3_column_double(stmt, 11);
    out->connected_at = (time_t)sqlite3_column_int64(stmt, 12);
    out->active = sqlite3_column_int(stmt, 13) != 0;
}

bool db_publisher_list(db_context_t *db, const char *stream_id, db_publisher_t **out, int *count)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    if (stream_id) {
        sqlite3_prepare_v2(db->conn,
            "SELECT id,stream_id,remote_addr,app,stream_name,video_codec,audio_codec,video_width,video_height,fps,bytes_in,bitrate_kbps,connected_at,active "
            "FROM publishers WHERE stream_id=? AND active=1", -1, &stmt, NULL);
        sqlite3_bind_text(stmt, 1, stream_id, -1, SQLITE_STATIC);
    } else {
        sqlite3_prepare_v2(db->conn,
            "SELECT id,stream_id,remote_addr,app,stream_name,video_codec,audio_codec,video_width,video_height,fps,bytes_in,bitrate_kbps,connected_at,active "
            "FROM publishers WHERE active=1", -1, &stmt, NULL);
    }
    int cap = 64, n = 0;
    db_publisher_t *arr = calloc(cap, sizeof(db_publisher_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) { cap *= 2; arr = realloc(arr, cap * sizeof(db_publisher_t)); }
        load_publisher_row(stmt, &arr[n++]);
    }
    sqlite3_finalize(stmt);
    *out = arr;
    *count = n;
    pthread_mutex_unlock(&db->lock);
    return true;
}

bool db_publisher_list_all(db_context_t *db, db_publisher_t **out, int *count)
{
    return db_publisher_list(db, NULL, out, count);
}

void db_publisher_free_list(db_publisher_t *arr) { free(arr); }

/* ==================== PLAYERS ==================== */

bool db_player_add(db_context_t *db, const db_player_t *p)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db->conn,
        "INSERT OR REPLACE INTO players "
        "(id,stream_id,remote_addr,app,stream_name,bytes_out,bitrate_kbps,connected_at,active) "
        "VALUES (?,?,?,?,?,?,?,?,1)",
        -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, p->id, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, p->stream_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, p->remote_addr, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, p->app, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 5, p->stream_name, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 6, p->bytes_out);
    sqlite3_bind_double(stmt, 7, p->bitrate_kbps);
    sqlite3_bind_int64(stmt, 8, (int64_t)p->connected_at);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&db->lock);
    return rc == SQLITE_DONE;
}

bool db_player_update(db_context_t *db, const char *id, const db_player_t *p)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db->conn,
        "UPDATE players SET stream_id=?,remote_addr=?,app=?,stream_name=?,"
        "bytes_out=?,bitrate_kbps=?,active=? WHERE id=?",
        -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, p->stream_id, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 2, p->remote_addr, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 3, p->app, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 4, p->stream_name, -1, SQLITE_STATIC);
    sqlite3_bind_int64(stmt, 5, p->bytes_out);
    sqlite3_bind_double(stmt, 6, p->bitrate_kbps);
    sqlite3_bind_int(stmt, 7, p->active ? 1 : 0);
    sqlite3_bind_text(stmt, 8, id, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&db->lock);
    return rc == SQLITE_DONE;
}

bool db_player_remove(db_context_t *db, const char *id)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db->conn, "DELETE FROM players WHERE id=?", -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, id, -1, SQLITE_STATIC);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&db->lock);
    return rc == SQLITE_DONE;
}

static void load_player_row(sqlite3_stmt *stmt, db_player_t *out)
{
    db_col_text(out->id, sizeof(out->id), stmt, 0);
    db_col_text(out->stream_id, sizeof(out->stream_id), stmt, 1);
    db_col_text(out->remote_addr, sizeof(out->remote_addr), stmt, 2);
    db_col_text(out->app, sizeof(out->app), stmt, 3);
    db_col_text(out->stream_name, sizeof(out->stream_name), stmt, 4);
    out->bytes_out = (uint64_t)sqlite3_column_int64(stmt, 5);
    out->bitrate_kbps = sqlite3_column_double(stmt, 6);
    out->connected_at = (time_t)sqlite3_column_int64(stmt, 7);
    out->active = sqlite3_column_int(stmt, 8) != 0;
}

bool db_player_list(db_context_t *db, const char *stream_id, db_player_t **out, int *count)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    if (stream_id) {
        sqlite3_prepare_v2(db->conn,
            "SELECT id,stream_id,remote_addr,app,stream_name,bytes_out,bitrate_kbps,connected_at,active "
            "FROM players WHERE stream_id=? AND active=1", -1, &stmt, NULL);
        sqlite3_bind_text(stmt, 1, stream_id, -1, SQLITE_STATIC);
    } else {
        sqlite3_prepare_v2(db->conn,
            "SELECT id,stream_id,remote_addr,app,stream_name,bytes_out,bitrate_kbps,connected_at,active "
            "FROM players WHERE active=1", -1, &stmt, NULL);
    }
    int cap = 64, n = 0;
    db_player_t *arr = calloc(cap, sizeof(db_player_t));
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (n >= cap) { cap *= 2; arr = realloc(arr, cap * sizeof(db_player_t)); }
        load_player_row(stmt, &arr[n++]);
    }
    sqlite3_finalize(stmt);
    *out = arr;
    *count = n;
    pthread_mutex_unlock(&db->lock);
    return true;
}

bool db_player_list_all(db_context_t *db, db_player_t **out, int *count)
{
    return db_player_list(db, NULL, out, count);
}

void db_player_free_list(db_player_t *arr) { free(arr); }

/* ==================== STATS SAMPLES ==================== */

bool db_stat_add(db_context_t *db, const db_stat_sample_t *s)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db->conn,
        "INSERT INTO stats_samples "
        "(stream_id,bitrate_in_kbps,fps,width,height,video_codec,audio_codec,player_count,ts) "
        "VALUES (?,?,?,?,?,?,?,?,?)",
        -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, s->stream_id, -1, SQLITE_STATIC);
    sqlite3_bind_double(stmt, 2, s->bitrate_in_kbps);
    sqlite3_bind_double(stmt, 3, s->fps);
    sqlite3_bind_int(stmt, 4, s->width);
    sqlite3_bind_int(stmt, 5, s->height);
    sqlite3_bind_text(stmt, 6, s->video_codec, -1, SQLITE_STATIC);
    sqlite3_bind_text(stmt, 7, s->audio_codec, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 8, s->player_count);
    sqlite3_bind_int64(stmt, 9, (int64_t)s->ts);
    int rc = sqlite3_step(stmt);
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&db->lock);
    return rc == SQLITE_DONE;
}

bool db_stat_recent(db_context_t *db, const char *stream_id, int limit, db_stat_sample_t **out, int *count)
{
    pthread_mutex_lock(&db->lock);
    sqlite3_stmt *stmt;
    sqlite3_prepare_v2(db->conn,
        "SELECT stream_id,bitrate_in_kbps,fps,width,height,video_codec,audio_codec,player_count,ts "
        "FROM stats_samples WHERE stream_id=? ORDER BY ts DESC LIMIT ?",
        -1, &stmt, NULL);
    sqlite3_bind_text(stmt, 1, stream_id, -1, SQLITE_STATIC);
    sqlite3_bind_int(stmt, 2, limit);
    int cap = limit > 0 ? limit : 64, n = 0;
    db_stat_sample_t *arr = calloc(cap, sizeof(db_stat_sample_t));
    if (!arr) {
        sqlite3_finalize(stmt);
        pthread_mutex_unlock(&db->lock);
        return false;
    }
    while (sqlite3_step(stmt) == SQLITE_ROW && n < cap) {
        db_col_text(arr[n].stream_id, sizeof(arr[n].stream_id), stmt, 0);
        arr[n].bitrate_in_kbps = sqlite3_column_double(stmt, 1);
        arr[n].fps = sqlite3_column_double(stmt, 2);
        arr[n].width = (uint32_t)sqlite3_column_int(stmt, 3);
        arr[n].height = (uint32_t)sqlite3_column_int(stmt, 4);
        db_col_text(arr[n].video_codec, sizeof(arr[n].video_codec), stmt, 5);
        db_col_text(arr[n].audio_codec, sizeof(arr[n].audio_codec), stmt, 6);
        arr[n].player_count = sqlite3_column_int(stmt, 7);
        arr[n].ts = (time_t)sqlite3_column_int64(stmt, 8);
        n++;
    }
    sqlite3_finalize(stmt);
    *out = arr;
    *count = n;
    pthread_mutex_unlock(&db->lock);
    return true;
}

void db_stat_free_list(db_stat_sample_t *arr) { free(arr); }
