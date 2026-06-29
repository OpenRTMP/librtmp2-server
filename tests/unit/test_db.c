/**
 * test_db.c — SQLite database layer tests
 */
#include "librtmp2-server/db.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>

static int fail(const char *test, const char *reason)
{
    printf("  FAIL: %s — %s\n", test, reason);
    return 1;
}

static int pass(const char *test)
{
    printf("  PASS: %s\n", test);
    return 0;
}

int test_db_main(void)
{
    int errors = 0;
    const char *tmp = "/tmp/librtmp2_server_test.db";
    unlink(tmp);

    /* Open */
    db_context_t *db = db_open(tmp);
    if (!db) return fail("db_open", "returned NULL");
    pass("db_open");

    /* Add stream */
    {
        db_stream_t s;
        memset(&s, 0, sizeof(s));
        strncpy(s.id, "stream1", sizeof(s.id));
        strncpy(s.name, "Test Stream", sizeof(s.name));
        strncpy(s.app, "live", sizeof(s.app));
        strncpy(s.publish_key, "pub_key_123", sizeof(s.publish_key));
        strncpy(s.play_key, "pl_key_456", sizeof(s.play_key));
        strncpy(s.stats_key, "st_key_789", sizeof(s.stats_key));
        s.enabled = true;
        s.created_at = time(NULL);

        if (!db_stream_add(db, &s))
            errors += fail("db_stream_add", "failed");
        else
            pass("db_stream_add");
    }

    /* Get stream */
    {
        db_stream_t s;
        if (!db_stream_get(db, "stream1", &s))
            errors += fail("db_stream_get", "not found");
        else if (strcmp(s.name, "Test Stream") != 0)
            errors += fail("db_stream_get", "wrong name");
        else
            pass("db_stream_get");
    }

    /* Find by publish key */
    {
        db_stream_t s;
        if (!db_stream_find_by_publish_key(db, "pub_key_123", &s))
            errors += fail("db_stream_find_by_publish_key", "not found");
        else if (strcmp(s.id, "stream1") != 0)
            errors += fail("db_stream_find_by_publish_key", "wrong id");
        else
            pass("db_stream_find_by_publish_key");
    }

    /* Find by play key */
    {
        db_stream_t s;
        if (!db_stream_find_by_play_key(db, "pl_key_456", &s))
            errors += fail("db_stream_find_by_play_key", "not found");
        else
            pass("db_stream_find_by_play_key");
    }

    /* Find by stats key */
    {
        db_stream_t s;
        if (!db_stream_find_by_stats_key(db, "st_key_789", &s))
            errors += fail("db_stream_find_by_stats_key", "not found");
        else
            pass("db_stream_find_by_stats_key");
    }

    /* Wrong key rejected */
    {
        db_stream_t s;
        if (db_stream_find_by_stats_key(db, "wrong_key", &s))
            errors += fail("db_stream_find_by_stats_key", "should reject wrong key");
        else
            pass("db_stream_find_by_stats_key rejects wrong key");
    }

    /* List streams */
    {
        db_stream_t *arr = NULL;
        int count = 0;
        if (!db_stream_list(db, &arr, &count))
            errors += fail("db_stream_list", "failed");
        else if (count != 1)
            errors += fail("db_stream_list", "wrong count");
        else
            pass("db_stream_list");
        db_stream_free_list(arr);
    }

    /* Add publisher */
    {
        db_publisher_t p;
        memset(&p, 0, sizeof(p));
        strncpy(p.id, "pub1", sizeof(p.id));
        strncpy(p.stream_id, "stream1", sizeof(p.stream_id));
        strncpy(p.remote_addr, "127.0.0.1:54321", sizeof(p.remote_addr));
        strncpy(p.app, "live", sizeof(p.app));
        strncpy(p.stream_name, "test", sizeof(p.stream_name));
        strncpy(p.video_codec, "h264", sizeof(p.video_codec));
        strncpy(p.audio_codec, "aac", sizeof(p.audio_codec));
        p.video_width = 1920;
        p.video_height = 1080;
        p.fps = 60.0;
        p.bytes_in = 1024768;
        p.bitrate_kbps = 2500.0;
        p.active = true;
        p.connected_at = time(NULL);

        if (!db_publisher_add(db, &p))
            errors += fail("db_publisher_add", "failed");
        else
            pass("db_publisher_add");
    }

    /* List publishers by stream */
    {
        db_publisher_t *arr = NULL;
        int count = 0;
        if (!db_publisher_list(db, "stream1", &arr, &count))
            errors += fail("db_publisher_list", "failed");
        else if (count != 1)
            errors += fail("db_publisher_list", "wrong count");
        else
            pass("db_publisher_list");
        db_publisher_free_list(arr);
    }

    /* Add player */
    {
        db_player_t p;
        memset(&p, 0, sizeof(p));
        strncpy(p.id, "pl1", sizeof(p.id));
        strncpy(p.stream_id, "stream1", sizeof(p.stream_id));
        strncpy(p.remote_addr, "10.0.0.1:12345", sizeof(p.remote_addr));
        strncpy(p.app, "live", sizeof(p.app));
        strncpy(p.stream_name, "test", sizeof(p.stream_name));
        p.bytes_out = 512000;
        p.bitrate_kbps = 2400.0;
        p.active = true;
        p.connected_at = time(NULL);

        if (!db_player_add(db, &p))
            errors += fail("db_player_add", "failed");
        else
            pass("db_player_add");
    }

    /* List players by stream */
    {
        db_player_t *arr = NULL;
        int count = 0;
        if (!db_player_list(db, "stream1", &arr, &count))
            errors += fail("db_player_list", "failed");
        else if (count != 1)
            errors += fail("db_player_list", "wrong count");
        else
            pass("db_player_list");
        db_player_free_list(arr);
    }

    /* Stats sample */
    {
        db_stat_sample_t s;
        memset(&s, 0, sizeof(s));
        strncpy(s.stream_id, "stream1", sizeof(s.stream_id));
        s.bitrate_in_kbps = 2500.0;
        s.fps = 60.0;
        s.width = 1920;
        s.height = 1080;
        strncpy(s.video_codec, "h264", sizeof(s.video_codec));
        strncpy(s.audio_codec, "aac", sizeof(s.audio_codec));
        s.player_count = 1;
        s.ts = time(NULL);

        if (!db_stat_add(db, &s))
            errors += fail("db_stat_add", "failed");
        else
            pass("db_stat_add");
    }

    /* Recent stats */
    {
        db_stat_sample_t *arr = NULL;
        int count = 0;
        if (!db_stat_recent(db, "stream1", 10, &arr, &count))
            errors += fail("db_stat_recent", "failed");
        else if (count != 1)
            errors += fail("db_stat_recent", "wrong count");
        else
            pass("db_stat_recent");
        db_stat_free_list(arr);
    }

    /* Delete stream */
    {
        if (!db_stream_delete(db, "stream1"))
            errors += fail("db_stream_delete", "failed");
        else
            pass("db_stream_delete");
    }

    /* Cascade delete removes active publishers/players */
    {
        db_stream_t s;
        memset(&s, 0, sizeof(s));
        strncpy(s.id, "cascade", sizeof(s.id));
        strncpy(s.name, "Cascade Test", sizeof(s.name));
        strncpy(s.app, "live", sizeof(s.app));
        strncpy(s.publish_key, "pub_cascade", sizeof(s.publish_key));
        strncpy(s.play_key, "pl_cascade", sizeof(s.play_key));
        strncpy(s.stats_key, "st_cascade", sizeof(s.stats_key));
        s.enabled = true;
        s.created_at = time(NULL);
        if (!db_stream_add(db, &s))
            errors += fail("cascade setup stream", "failed");
        else {
            db_publisher_t p;
            memset(&p, 0, sizeof(p));
            strncpy(p.id, "pub_cascade_1", sizeof(p.id));
            strncpy(p.stream_id, "cascade", sizeof(p.stream_id));
            p.active = true;
            p.connected_at = time(NULL);
            if (!db_publisher_add(db, &p))
                errors += fail("cascade setup publisher", "failed");
            else if (!db_stream_delete(db, "cascade"))
                errors += fail("cascade delete stream", "failed");
            else {
                db_publisher_t *arr = NULL;
                int count = 0;
                db_publisher_list(db, "cascade", &arr, &count);
                if (count != 0)
                    errors += fail("cascade delete", "publisher orphaned after stream delete");
                else
                    pass("db_stream_delete cascades publishers");
                db_publisher_free_list(arr);
            }
        }
    }

    /* Max-length id (63 chars) must load with NUL terminator */
    {
        char long_id[64];
        memset(long_id, 'a', 63);
        long_id[63] = '\0';

        db_stream_t s;
        memset(&s, 0, sizeof(s));
        memcpy(s.id, long_id, 64);
        strncpy(s.name, "Long ID", sizeof(s.name));
        strncpy(s.app, "live", sizeof(s.app));
        strncpy(s.publish_key, "pub_long", sizeof(s.publish_key));
        strncpy(s.play_key, "pl_long", sizeof(s.play_key));
        strncpy(s.stats_key, "st_long", sizeof(s.stats_key));
        s.enabled = true;
        s.created_at = time(NULL);

        if (!db_stream_add(db, &s))
            errors += fail("max-length id add", "failed");
        else {
            db_stream_t got;
            memset(&got, 0, sizeof(got));
            if (!db_stream_get(db, long_id, &got))
                errors += fail("max-length id get", "not found");
            else if (strlen(got.id) != 63 || strcmp(got.id, long_id) != 0)
                errors += fail("max-length id get", "wrong id or missing NUL terminator");
            else
                pass("max-length stream id loads safely");
            db_stream_delete(db, long_id);
        }
    }

    db_close(db);
    unlink(tmp);

    if (errors == 0)
        printf("  ✓ All DB tests passed\n");
    return errors;
}
