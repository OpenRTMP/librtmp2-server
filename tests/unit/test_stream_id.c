#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include "librtmp2-server/db.h"

/* Regression test for #9 (on_close deactivate wrong publisher) and
 * #10 (stream_id used as id field in JSON stats). */

static int test_stream_id_field_mapping(void)
{
    db_context_t *db = db_open(":memory:");
    if (!db) { fprintf(stderr, "FAIL: cannot open DB\n"); return 1; }

    db_stream_t s;
    memset(&s, 0, sizeof(s));
    strncpy(s.id, "stream-abc", sizeof(s.id) - 1);
    strncpy(s.name, "My Fancy Stream Name", sizeof(s.name) - 1);
    strncpy(s.app, "live", sizeof(s.app) - 1);
    strncpy(s.publish_key, "pub_test_key", sizeof(s.publish_key) - 1);
    strncpy(s.play_key, "pl_test_key", sizeof(s.play_key) - 1);
    strncpy(s.stats_key, "st_test_key", sizeof(s.stats_key) - 1);
    s.enabled = true;
    s.created_at = time(NULL);

    if (!db_stream_add(db, &s)) {
        fprintf(stderr, "FAIL: db_stream_add\n");
        db_close(db);
        return 1;
    }

    db_stream_t fetched;
    if (!db_stream_get(db, "stream-abc", &fetched)) {
        fprintf(stderr, "FAIL: db_stream_get\n");
        db_close(db);
        return 1;
    }

    if (strcmp(fetched.id, "stream-abc") != 0) {
        fprintf(stderr, "FAIL: id mismatch: got '%s'\n", fetched.id);
        db_close(db);
        return 1;
    }

    if (strcmp(fetched.name, "My Fancy Stream Name") != 0) {
        fprintf(stderr, "FAIL: name mismatch\n");
        db_close(db);
        return 1;
    }

    printf("PASS: test_stream_id_field_mapping\n");
    db_close(db);
    return 0;
}

static int test_on_close_matches_correct_publisher(void)
{
    db_context_t *db = db_open(":memory:");
    if (!db) { fprintf(stderr, "FAIL: cannot open DB\n"); return 1; }

    db_stream_t s1, s2;
    memset(&s1, 0, sizeof(s1));
    memset(&s2, 0, sizeof(s2));
    strncpy(s1.id, "stream1", sizeof(s1.id) - 1);
    strncpy(s1.name, "Stream One", sizeof(s1.name) - 1);
    strncpy(s1.app, "live", sizeof(s1.app) - 1);
    strncpy(s1.publish_key, "pub_key_1", sizeof(s1.publish_key) - 1);
    strncpy(s1.play_key, "pl_key_1", sizeof(s1.play_key) - 1);
    strncpy(s1.stats_key, "st_key_1", sizeof(s1.stats_key) - 1);
    s1.enabled = true;
    s1.created_at = time(NULL);

    strncpy(s2.id, "stream2", sizeof(s2.id) - 1);
    strncpy(s2.name, "Stream Two", sizeof(s2.name) - 1);
    strncpy(s2.app, "live", sizeof(s2.app) - 1);
    strncpy(s2.publish_key, "pub_key_2", sizeof(s2.publish_key) - 1);
    strncpy(s2.play_key, "pl_key_2", sizeof(s2.play_key) - 1);
    strncpy(s2.stats_key, "st_key_2", sizeof(s2.stats_key) - 1);
    s2.enabled = true;
    s2.created_at = time(NULL);

    db_stream_add(db, &s1);
    db_stream_add(db, &s2);

    db_publisher_t pub1, pub2;
    memset(&pub1, 0, sizeof(pub1));
    memset(&pub2, 0, sizeof(pub2));
    strncpy(pub1.id, "pub_1000_abc", sizeof(pub1.id) - 1);
    strncpy(pub1.stream_id, "stream1", sizeof(pub1.stream_id) - 1);
    strncpy(pub1.app, "live", sizeof(pub1.app) - 1);
    pub1.active = true;
    pub1.connected_at = time(NULL);

    strncpy(pub2.id, "pub_1000_def", sizeof(pub2.id) - 1);
    strncpy(pub2.stream_id, "stream2", sizeof(pub2.stream_id) - 1);
    strncpy(pub2.app, "live", sizeof(pub2.app) - 1);
    pub2.active = true;
    pub2.connected_at = time(NULL);

    db_publisher_add(db, &pub1);
    db_publisher_add(db, &pub2);

    /* Simulate on_close for pub1: find by publish_key → stream_id → list */
    db_stream_t found;
    if (!db_stream_find_by_publish_key(db, "pub_key_1", &found)) {
        fprintf(stderr, "FAIL: find_by_publish_key\n");
        db_close(db);
        return 1;
    }

    db_publisher_t *pubs = NULL;
    int count = 0;
    db_publisher_list(db, found.id, &pubs, &count);

    if (count != 1) {
        fprintf(stderr, "FAIL: expected 1 publisher for stream1, got %d\n", count);
        db_publisher_free_list(pubs);
        db_close(db);
        return 1;
    }

    if (strcmp(pubs[0].id, "pub_1000_abc") != 0) {
        fprintf(stderr, "FAIL: wrong publisher matched: %s\n", pubs[0].id);
        db_publisher_free_list(pubs);
        db_close(db);
        return 1;
    }

    pubs[0].active = false;
    db_publisher_update(db, pubs[0].id, &pubs[0]);
    db_publisher_free_list(pubs);

    /* Verify pub2 is still active */
    db_publisher_t *all_pubs = NULL;
    int all_count = 0;
    db_publisher_list_all(db, &all_pubs, &all_count);

    if (all_count != 2) {
        fprintf(stderr, "FAIL: expected 2 total publishers, got %d\n", all_count);
        db_publisher_free_list(all_pubs);
        db_close(db);
        return 1;
    }

    if (all_pubs[0].active == all_pubs[1].active) {
        fprintf(stderr, "FAIL: expected one active and one inactive publisher\n");
        db_publisher_free_list(all_pubs);
        db_close(db);
        return 1;
    }

    printf("PASS: test_on_close_matches_correct_publisher\n");
    db_publisher_free_list(all_pubs);
    db_close(db);
    return 0;
}

int test_stream_id_main(void)
{
    int rc = 0;
    printf("\n--- Stream ID / on_close ---\n");
    rc |= test_stream_id_field_mapping();
    rc |= test_on_close_matches_correct_publisher();
    return rc;
}
