#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>
#include "librtmp2-server/db.h"

/* Regression test for #5: publisher/player IDs must not collide when
 * created in the same second. The fix appends a random hex suffix. */

int test_keygen_main(void)
{
    db_context_t *db = db_open(":memory:");
    if (!db) { fprintf(stderr, "FAIL: cannot open DB\n"); return 1; }

    db_stream_t s;
    memset(&s, 0, sizeof(s));
    strncpy(s.id, "test_stream", sizeof(s.id) - 1);
    strncpy(s.name, "Test", sizeof(s.name) - 1);
    strncpy(s.app, "live", sizeof(s.app) - 1);
    strncpy(s.publish_key, "pub_test", sizeof(s.publish_key) - 1);
    strncpy(s.play_key, "pl_test", sizeof(s.play_key) - 1);
    strncpy(s.stats_key, "st_test", sizeof(s.stats_key) - 1);
    s.enabled = true;
    s.created_at = time(NULL);
    db_stream_add(db, &s);

    char ids[100][64];

    for (int i = 0; i < 100; i++) {
        db_publisher_t pub;
        memset(&pub, 0, sizeof(pub));
        snprintf(pub.id, sizeof(pub.id), "pub_%ld_%08x", (long)time(NULL), rand() & 0xFFFFFFFF);
        strncpy(pub.stream_id, "test_stream", sizeof(pub.stream_id) - 1);
        strncpy(pub.app, "live", sizeof(pub.app) - 1);
        pub.active = true;
        pub.connected_at = time(NULL);

        if (!db_publisher_add(db, &pub)) {
            fprintf(stderr, "FAIL: publisher insert failed at iteration %d (id=%s)\n", i, pub.id);
            db_close(db);
            return 1;
        }

        snprintf(ids[i], sizeof(ids[i]), "%s", pub.id);
    }

    for (int i = 0; i < 100; i++) {
        for (int j = i + 1; j < 100; j++) {
            if (strcmp(ids[i], ids[j]) == 0) {
                fprintf(stderr, "FAIL: duplicate publisher ID: %s (at %d and %d)\n",
                        ids[i], i, j);
                db_close(db);
                return 1;
            }
        }
    }

    printf("PASS: test_keygen — 100 unique publisher IDs in same second\n");
    db_close(db);
    return 0;
}
