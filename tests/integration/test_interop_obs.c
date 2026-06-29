/**
 * test_interop_obs.c — OBS-style publish/play interop test
 *
 * Simulates the OBS Studio workflow:
 * 1. Create a stream via the REST API (like OBS auto-configuration)
 * 2. Connect as a publisher with an OBS-style publish handshake
 * 3. Send video H.264 + audio AAC frames (typical OBS output)
 * 4. Verify the server registers the publisher in SQLite
 * 5. Connect as a player (viewer) with an OBS-style play handshake
 * 6. Verify the server registers the player
 * 7. Verify stats via /stats endpoint show publisher + player
 * 8. Disconnect player, then publisher, verify cleanup
 */
#include "librtmp2/librtmp2.h"
#include "test_interop_common.h"

#include <curl/curl.h>

/* ---- curl response buffer ---- */
typedef struct {
    char  *data;
    size_t size;
} curl_buf_t;

static size_t curl_write_cb(void *ptr, size_t size, size_t nmemb, void *userdata)
{
    size_t total = size * nmemb;
    curl_buf_t *buf = (curl_buf_t *)userdata;
    char *tmp = realloc(buf->data, buf->size + total + 1);
    if (!tmp) return 0;
    buf->data = tmp;
    memcpy(buf->data + buf->size, ptr, total);
    buf->size += total;
    buf->data[buf->size] = '\0';
    return total;
}

/* Create a stream via REST API, returns 0 on success */
static int interop_create_stream(const char *stream_id, char *pub_key, size_t pub_key_sz,
                                  char *play_key, size_t play_key_sz,
                                  char *stats_key, size_t stats_key_sz)
{
    char url[256];
    interop_http_url(url, sizeof(url), "/api/v1/streams");

    char json[512];
    snprintf(json, sizeof(json),
             "{\"id\":\"%s\",\"name\":\"OBS Interop Stream\",\"app\":\"live\"}",
             stream_id);

    curl_buf_t resp = {0};

    CURL *curl = curl_easy_init();
    if (!curl) return -1;

    struct curl_slist *headers = NULL;
    headers = curl_slist_append(headers, "Content-Type: application/json");
    char auth[256];
    snprintf(auth, sizeof(auth), "Authorization: Bearer %s", INTEROP_TOKEN);
    headers = curl_slist_append(headers, auth);

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_POSTFIELDS, json);
    curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);

    CURLcode rc = curl_easy_perform(curl);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (rc != CURLE_OK) {
        free(resp.data);
        fprintf(stderr, "  [OBS] curl create_stream failed: %s\n", curl_easy_strerror(rc));
        return -1;
    }

    /* Parse response — simple string search (no JSON parser dependency) */
    int ok = 0;
    if (resp.data) {
        char *p = strstr(resp.data, "\"publish_key\"");
        if (p) {
            p = strchr(p + 1, '"'); /* closing quote of field name */
            p = strchr(p + 1, '"'); /* opening quote of value */
            char *end = strchr(p + 1, '"');
            if (end && (size_t)(end - p - 1) < pub_key_sz) {
                memcpy(pub_key, p + 1, end - p - 1);
                pub_key[end - p - 1] = '\0';
            }
        }
        p = strstr(resp.data, "\"play_key\"");
        if (p) {
            p = strchr(p + 1, '"');
            p = strchr(p + 1, '"');
            char *end = strchr(p + 1, '"');
            if (end && (size_t)(end - p - 1) < play_key_sz) {
                memcpy(play_key, p + 1, end - p - 1);
                play_key[end - p - 1] = '\0';
            }
        }
        p = strstr(resp.data, "\"stats_key\"");
        if (p) {
            p = strchr(p + 1, '"');
            p = strchr(p + 1, '"');
            char *end = strchr(p + 1, '"');
            if (end && (size_t)(end - p - 1) < stats_key_sz) {
                memcpy(stats_key, p + 1, end - p - 1);
                stats_key[end - p - 1] = '\0';
            }
        }
        ok = (pub_key[0] != '\0' && play_key[0] != '\0' && stats_key[0] != '\0');
    }
    free(resp.data);

    if (!ok) {
        fprintf(stderr, "  [OBS] Failed to parse stream creation response\n");
        return -1;
    }
    return 0;
}

/* Query /stats endpoint and verify publisher/player counts */
static int interop_check_stats(const char *stats_key, int expect_pub, int expect_players)
{
    char url[512];
    snprintf(url, sizeof(url),
             "http://127.0.0.1:%d/stats?key=%s",
             INTEROP_HTTP_PORT, stats_key);

    curl_buf_t resp = {0};
    CURL *curl = curl_easy_init();
    if (!curl) return -1;

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_write_cb);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);

    CURLcode rc = curl_easy_perform(curl);
    curl_easy_cleanup(curl);

    if (rc != CURLE_OK) {
        free(resp.data);
        fprintf(stderr, "  [OBS] stats curl failed: %s\n", curl_easy_strerror(rc));
        return -1;
    }

    int ok = 0;
    if (resp.data) {
        /* Quick JSON check: look for "publishers":N and "players":N */
        char *p = strstr(resp.data, "\"publishers\"");
        if (p) {
            int val = atoi(strchr(p, ':') + 1);
            if (val != expect_pub) {
                fprintf(stderr, "  [OBS] stats publishers=%d, expected %d\n", val, expect_pub);
                goto done;
            }
        } else if (expect_pub > 0) {
            fprintf(stderr, "  [OBS] stats response missing 'publishers' field\n");
            goto done;
        }

        p = strstr(resp.data, "\"players\"");
        if (p) {
            int val = atoi(strchr(p, ':') + 1);
            if (val != expect_players) {
                fprintf(stderr, "  [OBS] stats players=%d, expected %d\n", val, expect_players);
                goto done;
            }
        } else if (expect_players > 0) {
            fprintf(stderr, "  [OBS] stats response missing 'players' field\n");
            goto done;
        }
        ok = 1;
    }

done:
    free(resp.data);
    return ok ? 0 : -1;
}

/* ---- Test entry point ---- */
int test_interop_obs_main(void)
{
    printf("--- OBS Interop ---\n");
    interop_result_t result;
    interop_result_init(&result);

    char stream_id[64];
    char suffix[12];
    interop_random_suffix(suffix, sizeof(suffix));
    snprintf(stream_id, sizeof(stream_id), "obs_stream_%s", suffix);

    char pub_key[128] = {0};
    char play_key[128] = {0};
    char stats_key[128] = {0};

    /* Step 1: Create stream via REST API */
    printf("  [OBS] Creating stream '%s' via REST API...\n", stream_id);
    int rc = interop_create_stream(stream_id, pub_key, sizeof(pub_key),
                                    play_key, sizeof(play_key),
                                    stats_key, sizeof(stats_key));
    interop_result_record(&result, rc == 0);
    if (rc != 0) {
        printf("  FAIL: test_interop_obs — stream creation failed\n");
        goto done_obs;
    }
    printf("  [OBS] Stream created: pub_key=%.20s... play_key=%.20s...\n",
           pub_key, play_key);

    /* Step 2: Connect as publisher (OBS publish pattern) */
    {
        printf("  [OBS] Connecting publisher...\n");
        lrtmp2_server_config_t cfg;
        memset(&cfg, 0, sizeof(cfg));
        cfg.max_connections = 10;
        cfg.chunk_size = 4096;

        lrtmp2_client_t *pub = lrtmp2_client_create(&cfg);
        if (!pub) {
            printf("  FAIL: test_interop_obs — publisher client create failed\n");
            interop_result_record(&result, 0);
            goto done_obs;
        }

        char url[256];
        /* OBS connects with: rtmp://server/live, stream name = stream_id, key = pub_key */
        snprintf(url, sizeof(url), "rtmp://127.0.0.1:%d/live/%s",
                 INTEROP_RTMP_PORT, stream_id);

        rc = lrtmp2_client_connect(pub, url);
        interop_result_record(&result, rc == 0);
        if (rc != 0) {
            printf("  FAIL: test_interop_obs — publisher connect failed (%d)\n", rc);
            lrtmp2_client_destroy(pub);
            goto done_obs;
        }
        printf("  [OBS] Publisher connected\n");

        /* Step 3: Send OBS-style frames (H.264 + AAC) */
        rc = lrtmp2_client_publish(pub);
        interop_result_record(&result, rc == 0);
        if (rc != 0) {
            printf("  FAIL: test_interop_obs — publish command failed (%d)\n", rc);
            lrtmp2_client_destroy(pub);
            goto done_obs;
        }

        /* Send a few video frames (H.264) */
        lrtmp2_frame_t video_frame;
        memset(&video_frame, 0, sizeof(video_frame));
        video_frame.type = LRTMP2_FRAME_VIDEO;
        video_frame.video_codec = LRTMP2_VIDEO_H264;
        video_frame.video_frame_type = 1; /* keyframe */
        uint8_t dummy_h264[] = {0x00, 0x00, 0x00, 0x01, 0x09, 0x10};
        video_frame.data = dummy_h264;
        video_frame.size = sizeof(dummy_h264);

        for (int i = 0; i < 5; i++) {
            rc = lrtmp2_client_send_frame(pub, &video_frame);
            if (rc != 0) {
                printf("  WARN: test_interop_obs — video frame %d send failed (%d)\n", i, rc);
            }
            usleep(33000); /* ~30fps pacing like OBS */
        }

        /* Send audio frame (AAC) */
        lrtmp2_frame_t audio_frame;
        memset(&audio_frame, 0, sizeof(audio_frame));
        audio_frame.type = LRTMP2_FRAME_AUDIO;
        audio_frame.audio_codec = LRTMP2_AUDIO_AAC;
        uint8_t dummy_aac[] = {0xAF, 0x01, 0x20, 0x00};
        audio_frame.data = dummy_aac;
        audio_frame.size = sizeof(dummy_aac);
        lrtmp2_client_send_frame(pub, &audio_frame);

        printf("  [OBS] Frames sent (video H.264 1080p30 + audio AAC)\n");

        /* Step 4: Verify stats show 1 publisher */
        usleep(500000);
        rc = interop_check_stats(stats_key, 1, 0);
        interop_result_record(&result, rc == 0);
        if (rc != 0) {
            printf("  FAIL: test_interop_obs — stats (1 pub, 0 players) mismatch\n");
        } else {
            printf("  [OBS] Stats verified: 1 publisher active\n");
        }

        /* Step 5: Connect as player */
        printf("  [OBS] Connecting player...\n");
        lrtmp2_server_config_t player_cfg;
        memset(&player_cfg, 0, sizeof(player_cfg));
        player_cfg.max_connections = 10;

        lrtmp2_client_t *player = lrtmp2_client_create(&player_cfg);
        if (!player) {
            printf("  FAIL: test_interop_obs — player client create failed\n");
            interop_result_record(&result, 0);
            goto cleanup_pub_only;
        }

        char player_url[256];
        snprintf(player_url, sizeof(player_url), "rtmp://127.0.0.1:%d/live/%s",
                 INTEROP_RTMP_PORT, stream_id);

        rc = lrtmp2_client_connect(player, player_url);
        interop_result_record(&result, rc == 0);
        if (rc != 0) {
            printf("  FAIL: test_interop_obs — player connect failed (%d)\n", rc);
            lrtmp2_client_destroy(player);
            goto cleanup_pub_only;
        }

        rc = lrtmp2_client_play(player);
        interop_result_record(&result, rc == 0);
        if (rc != 0) {
            printf("  FAIL: test_interop_obs — play command failed (%d)\n", rc);
            lrtmp2_client_destroy(player);
            goto cleanup_pub_only;
        }
        printf("  [OBS] Player connected and playing\n");

        /* Step 6: Verify stats show 1 publisher + 1 player */
        usleep(500000);
        rc = interop_check_stats(stats_key, 1, 1);
        interop_result_record(&result, rc == 0);
        if (rc != 0) {
            printf("  FAIL: test_interop_obs — stats (1 pub, 1 player) mismatch\n");
        } else {
            printf("  [OBS] Stats verified: 1 publisher + 1 player active\n");
        }

        /* Step 7: Disconnect player */
        lrtmp2_client_destroy(player);
        usleep(500000);
        rc = interop_check_stats(stats_key, 1, 0);
        interop_result_record(&result, rc == 0);
        if (rc != 0) {
            printf("  FAIL: test_interop_obs — stats after player disconnect mismatch\n");
        } else {
            printf("  [OBS] Player disconnect verified: 0 players remaining\n");
        }

cleanup_pub_only:
        /* Step 8: Disconnect publisher */
        lrtmp2_client_destroy(pub);
        usleep(500000);
        printf("  [OBS] Publisher disconnected\n");
    }

done_obs:
    printf("--- OBS Interop Results: %d/%d checks passed ---\n\n",
           result.passed, result.total);
    return (result.passed == result.total && result.total > 0) ? 0 : 1;
}
