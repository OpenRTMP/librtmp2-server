/**
 * test_interop_ffmpeg.c — FFmpeg-style ingestion interop test
 *
 * Simulates FFmpeg's push workflow (e.g. `ffmpeg -re -i input -c copy -f flv rtmp://...`):
 * 1. Create a stream via REST API
 * 2. Connect as publisher with FFmpeg-style handshake (minimal connect, no app)
 * 3. Send concatenated H.264+AAC sequences (FFmpeg sends less metadata than OBS)
 * 4. Verify server accepts and registers the publisher
 * 5. Simulate FFmpeg reconnect behaviour (disconnect + same stream re-publish)
 * 6. Verify stream lifecycle: publish → disconnect → re-publish → final cleanup
 * 7. Query stats during active phase to verify data accounting
 */
#include "librtmp2/librtmp2.h"
#include "test_interop_common.h"

#include <curl/curl.h>

/* Reuse curl buffer type from OBS test — declared locally here for isolation */
typedef struct {
    char  *data;
    size_t size;
} ffmpeg_buf_t;

static size_t ffmpeg_curl_write(void *ptr, size_t size, size_t nmemb, void *userdata)
{
    size_t total = size * nmemb;
    ffmpeg_buf_t *buf = (ffmpeg_buf_t *)userdata;
    char *tmp = realloc(buf->data, buf->size + total + 1);
    if (!tmp) return 0;
    buf->data = tmp;
    memcpy(buf->data + buf->size, ptr, total);
    buf->size += total;
    buf->data[buf->size] = '\0';
    return total;
}

/* Create stream via REST API, returns fill keys */
static int ffmpeg_create_stream(const char *stream_id,
                                 char *pub_key, size_t pub_key_sz,
                                 char *stats_key, size_t stats_key_sz)
{
    char url[256];
    interop_http_url(url, sizeof(url), "/api/v1/streams");

    char json[512];
    snprintf(json, sizeof(json),
             "{\"id\":\"%s\",\"name\":\"FFmpeg Interop Stream\",\"app\":\"live\"}",
             stream_id);

    ffmpeg_buf_t resp = {0};
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
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, ffmpeg_curl_write);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);

    CURLcode rc = curl_easy_perform(curl);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (rc != CURLE_OK) {
        free(resp.data);
        fprintf(stderr, "  [FFmpeg] create stream curl: %s\n", curl_easy_strerror(rc));
        return -1;
    }

    int ok = 0;
    if (resp.data) {
        char *p = strstr(resp.data, "\"publish_key\"");
        if (p) {
            p = strchr(p + 1, '"');
            p = strchr(p + 1, '"');
            char *end = strchr(p + 1, '"');
            if (end && (size_t)(end - p - 1) < pub_key_sz) {
                memcpy(pub_key, p + 1, end - p - 1);
                pub_key[end - p - 1] = '\0';
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
        ok = (pub_key[0] != '\0' && stats_key[0] != '\0');
    }
    free(resp.data);

    if (!ok) {
        fprintf(stderr, "  [FFmpeg] Failed to parse stream creation response\n");
        return -1;
    }
    return 0;
}

/* Check if stats show expected publisher count */
static int ffmpeg_check_pub_count(const char *stats_key, int expect_pub)
{
    char url[512];
    snprintf(url, sizeof(url),
             "http://127.0.0.1:%d/stats?key=%s",
             INTEROP_HTTP_PORT, stats_key);

    ffmpeg_buf_t resp = {0};
    CURL *curl = curl_easy_init();
    if (!curl) return -1;

    curl_easy_setopt(curl, CURLOPT_URL, url);
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, ffmpeg_curl_write);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);

    CURLcode rc = curl_easy_perform(curl);
    curl_easy_cleanup(curl);

    if (rc != CURLE_OK) {
        free(resp.data);
        return -1;
    }

    int ok = 0;
    if (resp.data) {
        char *p = strstr(resp.data, "\"publishers\"");
        if (p) {
            int val = atoi(strchr(p, ':') + 1);
            ok = (val == expect_pub);
            if (!ok) {
                fprintf(stderr, "  [FFmpeg] stats publishers=%d, expected %d\n", val, expect_pub);
            }
        }
    }
    free(resp.data);
    return ok ? 0 : -1;
}

/* ---- Test entry point ---- */
int test_interop_ffmpeg_main(void)
{
    printf("--- FFmpeg Interop ---\n");
    interop_result_t result;
    interop_result_init(&result);

    char stream_id[64];
    char suffix[12];
    interop_random_suffix(suffix, sizeof(suffix));
    snprintf(stream_id, sizeof(stream_id), "ffmpeg_stream_%s", suffix);

    char pub_key[128] = {0};
    char stats_key[128] = {0};

    /* Step 1: Create stream via REST API */
    printf("  [FFmpeg] Creating stream '%s'...\n", stream_id);
    int rc = ffmpeg_create_stream(stream_id, pub_key, sizeof(pub_key),
                                   stats_key, sizeof(stats_key));
    interop_result_record(&result, rc == 0);
    if (rc != 0) {
        printf("  FAIL: test_interop_ffmpeg — stream creation failed\n");
        goto done_ffmpeg;
    }
    printf("  [FFmpeg] Stream created: pub_key=%.20s...\n", pub_key);

    /* Step 2: FFmpeg-style publish — connect, publish, send frames */
    lrtmp2_server_config_t pub_cfg;
    memset(&pub_cfg, 0, sizeof(pub_cfg));
    pub_cfg.max_connections = 16;
    pub_cfg.chunk_size = 128; /* FFmpeg uses small chunk size by default */

    /* ---- First publish cycle ---- */
    printf("  [FFmpeg] First publish cycle — connecting...\n");
    lrtmp2_client_t *pub = lrtmp2_client_create(&pub_cfg);
    if (!pub) {
        printf("  FAIL: test_interop_ffmpeg — client create failed\n");
        interop_result_record(&result, 0);
        goto done_ffmpeg;
    }

    char url[256];
    snprintf(url, sizeof(url), "rtmp://127.0.0.1:%d/live/%s",
             INTEROP_RTMP_PORT, stream_id);

    rc = lrtmp2_client_connect(pub, url);
    interop_result_record(&result, rc == 0);
    if (rc != 0) {
        printf("  FAIL: test_interop_ffmpeg — connect failed (%d)\n", rc);
        lrtmp2_client_destroy(pub);
        goto done_ffmpeg;
    }

    rc = lrtmp2_client_publish(pub);
    interop_result_record(&result, rc == 0);
    if (rc != 0) {
        printf("  FAIL: test_interop_ffmpeg — publish failed (%d)\n", rc);
        lrtmp2_client_destroy(pub);
        goto done_ffmpeg;
    }
    printf("  [FFmpeg] First publish cycle active\n");

    /* Send FFmpeg-style frames (minimal metadata, just ES) */
    {
        /* H.264 SPS/PPS NAL units (minimal) */
        uint8_t h264_sps[] = {0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0xC0, 0x0A};
        uint8_t h264_pps[] = {0x00, 0x00, 0x00, 0x01, 0x68, 0xCE, 0x3C, 0x80};
        uint8_t h264_idr[]  = {0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x00};

        lrtmp2_frame_t vf;
        memset(&vf, 0, sizeof(vf));
        vf.type = LRTMP2_FRAME_VIDEO;
        vf.video_codec = LRTMP2_VIDEO_H264;
        vf.video_frame_type = 1; /* keyframe */

        vf.data = h264_sps; vf.size = sizeof(h264_sps);
        lrtmp2_client_send_frame(pub, &vf);
        vf.data = h264_pps; vf.size = sizeof(h264_pps);
        lrtmp2_client_send_frame(pub, &vf);

        for (int i = 0; i < 3; i++) {
            vf.data = h264_idr; vf.size = sizeof(h264_idr);
            lrtmp2_client_send_frame(pub, &vf);
            usleep(40000); /* 25fps */
        }

        /* AAC AudioSpecificConfig (FFmpeg sends this as first audio packet) */
        uint8_t aac_config[] = {0xAF, 0x00, 0x11, 0x90};
        lrtmp2_frame_t af;
        memset(&af, 0, sizeof(af));
        af.type = LRTMP2_FRAME_AUDIO;
        af.audio_codec = LRTMP2_AUDIO_AAC;
        af.data = aac_config;
        af.size = sizeof(aac_config);
        lrtmp2_client_send_frame(pub, &af);

        printf("  [FFmpeg] Frames sent: H.264 SPS/PPS + IDR, AAC config\n");
    }

    /* Step 3: Verify publisher registered */
    usleep(500000);
    rc = ffmpeg_check_pub_count(stats_key, 1);
    interop_result_record(&result, rc == 0);
    if (rc != 0) {
        printf("  FAIL: test_interop_ffmpeg — publisher not found in stats\n");
    } else {
        printf("  [FFmpeg] Publisher registered in stats\n");
    }

    /* Step 4: Disconnect (simulate FFmpeg being stopped) */
    printf("  [FFmpeg] Disconnecting (simulating stop)...\n");
    lrtmp2_client_destroy(pub);
    usleep(500000);
    rc = ffmpeg_check_pub_count(stats_key, 0);
    interop_result_record(&result, rc == 0);
    if (rc != 0) {
        printf("  FAIL: test_interop_ffmpeg — publisher not cleaned up after disconnect\n");
    } else {
        printf("  [FFmpeg] Publisher cleaned up after disconnect\n");
    }

    /* Step 5: Reconnect (simulate FFmpeg being restarted) */
    printf("  [FFmpeg] Reconnecting (simulating restart)...\n");
    pub = lrtmp2_client_create(&pub_cfg);
    if (!pub) {
        printf("  FAIL: test_interop_ffmpeg — reconnect client create failed\n");
        interop_result_record(&result, 0);
        goto done_ffmpeg;
    }

    rc = lrtmp2_client_connect(pub, url);
    interop_result_record(&result, rc == 0);
    if (rc != 0) {
        printf("  FAIL: test_interop_ffmpeg — reconnect failed (%d)\n", rc);
        lrtmp2_client_destroy(pub);
        goto done_ffmpeg;
    }

    rc = lrtmp2_client_publish(pub);
    interop_result_record(&result, rc == 0);
    if (rc != 0) {
        printf("  FAIL: test_interop_ffmpeg — republish failed (%d)\n", rc);
        lrtmp2_client_destroy(pub);
        goto done_ffmpeg;
    }
    printf("  [FFmpeg] Re-publish active\n");

    /* Step 6: Verify re-publish registered */
    usleep(500000);
    rc = ffmpeg_check_pub_count(stats_key, 1);
    interop_result_record(&result, rc == 0);
    if (rc != 0) {
        printf("  FAIL: test_interop_ffmpeg — re-publish not registered\n");
    } else {
        printf("  [FFmpeg] Re-publish registered in stats\n");
    }

    /* Final cleanup */
    lrtmp2_client_destroy(pub);
    usleep(500000);
    printf("  [FFmpeg] Final disconnect\n");

done_ffmpeg:
    printf("--- FFmpeg Interop Results: %d/%d checks passed ---\n\n",
           result.passed, result.total);
    return (result.passed == result.total && result.total > 0) ? 0 : 1;
}
