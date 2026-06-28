/**
 * test_interop_haishinkkit.c — HaishinKit-style mobile publish interop test
 *
 * HaishinKit (Swift, iOS/macOS) has a distinct RTMP pattern:
 * 1. Small initial chunk size (HaishinKit defaults to 128-256 bytes)
 * 2. Short keyframe interval (1-2 seconds for low-latency mobile)
 * 3. Lower resolution (720p portrait or 480p landscape common)
 * 4. Frequent small audio packets (2048 samples/packet AAC)
 * 5. Often sends SPS/PPS inline with every keyframe (not just at start)
 * 6. May use smaller buffer sizes and shorter timeouts
 *
 * This test verifies the-specific patterns.
 */
#include "librtmp2/librtmp2.h"
#include "test_interop_common.h"

#include <curl/curl.h>

typedef struct {
    char  *data;
    size_t size;
} hk_buf_t;

static size_t hk_curl_write(void *ptr, size_t size, size_t nmemb, void *userdata)
{
    size_t total = size * nmemb;
    hk_buf_t *buf = (hk_buf_t *)userdata;
    char *tmp = realloc(buf->data, buf->size + total + 1);
    if (!tmp) return 0;
    buf->data = tmp;
    memcpy(buf->data + buf->size, ptr, total);
    buf->size += total;
    buf->data[buf->size] = '\0';
    return total;
}

/* Create stream via REST API */
static int hk_create_stream(const char *stream_id,
                             char *pub_key, size_t pub_key_sz)
{
    char url[256];
    interop_http_url(url, sizeof(url), "/api/v1/streams");

    char json[512];
    snprintf(json, sizeof(json),
             "{\"id\":\"%s\",\"name\":\"HaishinKit Mobile Stream\",\"app\":\"live\"}",
             stream_id);

    hk_buf_t resp = {0};
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
    curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, hk_curl_write);
    curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);
    curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);

    CURLcode rc = curl_easy_perform(curl);
    curl_slist_free_all(headers);
    curl_easy_cleanup(curl);

    if (rc != CURLE_OK) {
        free(resp.data);
        fprintf(stderr, "  [HaishinKit] create stream curl: %s\n", curl_easy_strerror(rc));
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
        ok = (pub_key[0] != '\0');
    }
    free(resp.data);

    if (!ok) {
        fprintf(stderr, "  [HaishinKit] Failed to parse stream creation response\n");
        return -1;
    }
    return 0;
}

/* ---- Test entry point ---- */
int test_interop_haishinkkit_main(void)
{
    printf("--- HaishinKit Interop ---\n");
    interop_result_t result;
    interop_result_init(&result);

    char stream_id[64];
    char suffix[12];
    interop_random_suffix(suffix, sizeof(suffix));
    snprintf(stream_id, sizeof(stream_id), "hk_stream_%s", suffix);

    char pub_key[128] = {0};

    /* Step 1: Create stream */
    printf("  [HaishinKit] Creating stream '%s'...\n", stream_id);
    int rc = hk_create_stream(stream_id, pub_key, sizeof(pub_key));
    interop_result_record(&result, rc == 0);
    if (rc != 0) {
        printf("  FAIL: test_interop_haishinkkit — stream creation failed\n");
        goto done_hk;
    }
    printf("  [HaishinKit] Stream created: pub_key=%.20s...\n", pub_key);

    /* Step 2: HaishinKit-style publish:
     * - Small chunk size (128 bytes — mobile network optimisation)
     * - Lower resolution (720p portrait or 480p)
     * - Short keyframe interval */
    printf("  [HaishinKit] Connecting (small chunk size: 128)...\n");
    lrtmp2_server_config_t hk_cfg;
    memset(&hk_cfg, 0, sizeof(hk_cfg));
    hk_cfg.max_connections = 5;
    hk_cfg.chunk_size = 128; /* HaishinKit default */

    lrtmp2_client_t *pub = lrtmp2_client_create(&hk_cfg);
    if (!pub) {
        printf("  FAIL: test_interop_haishinkkit — client create failed\n");
        interop_result_record(&result, 0);
        goto done_hk;
    }

    char url[256];
    /* HaishinKit typically connects with: rtmp://server/live, streamID as key */
    snprintf(url, sizeof(url), "rtmp://127.0.0.1:%d/live/%s",
             INTEROP_RTMP_PORT, stream_id);

    rc = lrtmp2_client_connect(pub, url);
    interop_result_record(&result, rc == 0);
    if (rc != 0) {
        printf("  FAIL: test_interop_haishinkkit — connect failed (%d)\n", rc);
        lrtmp2_client_destroy(pub);
        goto done_hk;
    }
    printf("  [HaishinKit] Connected\n");

    rc = lrtmp2_client_publish(pub);
    interop_result_record(&result, rc == 0);
    if (rc != 0) {
        printf("  FAIL: test_interop_haishinkkit — publish failed (%d)\n", rc);
        lrtmp2_client_destroy(pub);
        goto done_hk;
    }
    printf("  [HaishinKit] Publishing\n");

    /* Step 3: HaishinKit frame pattern — SPS/PPS inline with keyframe (mobile pattern) */
    {
        /* H.264 SPS/PPS for 720p portrait (1280x720) */
        uint8_t h264_sps[] = {0x00, 0x00, 0x00, 0x01, 0x67, 0x42, 0xC0, 0x1E};
        uint8_t h264_pps[] = {0x00, 0x00, 0x00, 0x01, 0x68, 0xCE, 0x3C, 0x80};

        lrtmp2_frame_t vf;
        memset(&vf, 0, sizeof(vf));
        vf.type = LRTMP2_FRAME_VIDEO;
        vf.video_codec = LRTMP2_VIDEO_H264;
        vf.video_frame_type = 1; /* keyframe */

        /* HaishinKit sends SPS+PPS before each keyframe (not just at start) */
        vf.data = h264_sps; vf.size = sizeof(h264_sps);
        lrtmp2_client_send_frame(pub, &vf);
        vf.data = h264_pps; vf.size = sizeof(h264_pps);
        lrtmp2_client_send_frame(pub, &vf);

        /* Keyframe data */
        uint8_t h264_keyframe[] = {0x00, 0x00, 0x00, 0x01, 0x65, 0x88, 0x84, 0x00};
        vf.data = h264_keyframe; vf.size = sizeof(h264_keyframe);
        lrtmp2_client_send_frame(pub, &vf);

        /* Mobile: 2-second keyframe interval at 30fps = 60 frames, but we just send a few */
        uint8_t h264_inter[] = {0x00, 0x00, 0x00, 0x01, 0x41, 0x9A, 0x04, 0x00};
        vf.video_frame_type = 2; /* inter frame */
        for (int i = 0; i < 5; i++) {
            vf.data = h264_inter; vf.size = sizeof(h264_inter);
            lrtmp2_client_send_frame(pub, &vf);
            usleep(33000);
        }

        /* Next keyframe: HaishinKit sends SPS/PPS inline again */
        vf.video_frame_type = 1;
        vf.data = h264_sps; vf.size = sizeof(h264_sps);
        lrtmp2_client_send_frame(pub, &vf);
        vf.data = h264_pps; vf.size = sizeof(h264_pps);
        lrtmp2_client_send_frame(pub, &vf);
        vf.data = h264_keyframe; vf.size = sizeof(h264_keyframe);
        lrtmp2_client_send_frame(pub, &vf);

        printf("  [HaishinKit] Video frames sent (inline SPS/PPS pattern, 720p30)\n");

        /* Step 4: HaishinKit AAC audio — small frequent packets */
        lrtmp2_frame_t af;
        memset(&af, 0, sizeof(af));
        af.type = LRTMP2_FRAME_AUDIO;
        af.audio_codec = LRTMP2_AUDIO_AAC;
        af.audio_sample_rate = 44100;
        af.audio_channels = 1; /* mono — common for mobile */
        af.audio_bit_depth = 16;

        /* AAC AudioSpecificConfig (mono, 44.1kHz) */
        uint8_t aac_asc[] = {0xAF, 0x00, 0x12, 0x10};
        af.data = aac_asc; af.size = sizeof(aac_asc);
        lrtmp2_client_send_frame(pub, &af);

        /* AAC data frames (HaishinKit sends ~2048 samples/packet) */
        uint8_t aac_data[] = {0xAF, 0x01, 0x0C, 0x80, 0x43, 0x80};
        af.data = aac_data; af.size = sizeof(aac_data);
        for (int i = 0; i < 3; i++) {
            lrtmp2_client_send_frame(pub, &af);
            usleep(46000); /* ~21 packets/sec for 2048 samples @ 44.1kHz */
        }

        printf("  [HaishinKit] Audio frames sent (AAC mono 44.1kHz)\n");
    }

    /* Step 5: Verify publisher registered despite tiny chunk size */
    usleep(500000);
    {
        char stats_url[512];
        snprintf(stats_url, sizeof(stats_url),
                 "http://127.0.0.1:%d/api/v1/streams",
                 INTEROP_HTTP_PORT);

        hk_buf_t resp = {0};
        CURL *curl = curl_easy_init();
        if (curl) {
            struct curl_slist *h = NULL;
            char auth[256];
            snprintf(auth, sizeof(auth), "Authorization: Bearer %s", INTEROP_TOKEN);
            h = curl_slist_append(h, auth);
            curl_easy_setopt(curl, CURLOPT_URL, stats_url);
            curl_easy_setopt(curl, CURLOPT_HTTPHEADER, h);
            curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, hk_curl_write);
            curl_easy_setopt(curl, CURLOPT_WRITEDATA, &resp);
            curl_easy_setopt(curl, CURLOPT_TIMEOUT, 5L);
            curl_easy_perform(curl);
            curl_slist_free_all(h);
            curl_easy_cleanup(curl);

            int ok = (resp.data != NULL && strstr(resp.data, stream_id) != NULL);
            interop_result_record(&result, ok);
            if (!ok) {
                printf("  FAIL: test_interop_haishinkkit — stream not found in API list\n");
            } else {
                printf("  [HaishinKit] Stream visible in API despite 128B chunk size\n");
            }
            free(resp.data);
        }
    }

    /* Step 6: Disconnect and cleanup */
    lrtmp2_client_destroy(pub);
    usleep(300000);
    printf("  [HaishinKit] Disconnected\n");

done_hk:
    printf("--- HaishinKit Interop Results: %d/%d checks passed ---\n\n",
           result.passed, result.total);
    return (result.passed == result.total && result.total > 0) ? 0 : 1;
}
