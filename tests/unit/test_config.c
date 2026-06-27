/**
 * test_config.c — Configuration parsing tests
 */
#include "librtmp2-server/config.h"
#include <stdio.h>
#include <string.h>
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

int test_config_main(void)
{
    int errors = 0;

    /* Defaults */
    {
        server_config_t config;
        config_set_defaults(&config);

        if (strcmp(config.rtmp_bind, "0.0.0.0:1935") != 0)
            errors += fail("defaults rtmp_bind", "wrong default");
        else
            pass("defaults rtmp_bind");

        if (strcmp(config.http_bind, "0.0.0.0:8080") != 0)
            errors += fail("defaults http_bind", "wrong default");
        else
            pass("defaults http_bind");

        if (config.rtmp_max_conn != 100)
            errors += fail("defaults rtmp_max_conn", "wrong default");
        else
            pass("defaults rtmp_max_conn");

        if (config.log_level != 2)
            errors += fail("defaults log_level", "wrong default");
        else
            pass("defaults log_level");
    }

    /* Load from file */
    {
        const char *tmp = "/tmp/librtmp2_test_config.json";
        FILE *f = fopen(tmp, "w");
        if (!f) {
            errors += fail("config_load", "cannot create test file");
        } else {
            fprintf(f, "{\n"
                "  \"rtmp\": {\"bind\": \"127.0.0.1:1936\", \"max_connections\": 50},\n"
                "  \"http\": {\"bind\": \"127.0.0.1:8081\"},\n"
                "  \"auth\": {\"api_token\": \"test-token-123\"},\n"
                "  \"log_level\": 3\n"
                "}\n");
            fclose(f);

            server_config_t config;
            char err[256];
            if (!config_load(tmp, &config, err, sizeof(err))) {
                errors += fail("config_load", err);
            } else {
                pass("config_load");

                if (strcmp(config.rtmp_bind, "127.0.0.1:1936") != 0)
                    errors += fail("config_load rtmp_bind", "wrong value");
                else
                    pass("config_load rtmp_bind");

                if (config.rtmp_max_conn != 50)
                    errors += fail("config_load max_conn", "wrong value");
                else
                    pass("config_load max_conn");

                if (strcmp(config.http_bind, "127.0.0.1:8081") != 0)
                    errors += fail("config_load http_bind", "wrong value");
                else
                    pass("config_load http_bind");

                if (strcmp(config.api_token, "test-token-123") != 0)
                    errors += fail("config_load api_token", "wrong value");
                else
                    pass("config_load api_token");

                if (config.log_level != 3)
                    errors += fail("config_load log_level", "wrong value");
                else
                    pass("config_load log_level");
            }
            unlink(tmp);
        }
    }

    /* Missing file */
    {
        server_config_t config;
        char err[256];
        if (config_load("/nonexistent/path.json", &config, err, sizeof(err)))
            errors += fail("config_load missing", "should fail on missing file");
        else
            pass("config_load missing file returns false");
    }

    if (errors == 0)
        printf("  ✓ All config tests passed\n");
    return errors;
}
