/**
 * test_config.c — Configuration parsing tests
 */
#include "librtmp2-server/config.h"
#include <stdio.h>
#include <stdlib.h>
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

        /* TLS is off by default, with empty cert/key paths. */
        if (config.tls_enabled)
            errors += fail("defaults tls_enabled", "TLS should be off by default");
        else
            pass("defaults tls_enabled off");

        if (config.tls_cert_file[0] != '\0' || config.tls_key_file[0] != '\0')
            errors += fail("defaults tls cert/key", "should be empty by default");
        else
            pass("defaults tls cert/key empty");
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
                "  \"tls\": {\"enabled\": true, \"cert_file\": \"/etc/ssl/cert.pem\", \"key_file\": \"/etc/ssl/key.pem\"},\n"
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

                if (!config.tls_enabled)
                    errors += fail("config_load tls_enabled", "expected true");
                else
                    pass("config_load tls_enabled");

                if (strcmp(config.tls_cert_file, "/etc/ssl/cert.pem") != 0)
                    errors += fail("config_load tls_cert_file", "wrong value");
                else
                    pass("config_load tls_cert_file");

                if (strcmp(config.tls_key_file, "/etc/ssl/key.pem") != 0)
                    errors += fail("config_load tls_key_file", "wrong value");
                else
                    pass("config_load tls_key_file");
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

    /* Environment overrides for TLS */
    {
        setenv("LRTMP2_TLS_ENABLED", "1", 1);
        setenv("LRTMP2_TLS_CERT_FILE", "/env/cert.pem", 1);
        setenv("LRTMP2_TLS_KEY_FILE", "/env/key.pem", 1);

        server_config_t config;
        config_set_defaults(&config);
        config_apply_env(&config);

        if (!config.tls_enabled)
            errors += fail("env tls_enabled", "LRTMP2_TLS_ENABLED=1 should enable TLS");
        else
            pass("env tls_enabled");

        if (strcmp(config.tls_cert_file, "/env/cert.pem") != 0)
            errors += fail("env tls_cert_file", "wrong value");
        else
            pass("env tls_cert_file");

        if (strcmp(config.tls_key_file, "/env/key.pem") != 0)
            errors += fail("env tls_key_file", "wrong value");
        else
            pass("env tls_key_file");

        /* An invalid value must not silently flip TLS off. */
        setenv("LRTMP2_TLS_ENABLED", "yesplease", 1);
        config_set_defaults(&config);
        config.tls_enabled = true;  /* pretend the JSON enabled it */
        config_apply_env(&config);
        if (!config.tls_enabled)
            errors += fail("env tls_enabled invalid", "invalid value should leave TLS unchanged");
        else
            pass("env tls_enabled invalid value ignored");

        unsetenv("LRTMP2_TLS_ENABLED");
        unsetenv("LRTMP2_TLS_CERT_FILE");
        unsetenv("LRTMP2_TLS_KEY_FILE");
    }

    if (errors == 0)
        printf("  ✓ All config tests passed\n");
    return errors;
}
