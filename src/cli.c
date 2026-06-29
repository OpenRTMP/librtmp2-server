/**
 * cli.c — Command-line entry point
 */
#include "librtmp2-server/server.h"
#include "librtmp2-server/config.h"
#include "librtmp2-server/logger.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <signal.h>

#include "librtmp2-server/config.h"
#include "librtmp2-server/logger.h"
{
    fprintf(stderr, "Usage: %s [-c config.json]\n", prog);
    fprintf(stderr, "\n");
    fprintf(stderr, "Options:\n");
    fprintf(stderr, "  -c <path>   Config file path (default: config.json)\n");
    fprintf(stderr, "  -p <port>   RTMP port (overrides config, default: 1935)\n");
    fprintf(stderr, "  -w <port>   HTTP port (overrides config, default: 8080)\n");
    fprintf(stderr, "  -t <token>  API token (overrides config)\n");
    fprintf(stderr, "  -v          Verbose (debug logging)\n");
    fprintf(stderr, "  -h          Show this help\n");
}

int main(int argc, char **argv)
{
    server_config_t config;
    config_set_defaults(&config);

    char config_path[256] = "config.json";
    int verbose = 0;

    /* Parse CLI args */
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "-c") == 0 && i + 1 < argc) {
            strncpy(config_path, argv[++i], sizeof(config_path) - 1);
        } else if (strcmp(argv[i], "-p") == 0 && i + 1 < argc) {
            int port = atoi(argv[++i]);
            snprintf(config.rtmp_bind, sizeof(config.rtmp_bind), "0.0.0.0:%d", port);
        } else if (strcmp(argv[i], "-w") == 0 && i + 1 < argc) {
            int port = atoi(argv[++i]);
            snprintf(config.http_bind, sizeof(config.http_bind), "0.0.0.0:%d", port);
        } else if (strcmp(argv[i], "-t") == 0 && i + 1 < argc) {
            strncpy(config.api_token, argv[++i], sizeof(config.api_token) - 1);
        } else if (strcmp(argv[i], "-v") == 0) {
            verbose = 1;
        } else if (strcmp(argv[i], "-h") == 0) {
            print_usage(argv[0]);
            return 0;
        } else {
            fprintf(stderr, "Unknown option: %s\n", argv[i]);
            print_usage(argv[0]);
            return 1;
        }
    }

    /* Load config file if it exists */
    char err[256];
    FILE *test = fopen(config_path, "r");
    if (test) {
        fclose(test);
        if (!config_load(config_path, &config, err, sizeof(err))) {
            fprintf(stderr, "Config error: %s\n", err);
            return 1;
        }
    } else {
        fprintf(stderr, "No config file at %s, using defaults\n", config_path);
    }

    /* Environment variables override config file values */
    config_apply_env(&config);

    if (verbose) config.log_level = 3;

    /* Refuse to start with missing, placeholder, or otherwise weak API tokens.
     * An empty token would bypass Bearer auth; the shipped config placeholder
     * is public knowledge and must not be accepted as a real secret. */
    if (!config_api_token_usable(config.api_token)) {
        fprintf(stderr, "FATAL: auth.api_token is missing or uses a known weak "
                        "placeholder. Set a strong random token in %s, via -t, "
                        "or LRTMP2_API_TOKEN.\n", config_path);
        return 1;
    }

    /* Init logger */
    logger_init(config.log_level, config.log_file);

    /* Create and run server */
    lrtmp2_server_app_t *app = server_app_create(&config);
    if (!app) {
        log_error("Failed to create server app");
        logger_close();
        return 1;
    }

    int rc = server_app_run(app);
    server_app_destroy(app);
    logger_close();

    return rc;
}
