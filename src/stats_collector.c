/**
 * stats_collector.c — Aggregated server statistics
 */
#include "librtmp2-server/stats_collector.h"
#include <stdlib.h>
#include <time.h>

struct stats_collector {
    time_t   started;
    uint64_t total_bytes_in;
    uint64_t total_bytes_out;
    int      error_count;
    int      active_sessions;
    int      active_publishers;
    int      active_players;
    int      total_streams;
    double   total_bitrate_in;
};

stats_collector_t *stats_collector_create(void)
{
    stats_collector_t *s = calloc(1, sizeof(stats_collector_t));
    if (s) s->started = time(NULL);
    return s;
}

void stats_collector_destroy(stats_collector_t *stats)
{
    free(stats);
}

void stats_get_overview(stats_collector_t *stats, stats_overview_t *out)
{
    if (!stats || !out) return;
    out->server_started    = stats->started;
    out->uptime_seconds    = time(NULL) - stats->started;
    out->active_sessions   = stats->active_sessions;
    out->active_publishers = stats->active_publishers;
    out->active_players    = stats->active_players;
    out->total_bytes_in    = stats->total_bytes_in;
    out->total_bytes_out   = stats->total_bytes_out;
    out->total_bitrate_in  = stats->total_bitrate_in;
    out->total_streams     = stats->total_streams;
    out->error_count       = stats->error_count;
}

void stats_inc_errors(stats_collector_t *stats)
{
    if (stats) stats->error_count++;
}
