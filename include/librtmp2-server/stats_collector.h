/**
 * stats_collector.h — Aggregated server statistics
 */
#ifndef LRTMP2_SERVER_STATS_H
#define LRTMP2_SERVER_STATS_H

#include <stdint.h>
#include <stdbool.h>
#include <time.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
    uint64_t uptime_seconds;
    int      active_sessions;
    int      active_publishers;
    int      active_players;
    uint64_t total_bytes_in;
    uint64_t total_bytes_out;
    double   total_bitrate_in;    /* bps */
    int      total_streams;
    int      error_count;
    time_t   server_started;
} stats_overview_t;

typedef struct stats_collector stats_collector_t;

stats_collector_t *stats_collector_create(void);
void               stats_collector_destroy(stats_collector_t *stats);

void stats_get_overview(stats_collector_t *stats, stats_overview_t *out);

/* Increment error counter */
void stats_inc_errors(stats_collector_t *stats);

#ifdef __cplusplus
}
#endif

#endif /* LRTMP2_SERVER_STATS_H */
