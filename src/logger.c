/**
 * logger.c — Simple thread-aware logging to stderr or file
 */
#include "librtmp2-server/logger.h"
#include <stdio.h>
#include <stdarg.h>
#include <time.h>
#include <string.h>

static log_level_t g_level = LOG_INFO;
static FILE       *g_file = NULL;
static int         g_initialized = 0;

void logger_init(log_level_t level, const char *file_path)
{
    g_level = level;
    if (file_path && file_path[0]) {
        g_file = fopen(file_path, "a");
    }
    g_initialized = 1;
}

void logger_close(void)
{
    if (g_file) {
        fclose(g_file);
        g_file = NULL;
    }
}

static void log_write(const char *prefix, const char *fmt, va_list ap)
{
    FILE *out = g_file ? g_file : stderr;
    time_t now = time(NULL);
    struct tm tm;
    char buf[64];

    localtime_r(&now, &tm);
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", &tm);

    fprintf(out, "[%s] %s ", buf, prefix);
    vfprintf(out, fmt, ap);
    fprintf(out, "\n");
    fflush(out);
}

void log_error(const char *fmt, ...)
{
    va_list ap; va_start(ap, fmt); log_write("ERROR", fmt, ap); va_end(ap);
}

void log_warn(const char *fmt, ...)
{
    if (g_level < LOG_WARN) return;
    va_list ap; va_start(ap, fmt); log_write("WARN ", fmt, ap); va_end(ap);
}

void log_info(const char *fmt, ...)
{
    if (g_level < LOG_INFO) return;
    va_list ap; va_start(ap, fmt); log_write("INFO ", fmt, ap); va_end(ap);
}

void log_debug(const char *fmt, ...)
{
    if (g_level < LOG_DEBUG) return;
    va_list ap; va_start(ap, fmt); log_write("DEBUG", fmt, ap); va_end(ap);
}
