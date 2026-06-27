/**
 * logger.h — Simple logging
 */
#ifndef LRTMP2_SERVER_LOGGER_H
#define LRTMP2_SERVER_LOGGER_H

#ifdef __cplusplus
extern "C" {
#endif

typedef enum {
    LOG_ERROR = 0,
    LOG_WARN,
    LOG_INFO,
    LOG_DEBUG,
} log_level_t;

void logger_init(log_level_t level, const char *file_path);
void logger_close(void);
void log_error(const char *fmt, ...);
void log_warn(const char *fmt, ...);
void log_info(const char *fmt, ...);
void log_debug(const char *fmt, ...);

#ifdef __cplusplus
}
#endif

#endif /* LRTMP2_SERVER_LOGGER_H */
