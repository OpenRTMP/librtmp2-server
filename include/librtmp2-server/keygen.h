/**
 * keygen.h — Cryptographically secure key generation for stream secrets
 */
#ifndef LRTMP2_SERVER_KEYGEN_H
#define LRTMP2_SERVER_KEYGEN_H

#include <stddef.h>
#include <stdbool.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Fill `out` with `prefix` followed by 32 hex chars (16 bytes / 128 bits of entropy).
 * Returns false on failure (buffer too small or OS randomness unavailable). */
bool keygen_secret(char *out, size_t outlen, const char *prefix);

#ifdef __cplusplus
}
#endif

#endif /* LRTMP2_SERVER_KEYGEN_H */
