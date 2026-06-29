/**
 * keygen.c — OS-backed cryptographically secure key material
 */
#include "librtmp2-server/keygen.h"
#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

static bool read_urandom(unsigned char *buf, size_t len)
{
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd < 0) return false;

    size_t got = 0;
    while (got < len) {
        ssize_t n = read(fd, buf + got, len - got);
        if (n <= 0) {
            close(fd);
            return false;
        }
        got += (size_t)n;
    }
    close(fd);
    return true;
}

bool keygen_secret(char *out, size_t outlen, const char *prefix)
{
    if (!out || outlen == 0 || !prefix) return false;

    size_t plen = strlen(prefix);
    /* prefix + 32 hex chars + NUL */
    if (plen + 32 + 1 > outlen) return false;

    unsigned char rnd[16];
    if (!read_urandom(rnd, sizeof(rnd))) return false;

    memcpy(out, prefix, plen);
    for (size_t i = 0; i < sizeof(rnd); i++) {
        snprintf(out + plen + i * 2, 3, "%02x", rnd[i]);
    }
    out[plen + 32] = '\0';
    return true;
}
