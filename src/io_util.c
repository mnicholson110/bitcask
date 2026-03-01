#include "../include/io_util.h"
#include "../include/entry.h"
#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <string.h>
#include <sys/uio.h>
#include <unistd.h>

bool pread_exact(int fd, uint8_t *buf, size_t len, off_t offset)
{
    size_t done = 0;

    while (done < len)
    {
        ssize_t n = pread(fd, buf + done, len - done, offset + (off_t)done);
        if (n == 0)
        {
            return false;
        }
        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return false;
        }

        done += (size_t)n;
    }

    return true;
}

bool pwrite_exact(int fd, uint8_t *buf, size_t len, off_t offset)
{
    size_t done = 0;

    while (done < len)
    {
        ssize_t n = pwrite(fd, buf + done, len - done, offset + (off_t)done);
        if (n == 0)
        {
            return false;
        }
        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return false;
        }

        done += (size_t)n;
    }

    return true;
}

bool write_entry_exact(int fd, const uint8_t *header, const uint8_t *key, size_t key_size,
                       const uint8_t *value, size_t value_size, off_t offset)
{
    size_t done = 0;
    int cur = 0;
    size_t total = ENTRY_HEADER_SIZE + key_size + value_size;

    struct iovec iov[3] =
        {
            {.iov_base = (void *)header, .iov_len = ENTRY_HEADER_SIZE},
            {.iov_base = (void *)key, .iov_len = key_size},
            {.iov_base = (void *)value, .iov_len = value_size},
        };

    while (done < total)
    {
        ssize_t written = pwritev(fd, iov + cur, 3 - cur, offset + done);
        if (written == 0)
        {
            return false;
        }
        if (written < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return false;
        }
        done += (size_t)written;
        if (done < total)
        {
            while (cur < 3 && (size_t)written >= iov[cur].iov_len)
            {
                written -= iov[cur++].iov_len;
            }
            iov[cur].iov_base = (uint8_t *)iov[cur].iov_base + written;
            iov[cur].iov_len -= written;
        }
    }

    return true;
}

bool write_hint_exact(int fd, const uint8_t *header, const uint8_t *key, size_t key_size,
                      const uint8_t *value_pos, off_t offset)
{
    size_t done = 0;
    int cur = 0;
    size_t header_slice_size = ENTRY_HEADER_SIZE - ENTRY_HEADER_TIMESTAMP_OFFSET; // 16
    size_t total = header_slice_size + sizeof(uint32_t) + key_size;

    struct iovec iov[3] =
        {
            {.iov_base = (void *)(header + ENTRY_HEADER_TIMESTAMP_OFFSET), .iov_len = header_slice_size},
            {.iov_base = (void *)value_pos, .iov_len = sizeof(uint32_t)},
            {.iov_base = (void *)key, .iov_len = key_size},
        };

    while (done < total)
    {
        ssize_t written = pwritev(fd, iov + cur, 3 - cur, offset + done);
        if (written == 0)
        {
            return false;
        }
        if (written < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return false;
        }
        done += (size_t)written;
        if (done < total)
        {
            while (cur < 3 && (size_t)written >= iov[cur].iov_len)
            {
                written -= iov[cur++].iov_len;
            }
            iov[cur].iov_base = (uint8_t *)iov[cur].iov_base + written;
            iov[cur].iov_len -= written;
        }
    }

    return true;
}

bool build_file_path(const char *dir_path, const char *suffix, uint32_t file_id, char *out, size_t out_size)
{
    int dir_len = strlen(dir_path);
    bool has_slash = (dir_len > 0 && dir_path[dir_len - 1] == '/');

    int path_n = snprintf(out, out_size, "%s%s%02" PRIu32 "%s", dir_path,
                          has_slash ? "" : "/", file_id, suffix);
    if (path_n < 0 || (size_t)path_n >= out_size)
    {
        return false;
    }

    return true;
}
