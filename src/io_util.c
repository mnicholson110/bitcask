#include "../include/io_util.h"
#include "../include/entry.h"
#include <errno.h>
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

bool write_entry_exact(int fd,
                       const void *header,
                       const void *key,
                       size_t key_size,
                       const void *value,
                       size_t value_size,
                       off_t offset,
                       size_t total)
{
    size_t done = 0;
    int cur = 0;

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
