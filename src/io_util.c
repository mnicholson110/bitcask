#include "io_util.h"

#include <errno.h>
#include <stdint.h>
#include <unistd.h>

bool pread_exact(int fd, void *buf, size_t len, off_t offset)
{
    uint8_t *p = (uint8_t *)buf;
    size_t done = 0;

    while (done < len)
    {
        ssize_t n = pread(fd, p + done, len - done, offset + (off_t)done);
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
