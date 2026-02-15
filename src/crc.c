#include "../include/crc.h"

bool crc32_validate_buf(uint32_t expected_crc, const uint8_t header[ENTRY_HEADER_SIZE],
                        const uint8_t *key, size_t key_size,
                        const uint8_t *value, size_t value_size)
{
    // compute crc
    uint32_t crc = crc_init();
    crc = crc32_update(crc, header + ENTRY_HEADER_TIMESTAMP_OFFSET, ENTRY_HEADER_SIZE - ENTRY_HEADER_TIMESTAMP_OFFSET);
    crc = crc32_update(crc, key, key_size);
    crc = crc32_update(crc, value, value_size);
    crc = crc32_final(crc);

    return crc == expected_crc;
}

bool crc32_validate(uint32_t expected_crc, const uint8_t header[ENTRY_HEADER_SIZE],
                    const uint8_t *key, size_t key_size,
                    int fd, off_t value_pos, size_t value_size)
{
    // compute crc
    uint32_t crc = crc_init();
    crc = crc32_update(crc, header + ENTRY_HEADER_TIMESTAMP_OFFSET, ENTRY_HEADER_SIZE - ENTRY_HEADER_TIMESTAMP_OFFSET);
    crc = crc32_update(crc, key, key_size);
    if (value_size > 0)
    {
        uint8_t scratch[4096];
        size_t remaining = value_size;
        off_t pos = value_pos;

        while (remaining > 0)
        {
            size_t want = remaining < sizeof(scratch) ? remaining : sizeof(scratch);
            ssize_t n = pread(fd, scratch, want, pos);
            if (n <= 0)
            {
                return false;
            }
            crc = crc32_update(crc, scratch, (size_t)n);

            remaining -= (size_t)n;
            pos += (off_t)n;
        }
    }
    crc = crc32_final(crc);

    return crc == expected_crc;
}

uint32_t crc32_update(uint32_t crc, const uint8_t *buf, size_t n)
{
    for (size_t i = 0; i < n; i++)
    {
        crc = (crc >> 8) ^ crc32_table[(crc ^ buf[i]) & 0xFFu];
    }
    return crc;
}
