#include "../include/crc.h"

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
        uint8_t *value = malloc(value_size);
        if (value == NULL)
        {
            return false;
        }
        size_t total = 0;
        while (total < value_size)
        {
            ssize_t n = pread(fd, value + total, (size_t)value_size - total, (value_pos + total));
            if (n <= 0)
            {
                free(value);
                return false;
            }
            total += (size_t)n;
        }
        crc = crc32_update(crc, value, value_size);
        free(value);
    }
    crc = crc32_final(crc);

    return crc == expected_crc;
}

uint32_t crc32_update(uint32_t crc, const uint8_t *val, size_t n)
{
    for (size_t i = 0; i < n; i++)
    {
        crc ^= val[i];
        for (size_t j = 0; j < 8; j++)
        {
            if ((crc & 1) == 1)
            {
                crc = (crc >> 1) ^ 0xEDB88320;
            }
            else
            {
                crc >>= 1;
            }
        };
    };
    return crc;
}
