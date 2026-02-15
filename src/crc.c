#include "../include/crc.h"

bool crc32_validate_buf(const uint8_t header[ENTRY_HEADER_SIZE],
                        const uint8_t *key, uint32_t key_size,
                        const uint8_t *value, uint32_t value_size)
{
    // compute crc
    uint32_t crc = crc_init();
    crc = crc32_update(crc, header + ENTRY_HEADER_TIMESTAMP_OFFSET, ENTRY_HEADER_SIZE - ENTRY_HEADER_TIMESTAMP_OFFSET);
    crc = crc32_update(crc, key, key_size);
    crc = crc32_update(crc, value, value_size);
    crc = crc32_final(crc);

    return crc == decode_u32_le(header);
}

uint32_t crc32_update(uint32_t crc, const uint8_t *buf, size_t n)
{
    for (size_t i = 0; i < n; i++)
    {
        crc = (crc >> 8) ^ crc32_table[(crc ^ buf[i]) & 0xFFu];
    }
    return crc;
}
