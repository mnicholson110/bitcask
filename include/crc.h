#include <stddef.h>
#include <stdint.h>

static inline uint32_t crc_init()
{
    return 0xFFFFFFFF;
}

uint32_t crc32_update(uint32_t crc, const uint8_t *val, size_t n);

static inline uint32_t crc32_final(uint32_t crc)
{
    return ~crc;
}
