#ifndef bitcask_crc_h
#define bitcask_crc_h

#include "entry.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>
#include <unistd.h>

static inline uint32_t crc_init(void)
{
    return 0xFFFFFFFF;
}

uint32_t crc32_update(uint32_t crc, const uint8_t *val, size_t n);

static inline uint32_t crc32_final(uint32_t crc)
{
    return ~crc;
}

bool crc32_validate(uint32_t expected_crc, const uint8_t header[ENTRY_HEADER_SIZE],
                    const uint8_t *key, size_t key_size,
                    int fd, uint64_t value_pos, uint64_t value_size);

#endif
