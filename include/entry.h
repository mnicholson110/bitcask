#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

// Header is 20 bytes when encoded
// | crc (4) | ts (8) | key_size (4) | value_size (4) |
#define ENTRY_HEADER_SIZE 20
#define ENTRY_HEADER_CRC_OFFSET 0
#define ENTRY_HEADER_TIMESTAMP_OFFSET 4
#define ENTRY_HEADER_KEY_SIZE_OFFSET 12
#define ENTRY_HEADER_VALUE_SIZE_OFFSET 16

typedef struct entry_header
{
    uint32_t crc;
    uint64_t timestamp;
    uint32_t key_size;
    uint32_t value_size;
} entry_header_t;

bool entry_header_encode(uint8_t out[ENTRY_HEADER_SIZE],
                         uint32_t crc, uint64_t timestamp,
                         uint32_t key_size, uint32_t value_size);

bool entry_header_decode(entry_header_t *out, const uint8_t in[ENTRY_HEADER_SIZE]);

static inline void encode_u32_le(uint8_t *buf, uint32_t i)
{
    buf[0] = (uint8_t)(i);
    buf[1] = (uint8_t)(i >> 8);
    buf[2] = (uint8_t)(i >> 16);
    buf[3] = (uint8_t)(i >> 24);
}

static inline uint32_t decode_u32_le(const uint8_t *buf)
{
    return ((uint32_t)buf[0]) |
           ((uint32_t)buf[1] << 8) |
           ((uint32_t)buf[2] << 16) |
           ((uint32_t)buf[3] << 24);
}

static inline void encode_u64_le(uint8_t *buf, uint64_t i)
{
    buf[0] = (uint8_t)(i);
    buf[1] = (uint8_t)(i >> 8);
    buf[2] = (uint8_t)(i >> 16);
    buf[3] = (uint8_t)(i >> 24);
    buf[4] = (uint8_t)(i >> 32);
    buf[5] = (uint8_t)(i >> 40);
    buf[6] = (uint8_t)(i >> 48);
    buf[7] = (uint8_t)(i >> 56);
}

static inline uint64_t decode_u64_le(const uint8_t *buf)
{
    return ((uint64_t)buf[0]) |
           ((uint64_t)buf[1] << 8) |
           ((uint64_t)buf[2] << 16) |
           ((uint64_t)buf[3] << 24) |
           ((uint64_t)buf[4] << 32) |
           ((uint64_t)buf[5] << 40) |
           ((uint64_t)buf[6] << 48) |
           ((uint64_t)buf[7] << 56);
}
