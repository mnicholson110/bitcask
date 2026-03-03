#ifndef bitcask_entry_h
#define bitcask_entry_h

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

void entry_header_encode(uint8_t out[ENTRY_HEADER_SIZE], uint32_t crc, uint64_t timestamp, uint32_t key_size, uint32_t value_size);

void entry_header_decode(entry_header_t *out, const uint8_t in[ENTRY_HEADER_SIZE]);

#endif
