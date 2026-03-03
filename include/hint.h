#ifndef bitcask_hint_h
#define bitcask_hint_h

#include <stdint.h>
#include <sys/types.h>

// Hint header is 20 bytes when encoded
// | ts (8) | key_size (4) | value_size (4) | value_pos (4) |
#define HINT_HEADER_SIZE 20
#define HINT_HEADER_TIMESTAMP_OFFSET 0
#define HINT_HEADER_KEY_SIZE_OFFSET 8
#define HINT_HEADER_VALUE_SIZE_OFFSET 12
#define HINT_HEADER_VALUE_POS_OFFSET 16

typedef struct hint_header
{
    uint64_t timestamp;
    uint32_t key_size;
    uint32_t value_size;
    off_t value_pos;
} hint_header_t;

void hint_header_encode(uint8_t out[HINT_HEADER_SIZE], uint64_t timestamp, uint32_t key_size, uint32_t value_size, off_t value_pos);

void hint_header_decode(hint_header_t *out, const uint8_t in[HINT_HEADER_SIZE]);

#endif
