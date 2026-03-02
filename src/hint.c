#include "../include/hint.h"
#include "../include/io_util.h"

void hint_header_encode(uint8_t out[HINT_HEADER_SIZE],
                        uint64_t timestamp, uint32_t key_size,
                        uint32_t value_size, off_t value_pos)
{
    encode_u64_le(out + HINT_HEADER_TIMESTAMP_OFFSET, timestamp);
    encode_u32_le(out + HINT_HEADER_KEY_SIZE_OFFSET, key_size);
    encode_u32_le(out + HINT_HEADER_VALUE_SIZE_OFFSET, value_size);
    encode_u32_le(out + HINT_HEADER_VALUE_POS_OFFSET, value_pos);
}

void hint_header_decode(hint_header_t *out, const uint8_t in[HINT_HEADER_SIZE])
{
    out->timestamp = decode_u64_le(in + HINT_HEADER_TIMESTAMP_OFFSET);
    out->key_size = decode_u32_le(in + HINT_HEADER_KEY_SIZE_OFFSET);
    out->value_size = decode_u32_le(in + HINT_HEADER_VALUE_SIZE_OFFSET);
    out->value_pos = decode_u32_le(in + HINT_HEADER_VALUE_POS_OFFSET);
}
