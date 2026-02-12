#include "../include/entry.h"

bool entry_header_encode(uint8_t out[ENTRY_HEADER_SIZE],
                         uint32_t crc, uint64_t timestamp,
                         uint32_t key_size, uint32_t value_size)
{
    if (out == NULL)
    {
        return false;
    }

    encode_u32_le(out + ENTRY_HEADER_CRC_OFFSET, crc);
    encode_u64_le(out + ENTRY_HEADER_TIMESTAMP_OFFSET, timestamp);
    encode_u32_le(out + ENTRY_HEADER_KEY_SIZE_OFFSET, key_size);
    encode_u32_le(out + ENTRY_HEADER_VALUE_SIZE_OFFSET, value_size);

    return true;
}

bool entry_header_decode(entry_header_t *out, const uint8_t in[ENTRY_HEADER_SIZE])
{
    if (out == NULL || in == NULL)
    {
        return false;
    }

    out->crc = decode_u32_le(in + ENTRY_HEADER_CRC_OFFSET);
    out->timestamp = decode_u64_le(in + ENTRY_HEADER_TIMESTAMP_OFFSET);
    out->key_size = decode_u32_le(in + ENTRY_HEADER_KEY_SIZE_OFFSET);
    out->value_size = decode_u32_le(in + ENTRY_HEADER_VALUE_SIZE_OFFSET);

    return true;
}
