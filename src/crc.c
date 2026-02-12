#include "../include/crc.h"

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
