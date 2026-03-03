#ifndef bitcask_io_util_h
#define bitcask_io_util_h

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#define MAX_PATH_LEN 255

bool pread_exact(int fd, uint8_t *buf, size_t len, off_t offset);

bool pwrite_exact(int fd, uint8_t *buf, size_t len, off_t offset);

bool write_entry_exact(int fd, const uint8_t *header, const uint8_t *key, size_t key_size, const uint8_t *value, size_t value_size, off_t offset);

bool write_hint_exact(int fd, const uint8_t *header, const uint8_t *key, size_t key_size, off_t offset);

bool build_file_path(const char *dir_path, const char *suffix, uint32_t file_id, char *out, size_t out_size);

bool scan_dir(const char *dir_path, bool can_write, uint32_t **datafiles, size_t *count, uint32_t **hints, size_t *hint_count, bool *locked);

void unlock_dir(const char *dir_path);

bool lock_dir(const char *dir_path);

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

#endif
