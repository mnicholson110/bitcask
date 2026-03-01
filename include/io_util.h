#ifndef bitcask_io_util_h
#define bitcask_io_util_h

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#define MAX_PATH_LEN 255

bool pread_exact(int fd, uint8_t *buf, size_t len, off_t offset);

bool pwrite_exact(int fd, uint8_t *buf, size_t len, off_t offset);

bool write_entry_exact(int fd, const uint8_t *header, const uint8_t *key, size_t key_size,
                       const uint8_t *value, size_t value_size, off_t offset);

bool write_hint_exact(int fd, const uint8_t *header, const uint8_t *key, size_t key_size,
                      const uint8_t *value_pos, off_t offset);

bool build_file_path(const char *dir_path, const char *suffix, uint32_t file_id, char *out, size_t out_size);

#endif
