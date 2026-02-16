#ifndef bitcask_io_util_h
#define bitcask_io_util_h

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

bool pread_exact(int fd, uint8_t *buf, size_t len, off_t offset);

bool pwrite_exact(int fd, uint8_t *buf, size_t len, off_t offset);

bool write_entry_exact(int fd,
                       const void *header,
                       const void *key,
                       size_t key_size,
                       const void *value,
                       size_t value_size,
                       off_t offset,
                       size_t total);

#endif
