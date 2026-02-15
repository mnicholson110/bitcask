#ifndef bitcask_io_util_h
#define bitcask_io_util_h

#include <stdbool.h>
#include <stddef.h>
#include <sys/types.h>

bool pread_exact(int fd, void *buf, size_t len, off_t offset);

#endif
