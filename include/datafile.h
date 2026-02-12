#ifndef bitcask_datafile_h
#define bitcask_datafile_h

#include "crc.h"
#include "entry.h"
#include "keydir.h"
#include <fcntl.h>
#include <limits.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

typedef enum datafile_mode
{
    DATAFILE_READ,
    DATAFILE_READ_WRITE
} datafile_mode_t;

typedef struct datafile
{
    int fd;
    uint64_t file_id;
    uint64_t write_offset;
    datafile_mode_t mode;
} datafile_t;

void datafile_init(datafile_t *datafile);

bool datafile_open(datafile_t *datafile, const char *path,
                   uint64_t file_id, datafile_mode_t mode);

void datafile_close(datafile_t *datafile);

bool datafile_sync(datafile_t *datafile);

// Appends one entry and returns keydir_value_t the value
bool datafile_append(datafile_t *datafile,
                     uint64_t timestamp,
                     const uint8_t *key, size_t key_size,
                     const uint8_t *value, size_t value_size,
                     keydir_value_t *out_keydir_value);

bool datafile_read_value_at(const datafile_t *datafile,
                            uint64_t value_pos,
                            size_t value_size,
                            uint8_t *out_value);

#endif
