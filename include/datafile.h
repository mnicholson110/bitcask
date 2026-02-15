#ifndef bitcask_datafile_h
#define bitcask_datafile_h

#include "keydir.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

#define MAX_FILE_SIZE 268435456 // 256MB
#define MAX_KEY_SIZE ((size_t)(1024 * 1024))       // 1 MiB
#define MAX_VALUE_SIZE ((size_t)(10 * 1024 * 1024)) // 10 MiB

typedef enum datafile_mode
{
    DATAFILE_READ,
    DATAFILE_READ_WRITE
} datafile_mode_t;

typedef struct datafile
{
    int fd;
    uint32_t file_id;
    off_t write_offset;
    datafile_mode_t mode;
} datafile_t;

void datafile_init(datafile_t *datafile);

bool datafile_open(datafile_t *datafile, const char *path,
                   uint32_t file_id, datafile_mode_t mode);

void datafile_close(datafile_t *datafile);

bool datafile_sync(datafile_t *datafile);

// Appends one entry and returns keydir_value_t the value
bool datafile_append(datafile_t *datafile,
                     uint64_t timestamp,
                     const uint8_t *key, uint32_t key_size,
                     const uint8_t *value, uint32_t value_size,
                     keydir_value_t *out_keydir_value);

bool datafile_read_value_at(const datafile_t *datafile,
                            uint32_t value_pos,
                            uint32_t value_size,
                            uint8_t *out_value);

#endif
