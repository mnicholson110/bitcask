#ifndef bitcask_h
#define bitcask_h

#include "datafile.h"
#include "keydir.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

typedef enum bitcask_opts
{
    BITCASK_READ_ONLY = 0,
    BITCASK_READ_WRITE = 1,
    BITCASK_SYNC_ON_PUT = 2
} bitcask_opts_t;

typedef struct bitcask_handle
{
    keydir_t keydir;
    datafile_t active_file;
    datafile_t *inactive_files;
    size_t file_count;
    size_t inactive_capacity;
    char *dir_path;
    uint8_t opts;
} bitcask_handle_t;

bool bitcask_open(bitcask_handle_t *bitcask, const char *dir_path,
                  uint8_t opts);

bool bitcask_get(bitcask_handle_t *bitcask, const uint8_t *key,
                 size_t key_size, uint8_t **out, size_t *out_size);

bool bitcask_put(bitcask_handle_t *bitcask, const uint8_t *key,
                 size_t key_size, const uint8_t *value, size_t value_size);

bool bitcask_delete(bitcask_handle_t *bitcask, const uint8_t *key, size_t key_size);

bool bitcask_sync(bitcask_handle_t *bitcask);

void bitcask_close(bitcask_handle_t *bitcask);

// eventually:
// bitcask_list_keys()
// bitcask_merge()
// bitcask_fold()

#endif
