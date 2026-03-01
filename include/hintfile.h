#ifndef bitcask_hintfile_h
#define bitcask_hintfile_h

#include "keydir.h"
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <sys/types.h>

typedef struct hintfile
{
    int fd;
    uint32_t file_id;
    off_t write_offset;
    char *file_path;
} hintfile_t;

void hintfile_init(hintfile_t *hintfile);

bool hintfile_open(hintfile_t *hintfile, const char *dir_path,
                   uint32_t file_id);

bool hintfile_open_merge(hintfile_t *hintfile, const char *dir_path,
                         uint32_t file_id);

void hintfile_close(hintfile_t *hintfile);

bool hintfile_sync(hintfile_t *hintfile);

// Appends one entry and returns keydir_value_t the value
bool hintfile_append(hintfile_t *hintfile,
                     uint64_t timestamp,
                     const uint8_t *key, uint32_t key_size,
                     const uint8_t *value, uint32_t value_size,
                     keydir_value_t *out_keydir_value);

bool hintfile_read_at(const hintfile_t *hintfile,
                      off_t offset,
                      uint32_t size,
                      uint8_t *out);

bool hintfile_copy_entry(hintfile_t *src, hintfile_t *dest, off_t src_offset, size_t entry_size);

#endif
