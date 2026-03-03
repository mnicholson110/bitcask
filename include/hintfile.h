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

void hintfile_delete(hintfile_t *hintfile);

bool hintfile_sync(hintfile_t *hintfile);

bool hintfile_append(hintfile_t *hintfile, uint64_t timestamp,
                     uint32_t key_size, uint32_t value_size,
                     off_t value_pos, const uint8_t *key);

bool hintfile_read_at(const hintfile_t *hintfile,
                      off_t offset,
                      uint32_t size,
                      uint8_t *out);

bool hintfile_populate_keydir(uint32_t id, keydir_t *keydir, const char *dir_path);

#endif
