#ifndef bitcask_keydir_h
#define bitcask_keydir_h

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#define TABLE_MAX_LOAD_NUM 3
#define TABLE_MAX_LOAD_DEN 4

typedef enum entry_state
{
    ENTRY_EMPTY,
    ENTRY_OCCUPIED,
    ENTRY_TOMBSTONE
} entry_state_t;

typedef struct keydir_value
{
    uint32_t file_id;
    uint32_t value_size;
    uint32_t value_pos;
    uint64_t timestamp;
} keydir_value_t;

typedef struct keydir_entry
{
    uint8_t *key;
    size_t key_length;
    keydir_value_t value;
    entry_state_t state;
} keydir_entry_t;

typedef struct keydir
{
    size_t count;
    size_t capacity;
    keydir_entry_t *entries;
} keydir_t;

void keydir_init(keydir_t *keydir);
void keydir_free(keydir_t *keydir);

bool keydir_put(keydir_t *keydir, const uint8_t *key, size_t key_length, const keydir_value_t *keydir_value);

const keydir_value_t *keydir_get(const keydir_t *keydir, const uint8_t *key, size_t key_length);

bool keydir_delete(keydir_t *keydir, const uint8_t *key, size_t key_length);

#endif
