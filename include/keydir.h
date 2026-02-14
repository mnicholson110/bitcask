#ifndef bitcask_keydir_h
#define bitcask_keydir_h

#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

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
    uint32_t crc;
    uint64_t file_id;
    uint64_t value_size;
    uint64_t value_pos;
    uint64_t timestamp;
} keydir_value_t;

typedef struct keydir_entry
{
    uint8_t *key;
    size_t key_length;
    keydir_value_t *value;
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

// keydir_put — copies key and value bytes into the keydir.
// Returns true if successful, false otherwise.
bool keydir_put(keydir_t *keydir, const uint8_t *key, size_t key_length, const keydir_value_t *keydir_value);

//  keydir_get — Returns a pointer to internal keydir_value_t if key is found, else NULL.
const keydir_value_t *keydir_get(keydir_t *keydir, const uint8_t *key, size_t key_length);

// keydir_delete — frees the key and value bytes, marks tombstone.
// Returns true if key is found, false otherwise.
bool keydir_delete(keydir_t *keydir, const uint8_t *key, size_t key_length);

#endif
