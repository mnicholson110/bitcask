#include <assert.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define TABLE_MAX_LOAD 0.75

typedef enum entry_state
{
    ENTRY_EMPTY,
    ENTRY_OCCUPIED,
    ENTRY_TOMBSTONE
} entry_state_t;

// 32 bytes per keydir_value_t
typedef struct keydir_value
{
    uint64_t file_id;
    size_t value_size;
    uint64_t value_pos;
    uint64_t timestamp;
} keydir_value_t;

typedef struct table_entry
{
    uint8_t *key;
    size_t key_length;
    keydir_value_t *value;
    entry_state_t state;
} table_entry_t;

typedef struct table
{
    uint64_t count;
    uint64_t capacity;
    table_entry_t *entries;
} table_t;

void init_table(table_t *table);
void free_table(table_t *table);

// table_put — copies key and value bytes into the table.
// Returns true if successful, false otherwise.
bool table_put(table_t *table, const uint8_t *key, size_t key_length, const keydir_value_t *keydir_value);

//  table_get — Returns a pointer to internal keydir_value_t if key is found, else NULL.
const keydir_value_t *table_get(table_t *table, const uint8_t *key, size_t key_length);

// table_delete — frees the key and value bytes, marks tombstone.
// Returns true if key is found, false otherwise.
bool table_delete(table_t *table, const uint8_t *key, size_t key_length);
