#include "../include/keydir.h"

void init_table(table_t *table)
{
    table->count = 0;
    table->capacity = 0;
    table->entries = NULL;
}
void free_table(table_t *table)
{
    for (size_t i = 0; i < table->capacity; i++)
    {
        free(table->entries[i].key);
        free(table->entries[i].value);
    }
    free(table->entries);
    init_table(table);
}

static uint32_t hash_bytes(const uint8_t *key, size_t length)
{
    uint32_t hash = 2166136261u;
    for (size_t i = 0; i < length; i++)
    {
        hash ^= key[i];
        hash *= 16777619;
    }
    return hash;
}

static table_entry_t *find_entry(table_entry_t *entries, uint64_t capacity, const uint8_t *key, size_t key_length)
{
    if (key_length < 1)
    {
        return NULL;
    }

    uint32_t index = hash_bytes(key, key_length) % capacity;
    table_entry_t *tombstone = NULL;
    for (;;)
    {
        table_entry_t *entry = entries + index;
        if (entry->key == NULL)
        {
            if (entry->state != ENTRY_TOMBSTONE)
            {
                return tombstone != NULL ? tombstone : entry;
            }
            else
            {
                assert(entry->state == ENTRY_TOMBSTONE);
                tombstone = entry;
            }
        }
        else if (key_length == entry->key_length && !memcmp(entry->key, key, key_length))
        {
            return entry;
        }

        index = (index + 1) % capacity;
    }
}

static void adjust_capacity(table_t *table, uint64_t capacity)
{
    table_entry_t *entries = malloc(sizeof(table_entry_t) * capacity);
    if (entries == NULL)
    {
        perror("malloc failure (perror)");
        exit(1);
    }
    for (size_t i = 0; i < capacity; i++)
    {
        entries[i].key = NULL;
        entries[i].value = NULL;
        entries[i].state = ENTRY_EMPTY;
    }

    table->count = 0;
    for (size_t i = 0; i < table->capacity; i++)
    {
        table_entry_t *entry = table->entries + i;
        if (entry->state == ENTRY_TOMBSTONE || entry->state == ENTRY_EMPTY)
        {
            assert(entry->key == NULL);
            continue;
        }

        table_entry_t *dest = find_entry(entries, capacity, entry->key, entry->key_length);
        dest->key = entry->key;
        dest->key_length = entry->key_length;
        dest->value = entry->value;
        dest->state = entry->state;
        table->count++;
    }

    free(table->entries);
    table->entries = entries;
    table->capacity = capacity;
}

bool table_put(table_t *table, const uint8_t *key, size_t key_length, const keydir_value_t *keydir_value)
{
    if (key_length < 1 || keydir_value == NULL)
    {
        return false;
    }

    if (table->count + 1 > table->capacity * TABLE_MAX_LOAD)
    {
        uint64_t capacity = table->capacity < 8 ? 8 : table->capacity * 2;
        adjust_capacity(table, capacity);
    }

    table_entry_t *entry = find_entry(table->entries, table->capacity, key, key_length);

    switch (entry->state)
    {
    case ENTRY_EMPTY:
        table->count++;
        /* fall through */
    case ENTRY_TOMBSTONE:
        assert(entry->key == NULL);
        entry->key = malloc(sizeof(uint8_t) * key_length);
        if (entry->key == NULL)
        {
            perror("malloc failure (perror)");
            exit(1);
        }

        assert(entry->value == NULL);
        entry->value = malloc(sizeof(keydir_value_t));
        if (entry->value == NULL)
        {
            perror("malloc failure (perror)");
            exit(1);
        }
        break;
    case ENTRY_OCCUPIED:
        break;
    }

    if (entry->state != ENTRY_OCCUPIED)
    {
        memcpy(entry->key, key, key_length);
        entry->key_length = key_length;
    }

    memcpy(entry->value, keydir_value, sizeof(keydir_value_t));

    entry->state = ENTRY_OCCUPIED;

    return true;
}

const keydir_value_t *table_get(table_t *table, const uint8_t *key, size_t key_length)
{
    if (table->count == 0 || key_length < 1)
    {
        return NULL;
    }

    table_entry_t *entry = find_entry(table->entries, table->capacity, key, key_length);
    if (entry->key == NULL)
    {
        return NULL;
    }

    return entry->value;
}

bool table_delete(table_t *table, const uint8_t *key, size_t key_length)
{
    if (table->count == 0 || key_length < 1)
    {
        return false;
    }

    table_entry_t *entry = find_entry(table->entries, table->capacity, key, key_length);
    if (entry->state == ENTRY_OCCUPIED)
    {
        free(entry->key);
        entry->key = NULL;
        free(entry->value);
        entry->value = NULL;
        entry->state = ENTRY_TOMBSTONE;
        return true;
    }

    return false;
}
