#include "../include/keydir.h"
#include <assert.h>
#include <stdlib.h>
#include <string.h>

void keydir_init(keydir_t *keydir)
{
    keydir->count = 0;
    keydir->capacity = 0;
    keydir->entries = NULL;
}
void keydir_free(keydir_t *keydir)
{
    if (keydir != NULL && keydir->entries != NULL)
    {
        for (size_t i = 0; i < keydir->capacity; i++)
        {
            free(keydir->entries[i].key);
        }
        free(keydir->entries);
        keydir_init(keydir);
    }
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

static keydir_entry_t *find_entry(keydir_entry_t *entries, size_t capacity, const uint8_t *key, size_t key_length)
{
    if (key_length < 1)
    {
        return NULL;
    }

    size_t index = ((size_t)hash_bytes(key, key_length)) % capacity;
    keydir_entry_t *tombstone = NULL;
    for (;;)
    {
        keydir_entry_t *entry = entries + index;
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

static bool adjust_capacity(keydir_t *keydir, size_t capacity)
{
    keydir_entry_t *entries = malloc(sizeof(keydir_entry_t) * capacity);
    if (entries == NULL)
    {
        return false;
    }
    for (size_t i = 0; i < capacity; i++)
    {
        entries[i].key = NULL;
        entries[i].state = ENTRY_EMPTY;
    }

    keydir->count = 0;
    for (size_t i = 0; i < keydir->capacity; i++)
    {
        keydir_entry_t *entry = keydir->entries + i;
        if (entry->state == ENTRY_TOMBSTONE || entry->state == ENTRY_EMPTY)
        {
            assert(entry->key == NULL);
            continue;
        }

        keydir_entry_t *dest = find_entry(entries, capacity, entry->key, entry->key_length);
        dest->key = entry->key;
        dest->key_length = entry->key_length;
        dest->value = entry->value;
        dest->state = entry->state;
        keydir->count++;
    }

    free(keydir->entries);
    keydir->entries = entries;
    keydir->capacity = capacity;
    return true;
}

bool keydir_put(keydir_t *keydir, const uint8_t *key, size_t key_length, const keydir_value_t *keydir_value)
{
    if (key_length < 1 || keydir_value == NULL)
    {
        return false;
    }

    if (keydir->count + 1 > (keydir->capacity * TABLE_MAX_LOAD_NUM) / TABLE_MAX_LOAD_DEN)
    {
        size_t capacity = keydir->capacity < 8 ? 8 : keydir->capacity * 2;
        if (!adjust_capacity(keydir, capacity))
        {
            return false;
        };
    }

    keydir_entry_t *entry = find_entry(keydir->entries, keydir->capacity, key, key_length);

    switch (entry->state)
    {
    case ENTRY_EMPTY:
        keydir->count++;
        /* fall through */
    case ENTRY_TOMBSTONE:
        assert(entry->key == NULL);
        entry->key = malloc(sizeof(uint8_t) * key_length);
        if (entry->key == NULL)
        {
            return false;
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

    entry->value = *keydir_value;

    entry->state = ENTRY_OCCUPIED;

    return true;
}

const keydir_value_t *keydir_get(const keydir_t *keydir, const uint8_t *key, size_t key_length)
{
    if (keydir->count == 0 || key_length < 1)
    {
        return NULL;
    }

    keydir_entry_t *entry = find_entry(keydir->entries, keydir->capacity, key, key_length);
    if (entry->key == NULL)
    {
        return NULL;
    }

    return &entry->value;
}

bool keydir_delete(keydir_t *keydir, const uint8_t *key, size_t key_length)
{
    if (keydir->count == 0 || key_length < 1)
    {
        return false;
    }

    keydir_entry_t *entry = find_entry(keydir->entries, keydir->capacity, key, key_length);
    if (entry->state == ENTRY_OCCUPIED)
    {
        free(entry->key);
        entry->key = NULL;
        entry->state = ENTRY_TOMBSTONE;
        return true;
    }

    return false;
}
