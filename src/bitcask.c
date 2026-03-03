#include "../include/bitcask.h"
#include "../include/entry.h"
#include "../include/hintfile.h"
#include "../include/io_util.h"
#include <dirent.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>

static inline bool can_write(uint8_t opts)
{
    return (opts & BITCASK_READ_WRITE) != 0;
}

static inline bool sync_on_put(uint8_t opts)
{
    return (opts & BITCASK_SYNC_ON_PUT) != 0;
}

static bool rotate_active_file(bitcask_handle_t *bitcask)
{
    if (!bitcask_sync(bitcask))
    {
        return false;
    }

    if (bitcask->inactive_count >= bitcask->inactive_capacity)
    {
        void *tmp = realloc(bitcask->inactive_files, sizeof(datafile_t) * bitcask->inactive_capacity * 2);
        if (tmp == NULL)
        {
            return false;
        }
        bitcask->inactive_files = tmp;
        bitcask->inactive_capacity *= 2;
    }
    // close and reopen current active file as read-only
    uint32_t old_active_id = bitcask->active_file.file_id;
    datafile_close(&bitcask->active_file);

    if (!datafile_open(&bitcask->inactive_files[bitcask->inactive_count], bitcask->dir_path, old_active_id, DATAFILE_READ))
    {
        return false;
    };

    // open new active file
    if (!datafile_open(&bitcask->active_file, bitcask->dir_path, bitcask->next_file_id, DATAFILE_READ_WRITE))
    {
        return false;
    }
    bitcask->inactive_count++;
    bitcask->next_file_id++;

    return true;
}

bool bitcask_open(bitcask_handle_t *bitcask, const char *dir_path,
                  uint8_t opts)
{
    if ((opts & ~(BITCASK_READ_WRITE | BITCASK_SYNC_ON_PUT)) != 0)
    {
        return false;
    }
    bitcask->inactive_files = NULL;
    bitcask->inactive_capacity = 0;
    bitcask->inactive_count = 0;
    bitcask->keydir.count = 0;
    bitcask->keydir.capacity = 0;
    bitcask->keydir.entries = NULL;
    bitcask->next_file_id = 0;
    bitcask->opts = opts;
    datafile_init(&bitcask->active_file);

    uint32_t *ids, *hints;
    size_t count = 0, hint_count = 0;

    if (!scan_datafiles_and_hintfiles(dir_path, bitcask->opts, &ids, &count, &hints, &hint_count))
    {
        return false;
    }

    if (count == 0 && !can_write(opts))
    {
        free(ids);
        free(hints);
        return false;
    }

    // allocate enough space for the number of inactive files to double (+1)
    bitcask->inactive_capacity = count == 0 ? 2 : count * 2;
    bitcask->inactive_files = malloc(sizeof(datafile_t) * bitcask->inactive_capacity);
    if (bitcask->inactive_files == NULL)
    {
        free(ids);
        free(hints);
        return false;
    }

    bitcask->dir_path = strdup(dir_path);
    if (bitcask->dir_path == NULL)
    {
        free(ids);
        free(hints);
        bitcask_close(bitcask);
        return false;
    }
    // set existing files as inactive
    for (size_t i = 0; i < count; i++)
    {
        datafile_init(&bitcask->inactive_files[i]);
        if (!datafile_open(&bitcask->inactive_files[i], bitcask->dir_path, ids[i], DATAFILE_READ))
        {
            free(ids);
            bitcask_close(bitcask);
            return false;
        }
        bitcask->inactive_count++;
    }

    // rebuild keydir
    keydir_init(&bitcask->keydir);
    // scan files from inactive[0] thru to active file and rebuild keydir
    size_t cur_hint = 0;
    for (size_t i = 0; i < count; i++)
    {
        if (cur_hint < hint_count && bitcask->inactive_files[i].file_id == hints[cur_hint])
        {
            if (!hintfile_populate_keydir(hints[cur_hint], &bitcask->keydir, bitcask->dir_path))
            {
                free(ids);
                free(hints);
                bitcask_close(bitcask);
                return false;
            }
            cur_hint++;
        }
        else if (!datafile_populate_keydir(&bitcask->inactive_files[i], &bitcask->keydir))
        {
            free(ids);
            free(hints);
            bitcask_close(bitcask);
            return false;
        }
    }

    // if RW, open a new file for writing
    if (can_write(opts))
    {
        bitcask->next_file_id = count == 0 ? 1 : ids[count - 1] + 1;

        // open datafile
        if (!datafile_open(&bitcask->active_file, bitcask->dir_path, bitcask->next_file_id, DATAFILE_READ_WRITE))
        {
            free(ids);
            free(hints);
            bitcask_close(bitcask);
            return false;
        }
        bitcask->next_file_id++;
    }

    free(ids);
    free(hints);
    return true;
}

bool bitcask_get(bitcask_handle_t *bitcask, const uint8_t *key,
                 size_t key_size, uint8_t **out, size_t *out_size)
{
    if (key_size == 0 || key_size > MAX_KEY_SIZE)
    {
        return false;
    }

    const keydir_value_t *entry = keydir_get(&bitcask->keydir, key, key_size);
    if (entry == NULL)
    {
        return false;
    }

    // find the file via file_id
    datafile_t *target = NULL;
    if (bitcask->active_file.file_id == entry->file_id)
    {
        target = &bitcask->active_file;
    }
    else
    {
        for (size_t i = 0; i < bitcask->inactive_count; i++)
        {
            if (bitcask->inactive_files[i].file_id == entry->file_id)
            {
                target = &bitcask->inactive_files[i];
                break;
            }
        }
    }

    if (target == NULL)
    {
        return false;
    }

    *out = malloc(entry->value_size);
    if (*out == NULL)
    {
        return false;
    }
    *out_size = entry->value_size;

    if (!datafile_read_at(target, entry->value_pos, entry->value_size, *out))
    {
        free(*out);
        *out = NULL;
        *out_size = 0;
        return false;
    }

    return true;
}

bool bitcask_put(bitcask_handle_t *bitcask, const uint8_t *key,
                 size_t key_size, const uint8_t *value, size_t value_size)
{
    if (!can_write(bitcask->opts))
    {
        // this is a read-only handle, put not allowed
        return false;
    }
    if (key_size == 0 || key_size > MAX_KEY_SIZE || value_size > MAX_VALUE_SIZE)
    {
        return false;
    }

    if ((size_t)bitcask->active_file.write_offset > MAX_FILE_SIZE - ENTRY_HEADER_SIZE - key_size - value_size)
    {
        if (!rotate_active_file(bitcask))
        {
            return false;
        }
    }

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    uint64_t timestamp = (uint64_t)ts.tv_sec * 1000000000 + (uint64_t)ts.tv_nsec;

    keydir_value_t out;

    if (!datafile_append(&bitcask->active_file, timestamp, key, key_size, value, value_size, &out))
    {
        return false;
    };

    if (value_size == 0)
    {
        keydir_delete(&bitcask->keydir, key, key_size);
    }
    else if (!keydir_put(&bitcask->keydir, key, key_size, &out))
    {
        return false;
    }

    if (sync_on_put(bitcask->opts))
    {
        return bitcask_sync(bitcask);
    }
    return true;
}

bool bitcask_delete(bitcask_handle_t *bitcask, const uint8_t *key, size_t key_size)
{
    return bitcask_put(bitcask, key, key_size, NULL, 0);
}

bool bitcask_sync(bitcask_handle_t *bitcask)
{
    if (!datafile_sync(&bitcask->active_file))
    {
        return false;
    }
    return true;
}

void bitcask_close(bitcask_handle_t *bitcask)
{
    bitcask_sync(bitcask);

    for (size_t i = 0; i < bitcask->inactive_count; i++)
    {
        datafile_close(&bitcask->inactive_files[i]);
    }
    if (can_write(bitcask->opts))
    {
        datafile_close(&bitcask->active_file);
    }

    if (bitcask->inactive_files != NULL)
    {
        free(bitcask->inactive_files);
        bitcask->inactive_files = NULL;
    }
    if (bitcask->dir_path != NULL)
    {
        free(bitcask->dir_path);
        bitcask->dir_path = NULL;
    }
    bitcask->inactive_count = 0;
    keydir_free(&bitcask->keydir);
}

bool bitcask_merge(bitcask_handle_t *bitcask)
{
    if (!can_write(bitcask->opts) || bitcask->inactive_count == 0)
    {
        return false;
    }

    // bitcask->inactive_capacity is safe size because
    // worst-case scenario is 0 merging, meaning
    // each file is simply copied as-is
    datafile_t *new_inactive = malloc(sizeof(datafile_t) * bitcask->inactive_capacity);
    hintfile_t *merge_hintfiles = malloc(sizeof(hintfile_t) * bitcask->inactive_capacity);
    if (new_inactive == NULL || merge_hintfiles == NULL)
    {
        if (new_inactive != NULL)
        {
            free(new_inactive);
        }
        if (merge_hintfiles != NULL)
        {
            free(merge_hintfiles);
        }
        return false;
    }

    size_t merge_idx = 0;

    if (!datafile_open_merge(&new_inactive[merge_idx], bitcask->dir_path, bitcask->next_file_id, DATAFILE_READ_WRITE))
    {
        return false;
    };
    if (!hintfile_open_merge(&merge_hintfiles[merge_idx], bitcask->dir_path, bitcask->next_file_id))
    {
        datafile_delete(&new_inactive[merge_idx]);
        return false;
    };

    // iterate over inactive files
    for (size_t i = 0; i < bitcask->inactive_count; i++)
    {
        off_t offset = 0;
        datafile_t *cur = &bitcask->inactive_files[i];

        while (offset < cur->write_offset)
        {
            entry_header_t header;
            uint8_t hdr_buf[ENTRY_HEADER_SIZE];
            if (!datafile_read_at(cur, offset, ENTRY_HEADER_SIZE, hdr_buf))
            {
                for (size_t j = 0; j <= merge_idx; j++)
                {
                    datafile_delete(&new_inactive[j]);
                    hintfile_delete(&merge_hintfiles[j]);
                }
                free(new_inactive);
                free(merge_hintfiles);
                return false;
            }

            entry_header_decode(&header, hdr_buf);

            offset += ENTRY_HEADER_SIZE;

            uint8_t *key = malloc(header.key_size);
            if (key == NULL)
            {
                for (size_t j = 0; j <= merge_idx; j++)
                {
                    datafile_delete(&new_inactive[j]);
                    hintfile_delete(&merge_hintfiles[j]);
                }
                free(new_inactive);
                free(merge_hintfiles);
                return false;
            }

            if (!datafile_read_at(cur, offset, header.key_size, key))
            {
                for (size_t j = 0; j <= merge_idx; j++)
                {
                    datafile_delete(&new_inactive[j]);
                    hintfile_delete(&merge_hintfiles[j]);
                }
                free(new_inactive);
                free(merge_hintfiles);
                free(key);
                return false;
            }

            offset += header.key_size;

            // check if entry is "live"
            const keydir_value_t *old_keydir_value = keydir_get(&bitcask->keydir, key, header.key_size);
            if (old_keydir_value == NULL || old_keydir_value->file_id != cur->file_id || header.value_size == 0 || old_keydir_value->value_pos != offset)
            {
                offset += header.value_size;
                free(key);
                continue;
            }

            // rotation
            if ((size_t)new_inactive[merge_idx].write_offset > MAX_FILE_SIZE - ENTRY_HEADER_SIZE - header.key_size - header.value_size)
            {
                // close active data
                datafile_close(&new_inactive[merge_idx]);

                // reopen data (read-only)
                if (!datafile_open_merge(&new_inactive[merge_idx], bitcask->dir_path, bitcask->next_file_id + merge_idx, DATAFILE_READ))
                {
                    // cleanup
                    return false;
                }

                // move to next idx
                merge_idx++;

                // open new data + hint
                if (!datafile_open_merge(&new_inactive[merge_idx], bitcask->dir_path, bitcask->next_file_id + merge_idx, DATAFILE_READ_WRITE))
                {
                    // cleanup
                    return false;
                }

                if (!hintfile_open_merge(&merge_hintfiles[merge_idx], bitcask->dir_path, bitcask->next_file_id + merge_idx))
                {
                    // cleanup
                    return false;
                };
            }

            if (!datafile_copy_entry(cur, &new_inactive[merge_idx], offset - header.key_size - ENTRY_HEADER_SIZE, ENTRY_HEADER_SIZE + header.key_size + header.value_size))
            {
                for (size_t j = 0; j <= merge_idx; j++)
                {
                    datafile_delete(&new_inactive[j]);
                    hintfile_delete(&merge_hintfiles[j]);
                }
                free(new_inactive);
                free(merge_hintfiles);
                free(key);
                return false;
            }

            if (!hintfile_append(&merge_hintfiles[merge_idx], header.timestamp, header.key_size, header.value_size, new_inactive[merge_idx].write_offset - header.value_size, key))
            {
                for (size_t j = 0; j <= merge_idx; j++)
                {
                    datafile_delete(&new_inactive[j]);
                    hintfile_delete(&merge_hintfiles[j]);
                }
                free(new_inactive);
                free(merge_hintfiles);
                free(key);
                return false;
            }

            free(key);
            offset += header.value_size;
        }
    }

    // close existing datafile, reopen as read-only
    datafile_close(&new_inactive[merge_idx]);
    if (!datafile_open_merge(&new_inactive[merge_idx], bitcask->dir_path, bitcask->next_file_id + merge_idx, DATAFILE_READ))
    {
        for (size_t i = 0; i <= merge_idx; i++)
        {
            datafile_delete(&new_inactive[i]);
            hintfile_delete(&merge_hintfiles[i]);
        }
        free(new_inactive);
        free(merge_hintfiles);
        return false;
    }

    if (new_inactive[merge_idx].write_offset == 0)
    {
        datafile_delete(&new_inactive[merge_idx]);
        hintfile_delete(&merge_hintfiles[merge_idx]);
        if (merge_idx == 0)
        {
            free(new_inactive);
            free(merge_hintfiles);
            return true;
        }
        merge_idx--;
    }

    // atomically swap/remove
    // first, rename
    char *new_file_path = NULL;
    char *new_hint_path = NULL;

    for (size_t i = 0; i <= merge_idx; i++)
    {
        new_file_path = strdup(new_inactive[i].file_path);
        // check strdup call
        new_file_path[strlen(new_file_path) - 6] = '\0';
        if (rename(new_inactive[i].file_path, new_file_path) != 0)
        {
            for (size_t j = 0; j <= merge_idx; j++)
            {
                datafile_delete(&new_inactive[j]);
                hintfile_delete(&merge_hintfiles[j]);
            }
            free(new_file_path);
            free(new_inactive);
            free(merge_hintfiles);
            return false;
        }
        new_inactive[i].file_path[strlen(new_inactive[i].file_path) - 6] = '\0';
        free(new_file_path);

        new_hint_path = strdup(merge_hintfiles[i].file_path);
        // check strdup call
        new_hint_path[strlen(new_hint_path) - 6] = '\0';
        if (rename(merge_hintfiles[i].file_path, new_hint_path) != 0)
        {
            for (size_t j = 0; j <= merge_idx; j++)
            {
                datafile_delete(&new_inactive[j]);
                hintfile_delete(&merge_hintfiles[j]);
            }
            free(new_hint_path);
            free(new_inactive);
            free(merge_hintfiles);
            return false;
        }
        merge_hintfiles[i].file_path[strlen(merge_hintfiles[i].file_path) - 6] = '\0';
        free(new_hint_path);
    }

    // swap inactives, free old

    datafile_t *old_inactive = bitcask->inactive_files;
    size_t old_inactive_count = bitcask->inactive_count;

    bitcask->inactive_files = new_inactive;
    bitcask->inactive_count = merge_idx + 1;

    for (size_t i = 0; i < old_inactive_count; i++)
    {
        datafile_delete(&old_inactive[i]);
    }
    free(old_inactive);

    // for now just rebuild keydir
    for (size_t i = 0; i < bitcask->inactive_count; i++)
    {
        // need to add check for num of hint files
        // but should be equal to inactive_count here
        if (bitcask->inactive_files[i].file_id == merge_hintfiles[i].file_id)
        {
            if (!hintfile_populate_keydir(merge_hintfiles[i].file_id, &bitcask->keydir, bitcask->dir_path))
            {
                for (size_t i = 0; i < bitcask->inactive_count; i++)
                {
                    hintfile_close(&merge_hintfiles[i]);
                }
                free(merge_hintfiles);
                return false;
            }
        }
        else if (!datafile_populate_keydir(&bitcask->inactive_files[i], &bitcask->keydir))
        {
            for (size_t i = 0; i < bitcask->inactive_count; i++)
            {
                hintfile_close(&merge_hintfiles[i]);
            }
            free(merge_hintfiles);
            return false;
        }
    }

    for (size_t i = 0; i < bitcask->inactive_count; i++)
    {
        hintfile_close(&merge_hintfiles[i]);
    }
    free(merge_hintfiles);

    bitcask->next_file_id += (merge_idx + 1);

    return true;
}
