#include "../include/bitcask.h"
#include "../include/crc.h"
#include "../include/entry.h"
#include "../include/hintfile.h"
#include "../include/io_util.h"
#include <dirent.h>
#include <errno.h>
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

static int cmp_u32(const void *a, const void *b)
{
    uint32_t x = *(const uint32_t *)a;
    uint32_t y = *(const uint32_t *)b;
    return (x > y) - (x < y); // avoids overflow from subtraction
}

static bool populate_keydir_from_hint(uint32_t id, bitcask_handle_t *bitcask)
{
    int path_max = strlen(bitcask->dir_path) + 40;
    char hint_path[path_max];
    if (!build_file_path(bitcask->dir_path, ".hint", id, hint_path, path_max))
    {
        return false;
    }

    int fd = open(hint_path, O_RDWR, 0644);
    if (fd < 0)
    {
        return false;
    }

    struct stat st;
    if (fstat(fd, &st) < 0)
    {
        close(fd);
        return false;
    }
    if (st.st_size < 0 || (size_t)st.st_size > MAX_FILE_SIZE)
    {
        close(fd);
        return false;
    }

    off_t offset = 0, end = st.st_size;

    while (offset < end)
    {
        uint64_t timestamp;
        uint32_t key_size, value_size;
        off_t value_pos;

        uint32_t remaining = (end - offset);
        if (remaining < 20)
        {
            return false;
        }

        uint8_t hint_buf[ENTRY_HEADER_SIZE];
        if (!pread_exact(fd, hint_buf, 20, offset))
        {
            return false;
        }

        timestamp = decode_u64_le(hint_buf);
        key_size = decode_u32_le(hint_buf + 8);
        value_size = decode_u32_le(hint_buf + 12);
        value_pos = decode_u32_le(hint_buf + 16);

        if (key_size == 0)
        {
            return false;
        }

        if (key_size > MAX_KEY_SIZE || value_size > MAX_VALUE_SIZE)
        {
            return false;
        }

        uint32_t remaining_payload = remaining - 20;
        if (key_size > remaining_payload)
        {
            return false;
        }

        offset += 20;

        uint8_t *key = malloc(key_size);
        if (key == NULL)
        {
            return false;
        }
        if (!pread_exact(fd, key, key_size, offset))
        {
            free(key);
            return false;
        }

        offset += key_size;

        keydir_value_t keydir_value = {
            .file_id = id,
            .value_pos = value_pos,
            .value_size = value_size,
            .timestamp = timestamp};

        if (!keydir_put(&bitcask->keydir, key, key_size, &keydir_value))
        {
            free(key);
            return false;
        }

        free(key);
    }

    return true;
}

static bool populate_keydir(datafile_t *datafile, keydir_t *keydir)
{
    off_t offset = 0;

    while (offset < datafile->write_offset)
    {
        uint32_t remaining = (datafile->write_offset - offset);
        if (remaining < ENTRY_HEADER_SIZE)
        {
            return false;
        }

        entry_header_t header;
        uint8_t hdr_buf[ENTRY_HEADER_SIZE];
        if (!datafile_read_at(datafile, offset, ENTRY_HEADER_SIZE, hdr_buf))
        {
            return false;
        }

        entry_header_decode(&header, hdr_buf);

        if (header.key_size == 0)
        {
            return false;
        }

        if (header.key_size > MAX_KEY_SIZE || header.value_size > MAX_VALUE_SIZE)
        {
            return false;
        }

        uint32_t remaining_payload = remaining - ENTRY_HEADER_SIZE;
        if (header.key_size > remaining_payload)
        {
            return false;
        }
        if (header.value_size > (remaining_payload - header.key_size))
        {
            return false;
        }

        offset += ENTRY_HEADER_SIZE;

        uint8_t *key = malloc(header.key_size);
        if (key == NULL)
        {
            return false;
        }
        if (!datafile_read_at(datafile, offset, header.key_size, key))
        {
            free(key);
            return false;
        }

        offset += header.key_size;

        if (!crc32_validate(header.crc, hdr_buf, key, header.key_size, datafile->fd, offset, header.value_size))
        {
            free(key);
            return false;
        }

        if (header.value_size != 0)
        {

            keydir_value_t keydir_value = {
                .file_id = datafile->file_id,
                .value_pos = offset,
                .value_size = header.value_size,
                .timestamp = header.timestamp};

            if (!keydir_put(keydir, key, header.key_size, &keydir_value))
            {
                free(key);
                return false;
            }
        }
        else
        {
            keydir_delete(keydir, key, header.key_size);
        }
        free(key);

        offset += header.value_size;
    }
    return true;
}

static bool parse_datafile_name(const struct dirent *dp, uint32_t *out, const char *suffix)
{
    const char *name = dp->d_name;

    const size_t suffix_len = 5;
    size_t len = strlen(name);
    if (len <= suffix_len || strcmp(name + (len - suffix_len), suffix) != 0)
    {
        return false;
    }

    size_t num_len = len - suffix_len;
    char *end = NULL;
    unsigned long parsed = strtoul(name, &end, 10);
    if (end != name + num_len || parsed > UINT32_MAX)
    {
        return false;
    }

    *out = (uint32_t)parsed;
    return true;
}

static bool check_path(const char *dir_path, uint8_t opts)
{
    struct stat sb;
    if (stat(dir_path, &sb) == 0)
    {
        return S_ISDIR(sb.st_mode);
    }

    if (errno != ENOENT)
    {
        return false;
    }

    if (!can_write(opts))
    {
        return false;
    }

    return mkdir(dir_path, 0755) == 0;
}

static bool scan_files(DIR *dirp, uint32_t **ids, size_t *count, const char *suffix)
{
    // return an allocated list of file_ids
    size_t limit = 20;
    *ids = malloc(sizeof(uint32_t) * limit);
    if (*ids == NULL)
    {
        return false;
    }
    struct dirent *dp;
    while ((dp = readdir(dirp)) != NULL)
    {
        uint32_t id;
        if (!parse_datafile_name(dp, &id, suffix))
        {
            continue;
        }

        (*ids)[*count] = id;
        (*count)++;
        if (*count >= limit)
        {
            limit *= 2;
            void *tmp = realloc(*ids, sizeof(uint32_t) * limit);
            if (tmp == NULL)
            {
                free(*ids);
                return false;
            }
            *ids = tmp;
        }
    }

    return true;
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

    if (!check_path(dir_path, opts))
    {
        return false;
    }

    DIR *dirp = opendir(dir_path);
    if (dirp == NULL)
    {
        return false;
    }

    uint32_t *ids, *hints;
    size_t count = 0, hint_count = 0;

    if (!scan_files(dirp, &ids, &count, ".data"))
    {
        closedir(dirp);
        return false;
    };

    rewinddir(dirp);

    if (!scan_files(dirp, &hints, &hint_count, ".hint"))
    {
        free(ids);
        closedir(dirp);
        return false;
    };
    if (count == 0 && !can_write(opts))
    {
        free(ids);
        free(hints);
        closedir(dirp);
        return false;
    }

    // allocate enough space for the number of inactive files to double (+1)
    bitcask->inactive_capacity = count == 0 ? 2 : count * 2;
    bitcask->inactive_files = malloc(sizeof(datafile_t) * bitcask->inactive_capacity);
    if (bitcask->inactive_files == NULL)
    {
        free(ids);
        free(hints);
        closedir(dirp);
        return false;
    }
    qsort(ids, count, sizeof(ids[0]), cmp_u32);
    qsort(hints, hint_count, sizeof(hints[0]), cmp_u32);

    bitcask->dir_path = strdup(dir_path);
    if (bitcask->dir_path == NULL)
    {
        free(ids);
        free(hints);
        closedir(dirp);
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
            closedir(dirp);
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
            if (!populate_keydir_from_hint(hints[cur_hint], bitcask))
            {
                free(ids);
                free(hints);
                closedir(dirp);
                bitcask_close(bitcask);
                return false;
            }
            cur_hint++;
        }
        else if (!populate_keydir(&bitcask->inactive_files[i], &bitcask->keydir))
        {
            free(ids);
            free(hints);
            closedir(dirp);
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
            closedir(dirp);
            bitcask_close(bitcask);
            return false;
        }
        bitcask->next_file_id++;
    }

    free(ids);
    free(hints);
    closedir(dirp);
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

    // open a temp datafile to write to
    datafile_t tmp;
    hintfile_t hint;
    datafile_init(&tmp);
    hintfile_init(&hint);

    if (!hintfile_open_merge(&hint, bitcask->dir_path, bitcask->next_file_id))
    {
        return false;
    };
    if (!datafile_open_merge(&tmp, bitcask->dir_path, bitcask->next_file_id, DATAFILE_READ_WRITE))
    {
        return false;
    };

    // bitcask->inactive_capacity is safe size because
    // worst-case scenario is 0 merging, meaning
    // each file is simply copied as-is
    datafile_t *new_inactive = malloc(sizeof(datafile_t) * bitcask->inactive_capacity);
    hintfile_t *merge_hintfiles = malloc(sizeof(datafile_t) * bitcask->inactive_capacity);
    if (new_inactive == NULL || merge_hintfiles == NULL)
    {
        (void)unlink(tmp.file_path);
        (void)unlink(hint.file_path);
        datafile_close(&tmp);
        hintfile_close(&hint);
        return false;
    }
    size_t current_merge_file = 0;

    // iterate over inactive files
    for (size_t i = 0; i < bitcask->inactive_count; i++)
    {
        off_t offset = 0;
        datafile_t *cur = &bitcask->inactive_files[i];

        while (offset < cur->write_offset)
        {
            uint32_t remaining = (cur->write_offset - offset);
            if (remaining < ENTRY_HEADER_SIZE)
            {
                for (size_t i = 0; i < current_merge_file + 1; i++)
                {
                    unlink(new_inactive[i].file_path);
                    unlink(merge_hintfiles[i].file_path);
                    if (i != current_merge_file)
                    {
                        datafile_close(&new_inactive[i]);
                    }
                }
                free(new_inactive);
                free(merge_hintfiles);
                datafile_close(&tmp);
                hintfile_close(&hint);
                return false;
            }

            entry_header_t header;
            uint8_t hdr_buf[ENTRY_HEADER_SIZE];
            if (!datafile_read_at(cur, offset, ENTRY_HEADER_SIZE, hdr_buf))
            {
                for (size_t i = 0; i < current_merge_file + 1; i++)
                {
                    unlink(new_inactive[i].file_path);
                    unlink(merge_hintfiles[i].file_path);
                    if (i != current_merge_file)
                    {
                        datafile_close(&new_inactive[i]);
                    }
                }
                free(new_inactive);
                free(merge_hintfiles);
                datafile_close(&tmp);
                hintfile_close(&hint);
                return false;
            }

            entry_header_decode(&header, hdr_buf);

            uint32_t remaining_payload = remaining - ENTRY_HEADER_SIZE;
            if (header.key_size > remaining_payload || header.value_size > (remaining_payload - header.key_size))
            {
                for (size_t i = 0; i < current_merge_file + 1; i++)
                {
                    unlink(new_inactive[i].file_path);
                    unlink(merge_hintfiles[i].file_path);
                    if (i != current_merge_file)
                    {
                        datafile_close(&new_inactive[i]);
                    }
                }
                free(new_inactive);
                free(merge_hintfiles);
                datafile_close(&tmp);
                hintfile_close(&hint);
                return false;
            }

            offset += ENTRY_HEADER_SIZE;

            uint8_t *key = malloc(header.key_size);
            if (key == NULL)
            {
                for (size_t i = 0; i < current_merge_file + 1; i++)
                {
                    unlink(new_inactive[i].file_path);
                    unlink(merge_hintfiles[i].file_path);
                    if (i != current_merge_file)
                    {
                        datafile_close(&new_inactive[i]);
                    }
                }
                free(new_inactive);
                free(merge_hintfiles);
                datafile_close(&tmp);
                hintfile_close(&hint);
                return false;
            }

            if (!datafile_read_at(cur, offset, header.key_size, key))
            {
                for (size_t i = 0; i < current_merge_file + 1; i++)
                {
                    unlink(new_inactive[i].file_path);
                    unlink(merge_hintfiles[i].file_path);
                    if (i != current_merge_file)
                    {
                        datafile_close(&new_inactive[i]);
                    }
                }
                free(new_inactive);
                free(merge_hintfiles);
                datafile_close(&tmp);
                hintfile_close(&hint);
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
            if ((size_t)tmp.write_offset > MAX_FILE_SIZE - ENTRY_HEADER_SIZE - header.key_size - header.value_size)
            {
                datafile_close(&tmp);
                hintfile_close(&hint);
                if (!datafile_open_merge(&new_inactive[current_merge_file], bitcask->dir_path, bitcask->next_file_id + current_merge_file, DATAFILE_READ) ||
                    !hintfile_open_merge(&merge_hintfiles[current_merge_file], bitcask->dir_path, bitcask->next_file_id + current_merge_file))
                {
                    free(key);
                    for (size_t i = 0; i < current_merge_file + 1; i++)
                    {
                        unlink(new_inactive[i].file_path);
                        unlink(merge_hintfiles[i].file_path);
                        if (i != current_merge_file)
                        {
                            datafile_close(&new_inactive[i]);
                        }
                    }
                    free(new_inactive);
                    free(merge_hintfiles);
                    return false;
                };

                // here: close hintfile
                hintfile_init(&hint);
                datafile_init(&tmp);
                current_merge_file++;
                if (!datafile_open_merge(&tmp, bitcask->dir_path, bitcask->next_file_id + current_merge_file, DATAFILE_READ_WRITE) ||
                    !hintfile_open_merge(&hint, bitcask->dir_path, bitcask->next_file_id + current_merge_file))
                {
                    free(key);
                    for (size_t i = 0; i < current_merge_file + 1; i++)
                    {
                        unlink(new_inactive[i].file_path);
                        unlink(merge_hintfiles[i].file_path);
                        if (i != current_merge_file)
                        {
                            datafile_close(&new_inactive[i]);
                        }
                    }
                    datafile_close(&tmp);
                    free(new_inactive);
                    free(merge_hintfiles);
                    return false;
                };

                // here: open new hintfile
            }

            if (!datafile_copy_entry(cur, &tmp, offset - header.key_size - ENTRY_HEADER_SIZE, ENTRY_HEADER_SIZE + header.key_size + header.value_size))
            {
                free(key);
                for (size_t i = 0; i < current_merge_file + 1; i++)
                {
                    unlink(new_inactive[i].file_path);
                    unlink(merge_hintfiles[i].file_path);
                    if (i != current_merge_file)
                    {
                        datafile_close(&new_inactive[i]);
                    }
                }
                datafile_close(&tmp);
                free(new_inactive);
                free(merge_hintfiles);
                return false;
            }

            // need to check this
            hintfile_append(&hint, hdr_buf, key, header.key_size, tmp.write_offset - header.value_size);

            free(key);
            offset += header.value_size;
        }
    }

    // seal final file or remove if empty

    // here: close hintfile

    if (tmp.write_offset == 0)
    {
        unlink(tmp.file_path);
        unlink(hint.file_path);
        datafile_close(&tmp);
        hintfile_close(&hint);
        current_merge_file--;
    }
    else
    {
        datafile_close(&tmp);
        hintfile_close(&hint);

        if (!datafile_open_merge(&new_inactive[current_merge_file], bitcask->dir_path, bitcask->next_file_id + current_merge_file, DATAFILE_READ) ||
            !hintfile_open_merge(&merge_hintfiles[current_merge_file], bitcask->dir_path, bitcask->next_file_id + current_merge_file))
        {
            for (size_t i = 0; i < current_merge_file + 1; i++)
            {
                unlink(new_inactive[i].file_path);
                unlink(merge_hintfiles[i].file_path);
                if (i != current_merge_file)
                {
                    datafile_close(&new_inactive[i]);
                }
            }
            datafile_close(&tmp);
            free(new_inactive);
            free(merge_hintfiles);
            return false;
        }
    };

    // atomically swap/remove
    // first, rename
    char *new_file_path, *new_hint_path;

    for (size_t i = 0; i < current_merge_file + 1; i++)
    {
        new_file_path = strdup(new_inactive[i].file_path);
        new_file_path[strlen(new_file_path) - 6] = '\0';
        if (rename(new_inactive[i].file_path, new_file_path) != 0)
        {
            for (size_t i = 0; i < current_merge_file + 1; i++)
            {
                unlink(new_inactive[i].file_path);
                datafile_close(&new_inactive[i]);
            }
            datafile_close(&tmp);
            free(new_inactive);
            return false;
        }

        new_hint_path = strdup(merge_hintfiles[i].file_path);
        new_hint_path[strlen(new_hint_path) - 6] = '\0';
        if (rename(merge_hintfiles[i].file_path, new_hint_path) != 0)
        {
            for (size_t i = 0; i < current_merge_file + 1; i++)
            {
                unlink(merge_hintfiles[i].file_path);
            }
            return false;
        }
    }

    // swap inactives, unlink, free old

    datafile_t *old_inactive = bitcask->inactive_files;
    size_t old_inactive_count = bitcask->inactive_count;

    bitcask->inactive_files = new_inactive;
    bitcask->inactive_count = current_merge_file + 1;

    for (size_t i = 0; i < old_inactive_count; i++)
    {
        (void)unlink(old_inactive[i].file_path);
        // do need to build old hintfile path here
        //(void)unlink(hint_path);

        datafile_close(&old_inactive[i]);
    }
    free(old_inactive);

    // for now just rebuild keydir
    for (size_t i = 0; i < bitcask->inactive_count; i++)
    {
        // need to add check for num of hint files
        // but should be equal to inactive_count here
        if (bitcask->inactive_files[i].file_id == merge_hintfiles[i].file_id)
        {
            if (!populate_keydir_from_hint(merge_hintfiles[i].file_id, bitcask))
            {
                return false;
            }
        }
        else if (!populate_keydir(&bitcask->inactive_files[i], &bitcask->keydir))
        {
            return false;
        }
    }

    bitcask->next_file_id += (current_merge_file + 1);

    for (size_t i = 0; i < bitcask->inactive_count; i++)
    {
        free((void *)merge_hintfiles[i].file_path);
    }
    free(merge_hintfiles);

    return true;
}
