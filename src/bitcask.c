#include "../include/bitcask.h"

static int cmp_u64(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a;
    uint64_t y = *(const uint64_t *)b;
    return (x > y) - (x < y); // avoids overflow from subtraction
}

static bool read_full(int fd, uint8_t *buf, size_t n)
{
    size_t total = 0;
    while (total < n)
    {
        ssize_t bytes = read(fd, buf + total, n - total);
        if (bytes <= 0)
        {
            return false;
        }
        total += (size_t)bytes;
    }

    return true;
}

static bool populate_keydir(datafile_t *datafile, table_t *keydir)
{
    off_t end = lseek(datafile->fd, 0, SEEK_END);
    if (end < 0)
    {
        return false;
    }

    off_t offset = lseek(datafile->fd, 0, SEEK_SET);
    if (offset < 0)
    {
        return false;
    }

    while (offset < end)
    {
        uint64_t remaining = (uint64_t)(end - offset);
        if (remaining < (uint64_t)ENTRY_HEADER_SIZE)
        {
            return false;
        }

        entry_header_t header;
        uint8_t hdr_buf[ENTRY_HEADER_SIZE];
        if (!read_full(datafile->fd, hdr_buf, ENTRY_HEADER_SIZE))
        {
            return false;
        }

        if (!entry_header_decode(&header, hdr_buf))
        {
            return false;
        }

        if (header.key_size == 0)
        {
            return false;
        }

        if (header.key_size > SIZE_MAX || header.value_size > SIZE_MAX)
        {
            return false;
        }

        uint64_t remaining_payload = remaining - (uint64_t)ENTRY_HEADER_SIZE;
        if (header.key_size > remaining_payload)
        {
            return false;
        }
        if (header.value_size > (remaining_payload - header.key_size))
        {
            return false;
        }

        uint8_t *key = malloc(header.key_size);
        if (key == NULL)
        {
            return false;
        }
        if (!read_full(datafile->fd, key, header.key_size))
        {
            free(key);
            return false;
        }

        off_t value_offset = lseek(datafile->fd, 0, SEEK_CUR);
        if (value_offset < 0)
        {
            free(key);
            return false;
        }

        if (header.value_size != 0)
        {
            keydir_value_t keydir_value = {.file_id = datafile->file_id,
                                           .value_pos = (uint64_t)value_offset,
                                           .value_size = header.value_size,
                                           .timestamp = header.timestamp};

            if (!table_put(keydir, key, header.key_size, &keydir_value))
            {
                free(key);
                return false;
            }
        }
        else
        {
            table_delete(keydir, key, header.key_size);
        }
        free(key);

        if (header.value_size > 0)
        {
            if (lseek(datafile->fd, header.value_size, SEEK_CUR) < 0)
            {
                return false;
            }
        }

        offset = lseek(datafile->fd, 0, SEEK_CUR);
        if (offset < 0)
        {
            return false;
        }
    }
    return true;
}

static bool parse_datafile_name(const struct dirent *dp, uint64_t *out)
{
    const char *name = dp->d_name;

    const char *suffix = ".data";
    const size_t suffix_len = 5;
    size_t len = strlen(name);
    if (len <= suffix_len || strcmp(name + (len - suffix_len), suffix) != 0)
    {
        return false;
    }

    size_t num_len = len - suffix_len;
    for (size_t i = 0; i < num_len; i++)
    {
        if (!isdigit((unsigned char)name[i]))
        {
            return false;
        }
    }

    char *end = NULL;
    uint64_t parsed = strtoull(name, &end, 10);
    if (end != name + num_len)
    {
        return false;
    }

    *out = parsed;
    return true;
}

static bool check_path(const char *dir_path)
{
    if (mkdir(dir_path, 0755) != 0)
    {
        if (errno != EEXIST)
        {
            return false;
        }
        else
        {
            struct stat sb;
            if (stat(dir_path, &sb) == 0)
            {
                if (!S_ISDIR(sb.st_mode))
                {
                    return false;
                }
            }
            else
            {
                return false;
            }
        }
    }
    return true;
}

static bool scan_datafiles(DIR *dirp, uint64_t **ids, size_t *count)
{
    // return an allocated list of file_ids
    size_t limit = 20;
    *ids = malloc(sizeof(uint64_t) * limit);
    if (*ids == NULL)
    {
        return false;
    }
    struct dirent *dp;
    while ((dp = readdir(dirp)) != NULL)
    {
        uint64_t id;
        if (!parse_datafile_name(dp, &id))
        {
            continue;
        }

        (*ids)[*count] = id;
        (*count)++;
        if (*count >= limit)
        {
            limit *= 2;
            void *tmp = realloc(*ids, sizeof(uint64_t) * limit);
            if (tmp == NULL)
            {
                return false;
            }
            *ids = tmp;
        }
    }

    return true;
}

bool rotate_active_file(bitcask_handle_t *bitcask)
{
    if (!bitcask_sync(bitcask))
    {
        return false;
    }

    if (bitcask->file_count + 1 >= bitcask->inactive_capacity)
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
    uint64_t old_active_id = bitcask->active_file.file_id;
    datafile_close(&bitcask->active_file);

    size_t dir_len = strlen(bitcask->dir_path);
    bool has_slash = (dir_len > 0 && bitcask->dir_path[dir_len - 1] == '/');
    size_t path_len = dir_len + (has_slash ? 0 : 1) + 32;
    char *file_path = malloc(path_len);
    if (file_path == NULL)
    {
        return false;
    }

    int n = snprintf(file_path, path_len, "%s%s%02" PRIu64 ".data", bitcask->dir_path,
                     has_slash ? "" : "/", old_active_id);
    if (n < 0 || (size_t)n >= path_len)
    {
        free(file_path);
        return false;
    }

    if (!datafile_open(&bitcask->inactive_files[bitcask->file_count - 1], file_path, old_active_id, DATAFILE_READ))
    {
        free(file_path);
        return false;
    };

    // open new active file
    n = snprintf(file_path, path_len, "%s%s%02" PRIu64 ".data", bitcask->dir_path,
                 has_slash ? "" : "/", old_active_id + 1);
    if (n < 0 || (size_t)n >= path_len)
    {
        free(file_path);
        return false;
    }

    if (!datafile_open(&bitcask->active_file, file_path, old_active_id + 1, DATAFILE_READ_WRITE))
    {
        free(file_path);
        return false;
    }
    bitcask->file_count++;

    free(file_path);
    return true;
}

bool bitcask_open(bitcask_handle_t *bitcask, const char *dir_path,
                  bitcask_opts_t opts)
{
    bitcask->inactive_files = NULL;
    bitcask->inactive_capacity = 0;
    bitcask->file_count = 0;
    bitcask->keydir.count = 0;
    bitcask->keydir.capacity = 0;
    bitcask->keydir.entries = NULL;

    if (!check_path(dir_path))
    {
        return false;
    }

    DIR *dirp = opendir(dir_path);
    if (dirp == NULL)
    {
        return false;
    }

    uint64_t *ids;
    size_t count = 0;

    if (!scan_datafiles(dirp, &ids, &count))
    {
        closedir(dirp);
        return false;
    };
    if (count == 0 && opts == BITCASK_READ_ONLY)
    {
        free(ids);
        closedir(dirp);
        return false;
    }
    bitcask->file_count = count;
    // allocate enough space for the number of inactive files to double (+1)
    bitcask->inactive_capacity = count == 0 ? 2 : count * 2;
    bitcask->inactive_files = malloc(sizeof(datafile_t) * bitcask->inactive_capacity);
    if (bitcask->inactive_files == NULL)
    {
        free(ids);
        closedir(dirp);
        return false;
    }
    qsort(ids, count, sizeof(ids[0]), cmp_u64);

    // buffer for filepaths
    size_t dir_len = strlen(dir_path);
    bool has_slash = (dir_len > 0 && dir_path[dir_len - 1] == '/');
    size_t path_len = dir_len + (has_slash ? 0 : 1) + 10;
    char *file_path = malloc(path_len);
    bitcask->dir_path = strdup(dir_path);
    if (file_path == NULL || bitcask->dir_path == NULL)
    {
        free(ids);
        closedir(dirp);
        bitcask_close(bitcask);
        return false;
    }

    if (count == 0)
    {
        // create initial datafile and set it as active
        datafile_init(&bitcask->active_file);
        bitcask->file_count = 1;
        // open datafile
        int n = snprintf(file_path, path_len, "%s%s%02" PRIu64 ".data", dir_path,
                         has_slash ? "" : "/", (uint64_t)1);
        if (n < 0 || (size_t)n >= path_len)
        {
            free(file_path);
            free(ids);
            closedir(dirp);
            bitcask_close(bitcask);
            return false;
        }
        if (opts == BITCASK_READ_ONLY)
        {
            if (!datafile_open(&bitcask->active_file, file_path, 1, DATAFILE_READ))
            {
                free(file_path);
                free(ids);
                closedir(dirp);
                bitcask_close(bitcask);
                return false;
            }
        }
        else
        {
            if (!datafile_open(&bitcask->active_file, file_path, 1, DATAFILE_READ_WRITE))
            {
                free(file_path);
                free(ids);
                closedir(dirp);
                bitcask_close(bitcask);
                return false;
            }
        }
        // initialize empty keydir
        init_table(&bitcask->keydir);
    }
    else
    {
        // find max file_id, set as active
        uint64_t active_id = ids[count - 1];

        datafile_init(&bitcask->active_file);
        int n = snprintf(file_path, path_len, "%s%s%02" PRIu64 ".data", dir_path,
                         has_slash ? "" : "/", active_id);
        if (n < 0 || (size_t)n >= path_len)
        {
            free(file_path);
            free(ids);
            closedir(dirp);
            bitcask_close(bitcask);
            return false;
        }
        if (opts == BITCASK_READ_ONLY)
        {
            if (!datafile_open(&bitcask->active_file, file_path, active_id, DATAFILE_READ))
            {
                free(file_path);
                free(ids);
                closedir(dirp);
                bitcask_close(bitcask);
                return false;
            }
        }
        else
        {
            if (!datafile_open(&bitcask->active_file, file_path, active_id, DATAFILE_READ_WRITE))
            {
                free(file_path);
                free(ids);
                closedir(dirp);
                bitcask_close(bitcask);
                return false;
            }
        }

        // set rest as inactive
        for (size_t i = 0; i < count - 1; i++)
        {

            datafile_init(&bitcask->inactive_files[i]);
            int n = snprintf(file_path, path_len, "%s%s%02" PRIu64 ".data", dir_path,
                             has_slash ? "" : "/", ids[i]);
            if (n < 0 || (size_t)n >= path_len)
            {
                free(file_path);
                free(ids);
                closedir(dirp);
                bitcask_close(bitcask);
                return false;
            }
            if (!datafile_open(&bitcask->inactive_files[i], file_path, ids[i], DATAFILE_READ))
            {
                free(file_path);
                free(ids);
                closedir(dirp);
                bitcask_close(bitcask);
                return false;
            }
        }
        // rebuild keydir
        init_table(&bitcask->keydir);
        // scan files from inactive[0] thru to active file and rebuild keydir
        for (size_t i = 0; i < count - 1; i++)
        {
            if (!populate_keydir(&bitcask->inactive_files[i], &bitcask->keydir))
            {
                free(file_path);
                free(ids);
                closedir(dirp);
                bitcask_close(bitcask);
                return false;
            }
        }
        if (!populate_keydir(&bitcask->active_file, &bitcask->keydir))
        {
            free(file_path);
            free(ids);
            closedir(dirp);
            bitcask_close(bitcask);
            return false;
        }
    }

    free(file_path);
    free(ids);
    closedir(dirp);
    return true;
}

bool bitcask_get(bitcask_handle_t *bitcask, const uint8_t *key,
                 size_t key_size, uint8_t **out, size_t *out_size)
{
    const keydir_value_t *entry = table_get(&bitcask->keydir, key, key_size);
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
        for (uint64_t i = 0; i < bitcask->file_count - 1; i++)
        {
            if (bitcask->inactive_files[i].file_id == entry->file_id)
            {
                target = &bitcask->inactive_files[i];
            }
        }
    }

    if (target == NULL)
    {
        return false;
    }

    *out = malloc(entry->value_size);
    *out_size = entry->value_size;

    off_t value_off = lseek(target->fd, entry->value_pos, SEEK_SET);
    if (value_off < 0 || (uint64_t)value_off != entry->value_pos)
    {
        free(*out);
        return false;
    }

    read(target->fd, *out, *out_size);
    // handle read error here
    return true;
}

bool bitcask_put(bitcask_handle_t *bitcask, const uint8_t *key,
                 size_t key_size, const uint8_t *value, size_t value_size)
{
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
        table_delete(&bitcask->keydir, key, key_size);
        return true;
    }

    if (!table_put(&bitcask->keydir, key, key_size, &out))
    {
        return false;
    }

    if (out.value_pos + out.value_size >= MAX_FILE_SIZE)
    {
        if (!rotate_active_file(bitcask))
        {
            return false;
        }
    }

    return true;
}

bool bitcask_delete(bitcask_handle_t *bitcask, const uint8_t *key, size_t key_size)
{
    if (!bitcask_put(bitcask, key, key_size, NULL, 0))
    {
        return false;
    }
    return true;
}

bool bitcask_sync(bitcask_handle_t *bitcask)
{
    if (!fsync(bitcask->active_file.fd))
    {
        return true;
    }
    return false;
}
void bitcask_close(bitcask_handle_t *bitcask)
{
    bitcask_sync(bitcask);
    // TODO: write keydir to hintfile

    if (bitcask->file_count != 0)
    {
        for (size_t i = 0; i < bitcask->file_count - 1; i++)
        {
            datafile_close(&bitcask->inactive_files[i]);
        }
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
    bitcask->file_count = 0;
    free_table(&bitcask->keydir);
}
