#include "../include/bitcask.h"
#include <ctype.h>
#include <inttypes.h>
#include <stdlib.h>

static int cmp_u64(const void *a, const void *b)
{
    uint64_t x = *(const uint64_t *)a;
    uint64_t y = *(const uint64_t *)b;
    return (x > y) - (x < y); // avoids overflow from subtraction
}

static bool parse_datafile_name(const struct dirent *dp, uint64_t *out)
{
    if (dp->d_type != DT_REG || dp->d_namlen != 7)
    {
        return false;
    }

    const char *d = dp->d_name;
    if (d[2] != '.' || strcmp(d + 2, ".data") != 0)
    {
        return false;
    }
    if (!isdigit((unsigned char)d[0]) || !isdigit((unsigned char)d[1]))
    {
        return false;
    }

    uint64_t id = (uint64_t)((d[0] - '0') * 10 + (d[1] - '0'));

    *out = id;
    return true;
}

bool check_path(const char *dir_path)
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

bool scan_datafiles(DIR *dirp, uint64_t **ids, size_t *count)
{
    // for now, return an allocated list of file_ids
    // hard-coded to size 20, for now (our max_files is 10)
    *ids = malloc(sizeof(uint64_t) * 20);
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
    }

    return true;
}

bool bitcask_open(bitcask_handle_t *bitcask, const char *dir_path,
                  bitcask_opts_t opts)
{
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

    scan_datafiles(dirp, &ids, &count);
    qsort(ids, count, sizeof(ids[0]), cmp_u64);

    // buffer for filepaths
    size_t dir_len = strlen(dir_path);
    bool has_slash = (dir_len > 0 && dir_path[dir_len - 1] == '/');
    size_t path_len = dir_len + (has_slash ? 0 : 1) + 10;
    char *file_path = malloc(path_len);
    if (file_path == NULL)
    {
        return false;
    }

    if (count == 0)
    {
        // create initial datafile and set it as active
        datafile_init(&bitcask->active_file);
        // open datafile
        int n = snprintf(file_path, path_len, "%s%s%02" PRIu64 ".data", dir_path,
                         has_slash ? "" : "/", (uint64_t)1);
        if (n < 0 || (size_t)n >= path_len)
        {
            free(file_path);
            free(ids);
            closedir(dirp);
            return false;
        }
        datafile_open(&bitcask->active_file, file_path, 1, DATAFILE_READ_WRITE);
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
        if (n < 0 || (size_t)n > path_len)
        {
            free(file_path);
            free(ids);
            closedir(dirp);
            return false;
        }
        datafile_open(&bitcask->active_file, file_path, active_id, DATAFILE_READ_WRITE);

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
                return false;
            }
            datafile_open(&bitcask->inactive_files[i], file_path, ids[i], DATAFILE_READ);
        }
        // rebuild keydir
        init_table(&bitcask->keydir);
        // scan files from inactive[0] thru to active file and rebuild keydir
        for (size_t i = 0; i < count - 1; i++)
        {
            int cur = bitcask->inactive_files[i].fd;
            off_t end = lseek(cur, 0, SEEK_END);
            off_t offset = lseek(cur, 0, SEEK_SET);

            while (offset < end)
            {
                entry_header_t header;
                uint8_t hdr_buf[ENTRY_HEADER_SIZE];
                read(cur, hdr_buf, ENTRY_HEADER_SIZE);
                // TODO: add read error checking
                if (!entry_header_decode(&header, hdr_buf))
                {
                    // TODO: handle gracefully -- for now just exit
                    free(file_path);
                    free(ids);
                    closedir(dirp);
                    return false;
                }
                uint8_t key[header.key_size];
                read(cur, key, header.key_size);
                offset = lseek(cur, 0, SEEK_CUR);

                keydir_value_t keydir_value = {.file_id = bitcask->inactive_files[i].file_id,
                                               .value_pos = (uint64_t)offset,
                                               .value_size = header.value_size,
                                               .timestamp = header.timestamp};

                table_put(&bitcask->keydir, key, header.key_size, &keydir_value);
                lseek(cur, header.value_size, SEEK_CUR);
                offset = lseek(cur, 0, SEEK_CUR);
            }
        }
        // active file
        int cur = bitcask->active_file.fd;
        off_t end = lseek(cur, 0, SEEK_END);
        off_t offset = lseek(cur, 0, SEEK_SET);

        while (offset < end)
        {
            entry_header_t header;
            uint8_t hdr_buf[ENTRY_HEADER_SIZE];
            read(cur, hdr_buf, ENTRY_HEADER_SIZE);
            // TODO: add read error checking
            if (!entry_header_decode(&header, hdr_buf))
            {
                // TODO: handle gracefully -- for now just exit
                free(file_path);
                free(ids);
                closedir(dirp);
                return false;
            }
            uint8_t key[header.key_size];
            read(cur, key, header.key_size);
            offset = lseek(cur, 0, SEEK_CUR);

            keydir_value_t keydir_value = {.file_id = bitcask->active_file.file_id,
                                           .value_pos = (uint64_t)offset,
                                           .value_size = header.value_size,
                                           .timestamp = header.timestamp};

            table_put(&bitcask->keydir, key, header.key_size, &keydir_value);
            lseek(cur, header.value_size, SEEK_CUR);
            offset = lseek(cur, 0, SEEK_CUR);
        }
    }

    free(file_path);
    free(ids);
    closedir(dirp);
    return true;
}

bool bitcask_get(bitcask_handle_t *bitcask, const uint8_t *key,
                 size_t key_size, uint8_t **out, size_t *out_size);

bool bitcask_put(bitcask_handle_t *bitcask, const uint8_t *key,
                 size_t key_size, const uint8_t *value, size_t value_size);

bool bitcask_delete(bitcask_handle_t *bitcask, const uint8_t *key, size_t key_size);

bool bitcask_sync(bitcask_handle_t *bitcask);

void bitcask_close(bitcask_handle_t *bitcask);
