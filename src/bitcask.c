#include "../include/bitcask.h"
#include <ctype.h>
#include <stdlib.h>

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

    if (count == 0)
    {
        // create initial datafile and set it as active
        // open datafile
        // initialize empty keydir
    }
    else
    {
        // find max file_id, set as active
        // set rest as inactive
        // open all datafiles
        // rebuild keydir
    }

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
