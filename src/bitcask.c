#include "../include/bitcask.h"
#include <dirent.h>
#include <errno.h>

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
        }
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
}

bool bitcask_get(bitcask_handle_t *bitcask, const uint8_t *key,
                 size_t key_size, uint8_t **out, size_t *out_size);

bool bitcask_put(bitcask_handle_t *bitcask, const uint8_t *key,
                 size_t key_size, const uint8_t *value, size_t value_size);

bool bitcask_delete(bitcask_handle_t *bitcask, const uint8_t *key, size_t key_size);

bool bitcask_sync(bitcask_handle_t *bitcask);

void bitcask_close(bitcask_handle_t *bitcask);
