#include "../include/io_util.h"
#include "../include/entry.h"
#include "../include/hint.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/uio.h>
#include <unistd.h>

static int cmp_u32(const void *a, const void *b)
{
    uint32_t x = *(const uint32_t *)a;
    uint32_t y = *(const uint32_t *)b;
    return (x > y) - (x < y); // avoids overflow from subtraction
}

static bool build_lock_path(const char *dir_path, char *out, size_t out_size)
{
    size_t dir_len = strlen(dir_path);
    bool has_slash = (dir_len > 0 && dir_path[dir_len - 1] == '/');
    int path_n = snprintf(out, out_size, "%s%sdir.lock", dir_path, has_slash ? "" : "/");
    if (path_n < 0 || (size_t)path_n >= out_size)
    {
        return false;
    }
    return true;
}

bool pread_exact(int fd, uint8_t *buf, size_t len, off_t offset)
{
    size_t done = 0;

    while (done < len)
    {
        ssize_t n = pread(fd, buf + done, len - done, offset + (off_t)done);
        if (n == 0)
        {
            return false;
        }
        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return false;
        }

        done += (size_t)n;
    }

    return true;
}

bool pwrite_exact(int fd, uint8_t *buf, size_t len, off_t offset)
{
    size_t done = 0;

    while (done < len)
    {
        ssize_t n = pwrite(fd, buf + done, len - done, offset + (off_t)done);
        if (n == 0)
        {
            return false;
        }
        if (n < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return false;
        }

        done += (size_t)n;
    }

    return true;
}

bool write_entry_exact(int fd, const uint8_t *header, const uint8_t *key, size_t key_size, const uint8_t *value, size_t value_size, off_t offset)
{
    size_t done = 0;
    int cur = 0;
    size_t total = ENTRY_HEADER_SIZE + key_size + value_size;

    struct iovec iov[3] =
        {
            {.iov_base = (void *)header, .iov_len = ENTRY_HEADER_SIZE},
            {.iov_base = (void *)key, .iov_len = key_size},
            {.iov_base = (void *)value, .iov_len = value_size},
        };

    while (done < total)
    {
        ssize_t written = pwritev(fd, iov + cur, 3 - cur, offset + done);
        if (written == 0)
        {
            return false;
        }
        if (written < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return false;
        }
        done += (size_t)written;
        if (done < total)
        {
            while (cur < 3 && (size_t)written >= iov[cur].iov_len)
            {
                written -= iov[cur++].iov_len;
            }
            iov[cur].iov_base = (uint8_t *)iov[cur].iov_base + written;
            iov[cur].iov_len -= written;
        }
    }

    return true;
}

bool write_hint_exact(int fd, const uint8_t *header, const uint8_t *key, size_t key_size, off_t offset)
{
    size_t done = 0;
    int cur = 0;
    size_t total = HINT_HEADER_SIZE + key_size;

    struct iovec iov[2] =
        {
            {.iov_base = (void *)header, .iov_len = HINT_HEADER_SIZE},
            {.iov_base = (void *)key, .iov_len = key_size}};

    while (done < total)
    {
        ssize_t written = pwritev(fd, iov + cur, 2 - cur, offset + done);
        if (written == 0)
        {
            return false;
        }
        if (written < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }
            return false;
        }
        done += (size_t)written;
        if (done < total)
        {
            while (cur < 2 && (size_t)written >= iov[cur].iov_len)
            {
                written -= iov[cur++].iov_len;
            }
            iov[cur].iov_base = (uint8_t *)iov[cur].iov_base + written;
            iov[cur].iov_len -= written;
        }
    }

    return true;
}

bool build_file_path(const char *dir_path, const char *suffix, uint32_t file_id, char *out, size_t out_size)
{
    size_t dir_len = strlen(dir_path);
    bool has_slash = (dir_len > 0 && dir_path[dir_len - 1] == '/');

    int path_n = snprintf(out, out_size, "%s%s%02" PRIu32 "%s", dir_path,
                          has_slash ? "" : "/", file_id, suffix);
    if (path_n < 0 || (size_t)path_n >= out_size)
    {
        return false;
    }

    return true;
}

static DIR *check_path(const char *dir_path, bool can_write)
{
    struct stat sb;
    if (stat(dir_path, &sb) == 0)
    {
        if (S_ISDIR(sb.st_mode))
        {
            return opendir(dir_path);
        }
    }

    if (errno != ENOENT)
    {
        return NULL;
    }

    if (!can_write)
    {
        return NULL;
    }

    if (mkdir(dir_path, 0755) != 0)
    {
        return NULL;
    }

    return opendir(dir_path);
}

static uint32_t parse_file_id(const char *file)
{
    char *end = NULL;
    uint32_t id = strtoul(file, &end, 10);
    if (end != file + strlen(file) - 5)
    {
        return 0;
    }
    return id;
}

bool scan_dir(const char *dir_path, bool can_write, uint32_t **datafiles, size_t *count, uint32_t **hints, size_t *hint_count, bool *locked)
{
    DIR *dirp = check_path(dir_path, can_write);
    if (dirp == NULL)
    {
        return false;
    }
    // return an allocated list of file_ids
    size_t limit = 20;
    *datafiles = malloc(sizeof(uint32_t) * limit);
    *hints = malloc(sizeof(uint32_t) * limit);
    if (*datafiles == NULL || *hints == NULL)
    {
        free(*datafiles);
        free(*hints);
        closedir(dirp);
        return false;
    }

    struct dirent *dp;
    while ((dp = readdir(dirp)) != NULL)
    {
        size_t name_len = strlen(dp->d_name);
        size_t suffix_len = 5; // .data, .hint, .lock
        if (name_len <= suffix_len)
        {
            continue;
        }

        if (strcmp(dp->d_name + name_len - suffix_len, ".data") == 0)
        {
            uint32_t id = parse_file_id(dp->d_name);
            if (id == 0)
            {
                continue;
            }
            (*datafiles)[*count] = id;
            (*count)++;
            if (*count >= limit)
            {
                limit *= 2;
                void *tmp = realloc(*datafiles, sizeof(uint32_t) * limit);
                if (tmp == NULL)
                {
                    free(*datafiles);
                    closedir(dirp);
                    return false;
                }

                void *tmp_hint = realloc(*hints, sizeof(uint32_t) * limit);
                if (tmp_hint == NULL)
                {
                    free(*hints);
                    closedir(dirp);
                    return false;
                }
                *datafiles = tmp;
                *hints = tmp_hint;
            }
        }
        else if (strcmp(dp->d_name + name_len - suffix_len, ".hint") == 0)
        {
            uint32_t id = parse_file_id(dp->d_name);
            if (id == 0)
            {
                continue;
            }
            (*hints)[*hint_count] = id;
            (*hint_count)++;
            if (*hint_count >= limit)
            {
                limit *= 2;
                void *tmp = realloc(*datafiles, sizeof(uint32_t) * limit);
                if (tmp == NULL)
                {
                    free(*datafiles);
                    closedir(dirp);
                    return false;
                }

                void *tmp_hint = realloc(*hints, sizeof(uint32_t) * limit);
                if (tmp_hint == NULL)
                {
                    free(*hints);
                    closedir(dirp);
                    return false;
                }
                *datafiles = tmp;
                *hints = tmp_hint;
            }
        }
        else if (strcmp(dp->d_name, "dir.lock") == 0)
        {
            *locked = true;
        }
    }

    qsort(*datafiles, *count, sizeof(*datafiles[0]), cmp_u32);
    qsort(*hints, *hint_count, sizeof(*hints[0]), cmp_u32);
    closedir(dirp);
    return true;
}

bool lock_dir(const char *dir_path)
{
    char lock_path[MAX_PATH_LEN];
    if (!build_lock_path(dir_path, lock_path, sizeof(lock_path)))
    {
        return false;
    }

    int fd = open(lock_path, (O_WRONLY | O_CREAT | O_EXCL), 0644);
    if (fd == -1)
    {
        return false;
    }

    close(fd);
    return true;
}

void unlock_dir(const char *dir_path)
{
    char lock_path[MAX_PATH_LEN];
    if (!build_lock_path(dir_path, lock_path, sizeof(lock_path)))
    {
        return;
    }

    (void)unlink(lock_path);
}

bool sync_dir(const char *dir_path)
{
    int fd = open(dir_path, (O_RDONLY | O_DIRECTORY), 0664);
    if (fd == -1)
    {
        return false;
    }
    if (fsync(fd) == -1)
    {
        close(fd);
        return false;
    }
    close(fd);
    return true;
}
