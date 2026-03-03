#include "../include/hintfile.h"
#include "../include/hint.h"
#include "../include/io_util.h"
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

void hintfile_init(hintfile_t *hintfile)
{
    hintfile->fd = -1;
    hintfile->file_id = 0;
    hintfile->write_offset = 0;
    hintfile->file_path = NULL;
}

static bool hintfile_open_suffix(const char *suffix, hintfile_t *hintfile, const char *dir_path,
                                 uint32_t file_id)
{
    char path[MAX_PATH_LEN];
    if (!build_file_path(dir_path, suffix, file_id, path, MAX_PATH_LEN))
    {
        return false;
    }

    int flags = (O_RDWR | O_CREAT);
    int fd = open(path, flags, 0644);
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
    if (st.st_size < 0)
    {
        close(fd);
        return false;
    }

    hintfile->fd = fd;
    hintfile->file_id = file_id;
    hintfile->write_offset = st.st_size;
    hintfile->file_path = strdup(path); // should check this return value
    return true;
}

bool hintfile_open(hintfile_t *hintfile, const char *dir_path,
                   uint32_t file_id)
{
    return hintfile_open_suffix(".hint", hintfile, dir_path, file_id);
}

bool hintfile_open_merge(hintfile_t *hintfile, const char *dir_path,
                         uint32_t file_id)
{
    return hintfile_open_suffix(".hint.merge", hintfile, dir_path, file_id);
}

void hintfile_close(hintfile_t *hintfile)
{
    if (hintfile->fd != -1)
    {
        close(hintfile->fd);
    }
    if (hintfile->file_path != NULL)
    {
        free((void *)hintfile->file_path);
    }
    hintfile_init(hintfile);
}

void hintfile_delete(hintfile_t *hintfile)
{
    if (hintfile->file_path != NULL)
    {
        unlink(hintfile->file_path);
    }
    hintfile_close(hintfile);
}

bool hintfile_sync(hintfile_t *hintfile)
{
    if (hintfile->fd == -1)
    {
        return false;
    }

    if (fsync(hintfile->fd) == -1)
    {
        return false;
    }

    return true;
}

bool hintfile_append(hintfile_t *hintfile, uint64_t timestamp,
                     uint32_t key_size, uint32_t value_size,
                     off_t value_pos, const uint8_t *key)
{

    if (hintfile->fd == -1)
    {
        return false;
    }

    // encode header values
    uint8_t header[HINT_HEADER_SIZE];
    hint_header_encode(header, timestamp, key_size, value_size, value_pos);

    if (!write_hint_exact(hintfile->fd, header, key, key_size, hintfile->write_offset))
    {
        return false;
    }

    hintfile->write_offset += (HINT_HEADER_SIZE + key_size);

    return true;
}

bool hintfile_read_at(const hintfile_t *hintfile,
                      off_t offset,
                      uint32_t size,
                      uint8_t *out)
{
    if (hintfile->fd == -1 || out == NULL)
    {
        return false;
    }

    if (!pread_exact(hintfile->fd, out, size, offset))
    {
        return false;
    }

    return true;
}

bool hintfile_populate_keydir(uint32_t id, keydir_t *keydir, const char *dir_path)
{
    int path_max = strlen(dir_path) + 40;
    char hint_path[path_max];
    if (!build_file_path(dir_path, ".hint", id, hint_path, path_max))
    {
        return false;
    }

    int fd = open(hint_path, O_RDONLY, 0644);
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
    if (st.st_size < 0)
    {
        close(fd);
        return false;
    }

    off_t offset = 0, end = st.st_size;

    while (offset < end)
    {
        uint32_t remaining = (end - offset);
        if (remaining < HINT_HEADER_SIZE)
        {
            close(fd);
            return false;
        }

        hint_header_t header;
        uint8_t hint_buf[HINT_HEADER_SIZE];
        if (!pread_exact(fd, hint_buf, HINT_HEADER_SIZE, offset))
        {
            close(fd);
            return false;
        }

        hint_header_decode(&header, hint_buf);

        if (header.key_size == 0)
        {
            close(fd);
            return false;
        }

        offset += HINT_HEADER_SIZE;

        uint8_t *key = malloc(header.key_size);
        if (key == NULL)
        {
            close(fd);
            return false;
        }
        if (!pread_exact(fd, key, header.key_size, offset))
        {
            close(fd);
            free(key);
            return false;
        }

        offset += header.key_size;

        keydir_value_t keydir_value = {
            .file_id = id,
            .value_pos = header.value_pos,
            .value_size = header.value_size,
            .timestamp = header.timestamp};

        if (!keydir_put(keydir, key, header.key_size, &keydir_value))
        {
            close(fd);
            free(key);
            return false;
        }

        free(key);
    }

    close(fd);
    return true;
}
