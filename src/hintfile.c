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
    unlink(hintfile->file_path);
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
