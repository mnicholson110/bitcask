#include "../include/datafile.h"
#include "../include/crc.h"
#include "../include/entry.h"
#include "../include/io_util.h"
#include <fcntl.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <unistd.h>

void datafile_init(datafile_t *datafile)
{
    datafile->fd = -1;
    datafile->file_id = 0;
    datafile->write_offset = 0;
    datafile->mode = DATAFILE_READ;
    datafile->file_path = NULL;
}

static bool datafile_open_suffix(const char *suffix, datafile_t *datafile, const char *dir_path, uint32_t file_id, datafile_mode_t mode)
{
    char path[MAX_PATH_LEN];
    if (!build_file_path(dir_path, suffix, file_id, path, MAX_PATH_LEN))
    {
        return false;
    }

    int flags = (mode == DATAFILE_READ) ? O_RDONLY : (O_RDWR | O_CREAT);
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
    if (st.st_size < 0 || (size_t)st.st_size > MAX_FILE_SIZE)
    {
        close(fd);
        return false;
    }

    datafile->fd = fd;
    datafile->file_id = file_id;
    datafile->write_offset = st.st_size;
    datafile->mode = mode;
    datafile->file_path = strdup(path); // should check this return value
    return true;
}

bool datafile_open(datafile_t *datafile, const char *dir_path, uint32_t file_id, datafile_mode_t mode)
{
    return datafile_open_suffix(".data", datafile, dir_path, file_id, mode);
}

bool datafile_open_merge(datafile_t *datafile, const char *dir_path, uint32_t file_id, datafile_mode_t mode)
{
    return datafile_open_suffix(".data.merge", datafile, dir_path, file_id, mode);
}

void datafile_close(datafile_t *datafile)
{
    if (datafile->fd != -1)
    {
        close(datafile->fd);
    }
    if (datafile->file_path != NULL)
    {
        free((void *)datafile->file_path);
    }
    datafile_init(datafile);
}

void datafile_delete(datafile_t *datafile)
{
    if (datafile->file_path != NULL)
    {
        unlink(datafile->file_path);
    }
    datafile_close(datafile);
}

bool datafile_sync(datafile_t *datafile)
{
    if (datafile->fd == -1)
    {
        return false;
    }

    if (fsync(datafile->fd) == -1)
    {
        return false;
    }

    return true;
}

bool datafile_append(datafile_t *datafile, uint64_t timestamp, const uint8_t *key, uint32_t key_size, const uint8_t *value, uint32_t value_size, keydir_value_t *out)
{
    if (datafile->fd == -1 || datafile->mode == DATAFILE_READ || out == NULL)
    {
        return false;
    }

    if ((key == NULL && key_size != 0) || (value == NULL && value_size != 0))
    {
        return false;
    }

    // encode header values
    uint8_t header[ENTRY_HEADER_SIZE];
    entry_header_encode(header, 0, timestamp, key_size, value_size);

    // compute crc
    uint32_t crc = crc_init();
    crc = crc32_update(crc, header + ENTRY_HEADER_TIMESTAMP_OFFSET, ENTRY_HEADER_SIZE - ENTRY_HEADER_TIMESTAMP_OFFSET);
    crc = crc32_update(crc, key, key_size);
    crc = crc32_update(crc, value, value_size);
    crc = crc32_final(crc);

    // add crc value to header buf
    encode_u32_le(header, crc);

    if (!write_entry_exact(datafile->fd, header, key, key_size, value, value_size, datafile->write_offset))
    {
        return false;
    }

    off_t entry_pos = datafile->write_offset;
    datafile->write_offset += ENTRY_HEADER_SIZE + key_size + value_size;

    out->timestamp = timestamp;
    out->file_id = datafile->file_id;
    out->value_size = value_size;
    out->value_pos = entry_pos + ENTRY_HEADER_SIZE + key_size;

    return true;
}

bool datafile_read_at(const datafile_t *datafile, off_t offset, uint32_t size, uint8_t *out)
{
    if (datafile->fd == -1 || out == NULL)
    {
        return false;
    }

    if (!pread_exact(datafile->fd, out, size, offset))
    {
        return false;
    }

    return true;
}

bool datafile_copy_entry(datafile_t *src, datafile_t *dest, off_t src_offset, size_t entry_size)
{
    if (src->fd == -1 || dest->fd == -1 || dest->mode == DATAFILE_READ || entry_size == 0)
    {
        return false;
    }

    uint8_t scratch[4096];
    size_t remaining = entry_size;
    off_t pos = src_offset;

    while (remaining > 0)
    {
        size_t want = remaining < sizeof(scratch) ? remaining : sizeof(scratch);
        if (!pread_exact(src->fd, scratch, want, pos))
        {
            return false;
        }

        if (!pwrite_exact(dest->fd, scratch, want, dest->write_offset))
        {
            return false;
        }

        remaining -= want;
        pos += (off_t)want;
        dest->write_offset += (off_t)want;
    }

    return true;
}

bool datafile_populate_keydir(datafile_t *datafile, keydir_t *keydir)
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
