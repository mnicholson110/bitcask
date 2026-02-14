#include "../include/datafile.h"

#define BITCASK_SSIZE_MAX ((size_t)(~(size_t)0 >> 1))

static bool u64_to_off_t(uint64_t in, off_t *out)
{
    off_t converted = (off_t)in;
    if (converted < 0 || (uint64_t)converted != in)
    {
        return false;
    }

    *out = converted;
    return true;
}

static bool off_t_to_u64(off_t in, uint64_t *out)
{
    if (in < 0)
    {
        return false;
    }

    *out = (uint64_t)in;
    return true;
}

void datafile_init(datafile_t *datafile)
{
    datafile->fd = -1;
    datafile->file_id = 0;
    datafile->write_offset = 0;
    datafile->mode = DATAFILE_READ;
}

bool datafile_open(datafile_t *datafile, const char *path,
                   uint64_t file_id, datafile_mode_t mode)
{
    int flags = (mode == DATAFILE_READ) ? O_RDONLY : (O_RDWR | O_CREAT);
    int fd = open(path, flags, 0644);
    if (fd < 0)
    {
        return false;
    }

    off_t end = lseek(fd, 0, SEEK_END);
    if (end < 0)
    {
        close(fd);
        return false;
    }

    datafile->fd = fd;
    datafile->file_id = file_id;
    datafile->write_offset = (uint64_t)end;
    datafile->mode = mode;
    return true;
}

void datafile_close(datafile_t *datafile)
{
    if (datafile->fd != -1)
    {
        close(datafile->fd);
    }
    datafile_init(datafile);
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

bool datafile_append(datafile_t *datafile,
                     uint64_t timestamp,
                     const uint8_t *key, size_t key_size,
                     const uint8_t *value, size_t value_size,
                     keydir_value_t *out)
{
    if (datafile->fd == -1 || datafile->mode == DATAFILE_READ || out == NULL)
    {
        return false;
    }

    if ((key == NULL && key_size != 0) || (value == NULL && value_size != 0))
    {
        return false;
    }

    if (key_size > SIZE_MAX - ENTRY_HEADER_SIZE || value_size > SIZE_MAX - (ENTRY_HEADER_SIZE + key_size))
    {
        return false;
    }
    size_t total = ENTRY_HEADER_SIZE + key_size + value_size;

    if (total > BITCASK_SSIZE_MAX)
    {
        return false;
    }

    if (datafile->write_offset > UINT64_MAX - (uint64_t)total)
    {
        return false;
    }

    off_t write_offset;
    if (!u64_to_off_t(datafile->write_offset, &write_offset))
    {
        return false;
    }

    off_t pos = lseek(datafile->fd, write_offset, SEEK_SET);
    if (pos < 0)
    {
        return false;
    }

    // encode header values
    uint8_t header[ENTRY_HEADER_SIZE];
    if (!entry_header_encode(header, 0, timestamp, (uint64_t)key_size, (uint64_t)value_size))
    {
        return false;
    };

    // compute crc
    uint32_t crc = crc_init();
    crc = crc32_update(crc, header + ENTRY_HEADER_TIMESTAMP_OFFSET, ENTRY_HEADER_SIZE - ENTRY_HEADER_TIMESTAMP_OFFSET);
    crc = crc32_update(crc, key, key_size);
    crc = crc32_update(crc, value, value_size);
    crc = crc32_final(crc);

    // add crc value to header buf
    encode_u32_le(header, crc);

    const struct iovec iov[3] =
        {
            {.iov_base = header, .iov_len = ENTRY_HEADER_SIZE},
            {.iov_base = (void *)key, .iov_len = key_size},
            {.iov_base = (void *)value, .iov_len = value_size},
        };
    ssize_t written = writev(datafile->fd, iov, 3);
    if (written < 0 || (size_t)written != total)
    {
        return false;
    }

    datafile->write_offset += (uint64_t)total;

    uint64_t entry_pos;
    if (!off_t_to_u64(pos, &entry_pos))
    {
        return false;
    }

    if ((uint64_t)ENTRY_HEADER_SIZE > UINT64_MAX - entry_pos ||
        (uint64_t)key_size > UINT64_MAX - (entry_pos + (uint64_t)ENTRY_HEADER_SIZE))
    {
        return false;
    }

    out->crc = crc;
    out->timestamp = timestamp;
    out->file_id = datafile->file_id;
    out->value_size = (uint64_t)value_size;
    out->value_pos = entry_pos + (uint64_t)ENTRY_HEADER_SIZE + (uint64_t)key_size;

    return true;
}

bool datafile_read_value_at(const datafile_t *datafile,
                            uint64_t value_pos,
                            size_t value_size,
                            uint8_t *out)
{
    if (datafile->fd == -1 || out == NULL)
    {
        return false;
    }

    if (value_size > BITCASK_SSIZE_MAX)
    {
        return false;
    }

    off_t read_offset;
    if (!u64_to_off_t(value_pos, &read_offset))
    {
        return false;
    }

    off_t pos = lseek(datafile->fd, read_offset, SEEK_SET);
    if (pos < 0)
    {
        return false;
    }

    ssize_t read_bytes = read(datafile->fd, out, value_size);
    if (read_bytes < 0 || (size_t)read_bytes != value_size)
    {
        return false;
    }

    return true;
}
