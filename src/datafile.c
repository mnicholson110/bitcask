#include "../include/datafile.h"

#define BITCASK_SSIZE_MAX ((size_t)(~(size_t)0 >> 1))

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

    struct stat st;
    if (fstat(fd, &st) < 0)
    {
        close(fd);
        return false;
    }
    if (st.st_size < 0 || st.st_size > MAX_FILE_SIZE)
    {
        close(fd);
        return false;
    }

    datafile->fd = fd;
    datafile->file_id = file_id;
    datafile->write_offset = st.st_size;
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

    size_t total = ENTRY_HEADER_SIZE + key_size + value_size;

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
    ssize_t written = pwritev(datafile->fd, iov, 3, datafile->write_offset);
    if (written < 0 || (size_t)written != total)
    {
        return false;
    }

    off_t entry_pos = datafile->write_offset;
    datafile->write_offset += total;

    out->crc = crc;
    out->timestamp = timestamp;
    out->file_id = datafile->file_id;
    out->value_size = (uint64_t)value_size;
    out->value_pos = entry_pos + ENTRY_HEADER_SIZE + key_size;

    return true;
}

bool datafile_read_value_at(const datafile_t *datafile,
                            off_t value_pos,
                            size_t value_size,
                            uint8_t *out)
{
    if (datafile->fd == -1 || out == NULL)
    {
        return false;
    }

    ssize_t read_bytes = pread(datafile->fd, out, value_size, value_pos);
    if (read_bytes < 0 || (size_t)read_bytes != value_size)
    {
        return false;
    }

    return true;
}
