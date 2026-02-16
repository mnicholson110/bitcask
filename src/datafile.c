#include "../include/datafile.h"
#include "../include/crc.h"
#include "../include/entry.h"
#include "../include/io_util.h"
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

void datafile_init(datafile_t *datafile)
{
    datafile->fd = -1;
    datafile->file_id = 0;
    datafile->write_offset = 0;
    datafile->mode = DATAFILE_READ;
}

bool datafile_open(datafile_t *datafile, const char *path,
                   uint32_t file_id, datafile_mode_t mode)
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
                     const uint8_t *key, uint32_t key_size,
                     const uint8_t *value, uint32_t value_size,
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

    size_t total = ENTRY_HEADER_SIZE + key_size + value_size;
    if (!write_entry_exact(datafile->fd, header, key, key_size, value, value_size, datafile->write_offset, total))
    {
        return false;
    }

    off_t entry_pos = datafile->write_offset;
    datafile->write_offset += total;

    out->timestamp = timestamp;
    out->file_id = datafile->file_id;
    out->value_size = value_size;
    out->value_pos = entry_pos + ENTRY_HEADER_SIZE + key_size;

    return true;
}

bool datafile_read_at(const datafile_t *datafile,
                      off_t offset,
                      uint32_t size,
                      uint8_t *out)
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
