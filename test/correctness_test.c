#include "../include/bitcask.h"
#include "../include/entry.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

typedef bool (*test_fn_t)(void);

typedef struct test_case
{
    const char *name;
    test_fn_t fn;
} test_case_t;

static bool rm_rf(const char *path)
{
    char cmd[512];
    int n = snprintf(cmd, sizeof(cmd), "rm -rf %s", path);
    if (n < 0 || (size_t)n >= sizeof(cmd))
    {
        return false;
    }
    return system(cmd) == 0;
}

static bool cleanup_test_dirs(void)
{
    const char *dirs[] = {
        "test/test-basic",
        "test/test-limits",
        "test/test-zero-key",
        "test/test-empty-delete",
        "test/test-last-write",
        "test/test-rotate",
        "test/test-first-rotate",
        "test/test-max-boundary",
        "test/test-get-failure-out",
        "test/test-corrupt-sizes",
        "test/test-readonly-delete",
        "test/test-concurrent-readers",
        "test/test-reopen",
        "test/test-readonly-existing",
        "test/test-readonly-missing",
        "test/test-crc-get",
        "test/test-crc-open",
        "test/test-merge-compact",
        "test/test-merge-hint-values",
        "test/test-merge-readonly",
        "test/test-merge-no-inactive",
        "test/bench-seq",
        "test/bench-mixed",
    };

    bool ok = true;
    size_t n = sizeof(dirs) / sizeof(dirs[0]);
    for (size_t i = 0; i < n; i++)
    {
        if (!rm_rf(dirs[i]))
        {
            ok = false;
        }
    }

    return ok;
}

static bool path_exists(const char *path)
{
    struct stat sb;
    return stat(path, &sb) == 0;
}

static bool write_byte_at(const char *path, long offset, uint8_t byte)
{
    FILE *fp = fopen(path, "r+b");
    if (fp == NULL)
    {
        return false;
    }
    if (fseek(fp, offset, SEEK_SET) != 0)
    {
        fclose(fp);
        return false;
    }
    if (fputc((int)byte, fp) == EOF)
    {
        fclose(fp);
        return false;
    }
    if (fclose(fp) != 0)
    {
        return false;
    }
    return true;
}

static long value_offset_for_key_size(size_t key_size)
{
    return (long)(ENTRY_HEADER_SIZE + key_size);
}

static bool write_u32_le_at(const char *path, long offset, uint32_t value)
{
    FILE *fp = fopen(path, "r+b");
    if (fp == NULL)
    {
        return false;
    }
    if (fseek(fp, offset, SEEK_SET) != 0)
    {
        fclose(fp);
        return false;
    }

    uint8_t buf[4];
    encode_u32_le(buf, value);
    if (fwrite(buf, 1, sizeof(buf), fp) != sizeof(buf))
    {
        fclose(fp);
        return false;
    }
    if (fclose(fp) != 0)
    {
        return false;
    }
    return true;
}

static bool truncate_file_to(const char *path, off_t size)
{
    return truncate(path, size) == 0;
}

typedef struct seed_entry
{
    const char *key;
    const char *value; // NULL -> tombstone
} seed_entry_t;

static bool build_datafile_path(const char *dir, uint32_t file_id, const char *suffix,
                                char *out, size_t out_size)
{
    int n = snprintf(out, out_size, "%s/%02u%s", dir, (unsigned)file_id, suffix);
    if (n < 0 || (size_t)n >= out_size)
    {
        return false;
    }
    return true;
}

static bool seed_datafile(const char *dir, uint32_t file_id,
                          const seed_entry_t *entries, size_t entry_count)
{
    char path[512];
    if (!build_datafile_path(dir, file_id, ".data", path, sizeof(path)))
    {
        return false;
    }

    datafile_t file;
    datafile_init(&file);
    if (!datafile_open(&file, dir, file_id, DATAFILE_READ_WRITE))
    {
        return false;
    }

    bool ok = true;
    uint64_t ts = 1;
    for (size_t i = 0; i < entry_count; i++)
    {
        const uint8_t *key = (const uint8_t *)entries[i].key;
        uint32_t key_size = (uint32_t)strlen(entries[i].key);
        const uint8_t *value = entries[i].value == NULL ? NULL : (const uint8_t *)entries[i].value;
        uint32_t value_size = entries[i].value == NULL ? 0u : (uint32_t)strlen(entries[i].value);

        keydir_value_t out;
        if (!datafile_append(&file, ts, key, key_size, value, value_size, &out))
        {
            ok = false;
            break;
        }
        ts++;
    }

    datafile_close(&file);
    if (!ok)
    {
        (void)unlink(path);
    }
    return ok;
}

static bool expect_value_eq(bitcask_handle_t *db,
                            const uint8_t *key, size_t key_size,
                            const uint8_t *expected, size_t expected_size)
{
    uint8_t *out = NULL;
    size_t out_size = 0;
    bool ok = bitcask_get(db, key, key_size, &out, &out_size);
    if (!ok || out_size != expected_size || memcmp(out, expected, expected_size) != 0)
    {
        free(out);
        return false;
    }
    free(out);
    return true;
}

static bool expect_missing(bitcask_handle_t *db, const uint8_t *key, size_t key_size)
{
    uint8_t *out = NULL;
    size_t out_size = 0;
    if (bitcask_get(db, key, key_size, &out, &out_size))
    {
        free(out);
        return false;
    }
    return true;
}

static bool test_basic_put_get_delete(void)
{
    const char *dir = "test/test-basic";
    if (!rm_rf(dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    if (!bitcask_put(&db, (const uint8_t *)"alpha", 5, (const uint8_t *)"one", 3))
    {
        bitcask_close(&db);
        return false;
    }
    if (!bitcask_put(&db, (const uint8_t *)"beta", 4, (const uint8_t *)"two", 3))
    {
        bitcask_close(&db);
        return false;
    }

    uint8_t *out = NULL;
    size_t out_size = 0;
    bool ok = bitcask_get(&db, (const uint8_t *)"alpha", 5, &out, &out_size);
    if (!ok || out_size != 3 || memcmp(out, "one", 3) != 0)
    {
        free(out);
        bitcask_close(&db);
        return false;
    }
    free(out);

    if (!bitcask_delete(&db, (const uint8_t *)"alpha", 5))
    {
        bitcask_close(&db);
        return false;
    }

    out = NULL;
    out_size = 0;
    if (bitcask_get(&db, (const uint8_t *)"alpha", 5, &out, &out_size))
    {
        free(out);
        bitcask_close(&db);
        return false;
    }

    bitcask_close(&db);
    return true;
}

static bool test_size_limits(void)
{
    const char *dir = "test/test-limits";
    if (!rm_rf(dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    size_t too_big_key_size = MAX_KEY_SIZE + 1;
    size_t too_big_value_size = MAX_VALUE_SIZE + 1;
    uint8_t *too_big_key = malloc(too_big_key_size);
    uint8_t *too_big_value = malloc(too_big_value_size);
    if (too_big_key == NULL || too_big_value == NULL)
    {
        free(too_big_key);
        free(too_big_value);
        bitcask_close(&db);
        return false;
    }

    memset(too_big_key, 'k', too_big_key_size);
    memset(too_big_value, 'v', too_big_value_size);

    bool ok = true;
    if (bitcask_put(&db, too_big_key, too_big_key_size, (const uint8_t *)"x", 1))
    {
        ok = false;
    }
    if (bitcask_put(&db, (const uint8_t *)"k", 1, too_big_value, too_big_value_size))
    {
        ok = false;
    }

    free(too_big_key);
    free(too_big_value);
    bitcask_close(&db);
    return ok;
}

static bool test_zero_length_key_rejected(void)
{
    const char *dir = "test/test-zero-key";
    if (!rm_rf(dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    if (bitcask_put(&db, (const uint8_t *)"", 0, (const uint8_t *)"x", 1))
    {
        bitcask_close(&db);
        return false;
    }

    uint8_t sentinel = 0;
    uint8_t *out = &sentinel;
    size_t out_size = 123;
    if (bitcask_get(&db, (const uint8_t *)"", 0, &out, &out_size))
    {
        bitcask_close(&db);
        return false;
    }
    if (out != &sentinel || out_size != 123)
    {
        bitcask_close(&db);
        return false;
    }

    bitcask_close(&db);
    return true;
}

static bool test_empty_value_is_delete_tombstone(void)
{
    const char *dir = "test/test-empty-delete";
    if (!rm_rf(dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    if (!bitcask_put(&db, (const uint8_t *)"gone", 4, (const uint8_t *)"v1", 2))
    {
        bitcask_close(&db);
        return false;
    }
    if (!bitcask_delete(&db, (const uint8_t *)"gone", 4))
    {
        bitcask_close(&db);
        return false;
    }

    uint8_t *out = NULL;
    size_t out_size = 0;
    if (bitcask_get(&db, (const uint8_t *)"gone", 4, &out, &out_size))
    {
        free(out);
        bitcask_close(&db);
        return false;
    }

    bitcask_close(&db);

    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }
    if (bitcask_get(&db, (const uint8_t *)"gone", 4, &out, &out_size))
    {
        free(out);
        bitcask_close(&db);
        return false;
    }

    bitcask_close(&db);
    return true;
}

static bool test_last_write_wins_same_key(void)
{
    const char *dir = "test/test-last-write";
    if (!rm_rf(dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    if (!bitcask_put(&db, (const uint8_t *)"k", 1, (const uint8_t *)"first", 5))
    {
        bitcask_close(&db);
        return false;
    }
    if (!bitcask_put(&db, (const uint8_t *)"k", 1, (const uint8_t *)"second", 6))
    {
        bitcask_close(&db);
        return false;
    }
    if (!expect_value_eq(&db, (const uint8_t *)"k", 1, (const uint8_t *)"second", 6))
    {
        bitcask_close(&db);
        return false;
    }

    bitcask_close(&db);

    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }
    bool ok = expect_value_eq(&db, (const uint8_t *)"k", 1, (const uint8_t *)"second", 6);
    bitcask_close(&db);
    return ok;
}

static void fill_tagged_value(uint8_t *value, size_t value_size, uint8_t tag)
{
    memset(value, tag, value_size);
}

static bool check_tagged_value(const uint8_t *value, size_t value_size, uint8_t tag)
{
    if (value_size == 0)
    {
        return false;
    }
    return value[0] == tag && value[value_size - 1] == tag;
}

static bool test_file_rotation_and_reopen(void)
{
    const char *dir = "test/test-rotate";
    const char *next_file = "test/test-rotate/02.data";
    if (!rm_rf(dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    const size_t key_buf_size = 32;
    char key_buf[32];
    size_t value_size = MAX_VALUE_SIZE;
    uint8_t *value = malloc(value_size);
    if (value == NULL)
    {
        bitcask_close(&db);
        return false;
    }

    size_t entry_size = ENTRY_HEADER_SIZE + 8 + value_size;
    size_t writes = (MAX_FILE_SIZE / entry_size) + 2;

    for (size_t i = 0; i < writes; i++)
    {
        int key_n = snprintf(key_buf, key_buf_size, "k%06zu", i);
        if (key_n < 0 || (size_t)key_n >= key_buf_size)
        {
            free(value);
            bitcask_close(&db);
            return false;
        }
        fill_tagged_value(value, value_size, (uint8_t)i);
        if (!bitcask_put(&db, (const uint8_t *)key_buf, (size_t)key_n, value, value_size))
        {
            free(value);
            bitcask_close(&db);
            return false;
        }
    }

    if (!path_exists(next_file))
    {
        free(value);
        bitcask_close(&db);
        return false;
    }

    uint8_t *out = NULL;
    size_t out_size = 0;

    int first_n = snprintf(key_buf, key_buf_size, "k%06d", 0);
    if (first_n < 0 || (size_t)first_n >= key_buf_size)
    {
        free(value);
        bitcask_close(&db);
        return false;
    }
    if (!bitcask_get(&db, (const uint8_t *)key_buf, (size_t)first_n, &out, &out_size) ||
        out_size != value_size || !check_tagged_value(out, out_size, (uint8_t)0))
    {
        free(out);
        free(value);
        bitcask_close(&db);
        return false;
    }
    free(out);
    out = NULL;

    int last_n = snprintf(key_buf, key_buf_size, "k%06zu", writes - 1);
    if (last_n < 0 || (size_t)last_n >= key_buf_size)
    {
        free(value);
        bitcask_close(&db);
        return false;
    }
    if (!bitcask_get(&db, (const uint8_t *)key_buf, (size_t)last_n, &out, &out_size) ||
        out_size != value_size || !check_tagged_value(out, out_size, (uint8_t)(writes - 1)))
    {
        free(out);
        free(value);
        bitcask_close(&db);
        return false;
    }
    free(out);
    out = NULL;

    bitcask_close(&db);

    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        free(value);
        return false;
    }

    first_n = snprintf(key_buf, key_buf_size, "k%06d", 0);
    if (first_n < 0 || (size_t)first_n >= key_buf_size)
    {
        free(value);
        bitcask_close(&db);
        return false;
    }
    if (!bitcask_get(&db, (const uint8_t *)key_buf, (size_t)first_n, &out, &out_size) ||
        out_size != value_size || !check_tagged_value(out, out_size, (uint8_t)0))
    {
        free(out);
        free(value);
        bitcask_close(&db);
        return false;
    }
    free(out);
    out = NULL;

    last_n = snprintf(key_buf, key_buf_size, "k%06zu", writes - 1);
    if (last_n < 0 || (size_t)last_n >= key_buf_size)
    {
        free(value);
        bitcask_close(&db);
        return false;
    }
    if (!bitcask_get(&db, (const uint8_t *)key_buf, (size_t)last_n, &out, &out_size) ||
        out_size != value_size || !check_tagged_value(out, out_size, (uint8_t)(writes - 1)))
    {
        free(out);
        free(value);
        bitcask_close(&db);
        return false;
    }

    free(out);
    out = NULL;
    free(value);
    bitcask_close(&db);
    return true;
}

static bool test_first_rotation_edge(void)
{
    const char *dir = "test/test-first-rotate";
    const char *second_file = "test/test-first-rotate/02.data";
    if (!rm_rf(dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    const size_t value_size = MAX_VALUE_SIZE;
    uint8_t *value = malloc(value_size);
    if (value == NULL)
    {
        bitcask_close(&db);
        return false;
    }
    memset(value, 'r', value_size);

    const size_t entry_size = ENTRY_HEADER_SIZE + 2 + value_size; // 2-byte key
    const size_t writes_before_rotate = ((MAX_FILE_SIZE - entry_size) / entry_size) + 1;

    for (size_t i = 0; i < writes_before_rotate; i++)
    {
        uint8_t key[2] = {(uint8_t)(i & 0xFFu), (uint8_t)((i >> 8) & 0xFFu)};
        if (!bitcask_put(&db, key, sizeof(key), value, value_size))
        {
            free(value);
            bitcask_close(&db);
            return false;
        }
    }

    if (path_exists(second_file))
    {
        free(value);
        bitcask_close(&db);
        return false;
    }

    uint8_t rotate_key[2] = {0xAAu, 0x55u};
    if (!bitcask_put(&db, rotate_key, sizeof(rotate_key), value, value_size))
    {
        free(value);
        bitcask_close(&db);
        return false;
    }
    if (!path_exists(second_file))
    {
        free(value);
        bitcask_close(&db);
        return false;
    }

    uint8_t first_key[2] = {0x00u, 0x00u};
    uint8_t *out = NULL;
    size_t out_size = 0;
    if (!bitcask_get(&db, first_key, sizeof(first_key), &out, &out_size) ||
        out_size != value_size || out[0] != 'r' || out[out_size - 1] != 'r')
    {
        free(out);
        free(value);
        bitcask_close(&db);
        return false;
    }
    free(out);
    out = NULL;

    if (!bitcask_get(&db, rotate_key, sizeof(rotate_key), &out, &out_size) ||
        out_size != value_size || out[0] != 'r' || out[out_size - 1] != 'r')
    {
        free(out);
        free(value);
        bitcask_close(&db);
        return false;
    }

    free(out);
    free(value);
    bitcask_close(&db);
    return true;
}

static bool test_boundary_sizes_exact_max(void)
{
    const char *dir = "test/test-max-boundary";
    if (!rm_rf(dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    uint8_t *key = malloc(MAX_KEY_SIZE);
    uint8_t *value = malloc(MAX_VALUE_SIZE);
    if (key == NULL || value == NULL)
    {
        free(key);
        free(value);
        bitcask_close(&db);
        return false;
    }

    memset(key, 'k', MAX_KEY_SIZE);
    memset(value, 'v', MAX_VALUE_SIZE);

    if (!bitcask_put(&db, key, MAX_KEY_SIZE, value, MAX_VALUE_SIZE))
    {
        free(key);
        free(value);
        bitcask_close(&db);
        return false;
    }

    uint8_t *out = NULL;
    size_t out_size = 0;
    bool ok = bitcask_get(&db, key, MAX_KEY_SIZE, &out, &out_size) &&
              out_size == MAX_VALUE_SIZE &&
              memcmp(out, value, MAX_VALUE_SIZE) == 0;

    free(out);
    free(key);
    free(value);
    bitcask_close(&db);
    return ok;
}

static bool test_get_failure_out_params(void)
{
    const char *dir = "test/test-get-failure-out";
    const char *datafile = "test/test-get-failure-out/01.data";
    if (!rm_rf(dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    if (!bitcask_put(&db, (const uint8_t *)"k", 1, (const uint8_t *)"hello", 5))
    {
        bitcask_close(&db);
        return false;
    }

    uint8_t sentinel = 0;
    uint8_t *out = &sentinel;
    size_t out_size = 111;
    if (bitcask_get(&db, (const uint8_t *)"missing", 7, &out, &out_size))
    {
        bitcask_close(&db);
        return false;
    }
    if (out != &sentinel || out_size != 111)
    {
        bitcask_close(&db);
        return false;
    }

    // Force a short read to exercise the out-param behavior on true I/O failure.
    if (!truncate_file_to(datafile, value_offset_for_key_size(1) + 2))
    {
        bitcask_close(&db);
        return false;
    }

    out = &sentinel;
    out_size = 222;
    if (bitcask_get(&db, (const uint8_t *)"k", 1, &out, &out_size))
    {
        free(out);
        bitcask_close(&db);
        return false;
    }
    if (out != NULL || out_size != 0)
    {
        bitcask_close(&db);
        return false;
    }

    bitcask_close(&db);
    return true;
}

static bool test_corrupt_header_sizes_rejected_on_open(void)
{
    const char *dir = "test/test-corrupt-sizes";
    const char *datafile = "test/test-corrupt-sizes/01.data";

    if (!rm_rf(dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }
    if (!bitcask_put(&db, (const uint8_t *)"k", 1, (const uint8_t *)"hello", 5))
    {
        bitcask_close(&db);
        return false;
    }
    bitcask_close(&db);

    if (!write_u32_le_at(datafile, ENTRY_HEADER_KEY_SIZE_OFFSET, ((uint32_t)MAX_KEY_SIZE) + 1u))
    {
        return false;
    }
    if (bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        bitcask_close(&db);
        return false;
    }

    if (!rm_rf(dir))
    {
        return false;
    }

    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }
    if (!bitcask_put(&db, (const uint8_t *)"k", 1, (const uint8_t *)"hello", 5))
    {
        bitcask_close(&db);
        return false;
    }
    bitcask_close(&db);

    if (!write_u32_le_at(datafile, ENTRY_HEADER_VALUE_SIZE_OFFSET, ((uint32_t)MAX_VALUE_SIZE) + 1u))
    {
        return false;
    }
    if (bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        bitcask_close(&db);
        return false;
    }

    return true;
}

static bool test_read_only_delete_rejected(void)
{
    const char *dir = "test/test-readonly-delete";
    if (!rm_rf(dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }
    if (!bitcask_put(&db, (const uint8_t *)"k", 1, (const uint8_t *)"v", 1))
    {
        bitcask_close(&db);
        return false;
    }
    bitcask_close(&db);

    if (!bitcask_open(&db, dir, BITCASK_READ_ONLY))
    {
        return false;
    }
    if (bitcask_delete(&db, (const uint8_t *)"k", 1))
    {
        bitcask_close(&db);
        return false;
    }
    bitcask_close(&db);
    return true;
}

typedef struct reader_ctx
{
    bitcask_handle_t *db;
    size_t keyspace;
    size_t iterations;
    uint32_t seed;
    bool ok;
} reader_ctx_t;

static void *reader_thread_main(void *arg)
{
    reader_ctx_t *ctx = (reader_ctx_t *)arg;
    ctx->ok = true;

    char key[32];
    char expected[32];

    for (size_t i = 0; i < ctx->iterations; i++)
    {
        ctx->seed = ctx->seed * 1664525u + 1013904223u;
        size_t idx = (size_t)(ctx->seed % (uint32_t)ctx->keyspace);

        int key_n = snprintf(key, sizeof(key), "k%06zu", idx);
        int expected_n = snprintf(expected, sizeof(expected), "value-%06zu", idx);
        if (key_n < 0 || expected_n < 0 ||
            (size_t)key_n >= sizeof(key) || (size_t)expected_n >= sizeof(expected))
        {
            ctx->ok = false;
            return NULL;
        }

        if (!expect_value_eq(ctx->db, (const uint8_t *)key, (size_t)key_n,
                             (const uint8_t *)expected, (size_t)expected_n))
        {
            ctx->ok = false;
            return NULL;
        }
    }

    return NULL;
}

static bool test_concurrent_readers(void)
{
    const char *dir = "test/test-concurrent-readers";
    if (!rm_rf(dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    const size_t keyspace = 256;
    char key[32];
    char value[32];
    for (size_t i = 0; i < keyspace; i++)
    {
        int key_n = snprintf(key, sizeof(key), "k%06zu", i);
        int value_n = snprintf(value, sizeof(value), "value-%06zu", i);
        if (key_n < 0 || value_n < 0 ||
            (size_t)key_n >= sizeof(key) || (size_t)value_n >= sizeof(value))
        {
            bitcask_close(&db);
            return false;
        }
        if (!bitcask_put(&db, (const uint8_t *)key, (size_t)key_n,
                         (const uint8_t *)value, (size_t)value_n))
        {
            bitcask_close(&db);
            return false;
        }
    }
    bitcask_close(&db);

    if (!bitcask_open(&db, dir, BITCASK_READ_ONLY))
    {
        return false;
    }

    const size_t thread_count = 8;
    pthread_t threads[8];
    reader_ctx_t ctx[8];

    for (size_t i = 0; i < thread_count; i++)
    {
        ctx[i].db = &db;
        ctx[i].keyspace = keyspace;
        ctx[i].iterations = 3000;
        ctx[i].seed = (uint32_t)(i + 1);
        ctx[i].ok = false;

        if (pthread_create(&threads[i], NULL, reader_thread_main, &ctx[i]) != 0)
        {
            bitcask_close(&db);
            return false;
        }
    }

    bool ok = true;
    for (size_t i = 0; i < thread_count; i++)
    {
        if (pthread_join(threads[i], NULL) != 0)
        {
            ok = false;
        }
        if (!ctx[i].ok)
        {
            ok = false;
        }
    }

    bitcask_close(&db);
    return ok;
}

static bool test_reopen_persistence(void)
{
    const char *dir = "test/test-reopen";
    if (!rm_rf(dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }
    if (!bitcask_put(&db, (const uint8_t *)"persist", 7, (const uint8_t *)"hello-world", 11))
    {
        bitcask_close(&db);
        return false;
    }
    bitcask_close(&db);

    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    uint8_t *out = NULL;
    size_t out_size = 0;
    bool ok = bitcask_get(&db, (const uint8_t *)"persist", 7, &out, &out_size);
    bool valid = ok && out_size == 11 && memcmp(out, "hello-world", 11) == 0;
    free(out);
    bitcask_close(&db);
    return valid;
}

static bool test_read_only_semantics(void)
{
    const char *dir = "test/test-readonly-existing";
    const char *missing = "test/test-readonly-missing";
    if (!rm_rf(dir) || !rm_rf(missing))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }
    if (!bitcask_put(&db, (const uint8_t *)"k", 1, (const uint8_t *)"v", 1))
    {
        bitcask_close(&db);
        return false;
    }
    bitcask_close(&db);

    if (!bitcask_open(&db, dir, BITCASK_READ_ONLY))
    {
        return false;
    }

    uint8_t *out = NULL;
    size_t out_size = 0;
    bool get_ok = bitcask_get(&db, (const uint8_t *)"k", 1, &out, &out_size);
    free(out);
    if (!get_ok || out_size != 1)
    {
        bitcask_close(&db);
        return false;
    }

    if (bitcask_put(&db, (const uint8_t *)"x", 1, (const uint8_t *)"y", 1))
    {
        bitcask_close(&db);
        return false;
    }
    bitcask_close(&db);

    if (bitcask_open(&db, missing, BITCASK_READ_ONLY))
    {
        bitcask_close(&db);
        return false;
    }
    if (path_exists(missing))
    {
        return false;
    }
    return true;
}

static bool test_crc_not_checked_on_get(void)
{
    const char *dir = "test/test-crc-get";
    const char *datafile = "test/test-crc-get/01.data";
    if (!rm_rf(dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }
    if (!bitcask_put(&db, (const uint8_t *)"k", 1, (const uint8_t *)"hello", 5))
    {
        bitcask_close(&db);
        return false;
    }

    if (!write_byte_at(datafile, value_offset_for_key_size(1), (uint8_t)'X'))
    {
        bitcask_close(&db);
        return false;
    }

    uint8_t *out = NULL;
    size_t out_size = 0;
    bool ok = bitcask_get(&db, (const uint8_t *)"k", 1, &out, &out_size);
    bool valid = ok && out_size == 5 && memcmp(out, "Xello", 5) == 0;
    free(out);
    bitcask_close(&db);
    return valid;
}

static bool test_crc_rejected_on_reopen(void)
{
    const char *dir = "test/test-crc-open";
    const char *datafile = "test/test-crc-open/01.data";
    if (!rm_rf(dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }
    if (!bitcask_put(&db, (const uint8_t *)"k", 1, (const uint8_t *)"hello", 5))
    {
        bitcask_close(&db);
        return false;
    }
    bitcask_close(&db);

    if (!write_byte_at(datafile, value_offset_for_key_size(1), (uint8_t)'X'))
    {
        return false;
    }

    if (bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        bitcask_close(&db);
        return false;
    }
    return true;
}

static bool test_merge_compacts_inactive_files(void)
{
    const char *dir = "test/test-merge-compact";
    const char *old_1 = "test/test-merge-compact/01.data";
    const char *old_2 = "test/test-merge-compact/02.data";
    const char *active_3 = "test/test-merge-compact/03.data";
    const char *merged_4 = "test/test-merge-compact/04.data";
    const char *merged_4_tmp = "test/test-merge-compact/04.data.merge";
    if (!rm_rf(dir))
    {
        return false;
    }
    if (mkdir(dir, 0755) != 0)
    {
        return false;
    }

    const seed_entry_t file1_entries[] = {
        {.key = "alpha", .value = "alpha-v1"},
        {.key = "beta", .value = "beta-v1"},
        {.key = "gamma", .value = "gamma-v1"},
    };
    const seed_entry_t file2_entries[] = {
        {.key = "alpha", .value = "alpha-v2"},
        {.key = "beta", .value = NULL},
        {.key = "delta", .value = "delta-v1"},
    };

    if (!seed_datafile(dir, 1, file1_entries, sizeof(file1_entries) / sizeof(file1_entries[0])))
    {
        return false;
    }
    if (!seed_datafile(dir, 2, file2_entries, sizeof(file2_entries) / sizeof(file2_entries[0])))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    if (!bitcask_put(&db, (const uint8_t *)"gamma", 5, (const uint8_t *)"gamma-active", 12))
    {
        bitcask_close(&db);
        return false;
    }
    if (!bitcask_put(&db, (const uint8_t *)"delta", 5, (const uint8_t *)"delta-active", 12))
    {
        bitcask_close(&db);
        return false;
    }
    if (!bitcask_put(&db, (const uint8_t *)"epsilon", 7, (const uint8_t *)"epsilon-active", 14))
    {
        bitcask_close(&db);
        return false;
    }

    if (!bitcask_merge(&db))
    {
        bitcask_close(&db);
        return false;
    }

    if (!expect_value_eq(&db, (const uint8_t *)"alpha", 5, (const uint8_t *)"alpha-v2", 8) ||
        !expect_missing(&db, (const uint8_t *)"beta", 4) ||
        !expect_value_eq(&db, (const uint8_t *)"gamma", 5, (const uint8_t *)"gamma-active", 12) ||
        !expect_value_eq(&db, (const uint8_t *)"delta", 5, (const uint8_t *)"delta-active", 12) ||
        !expect_value_eq(&db, (const uint8_t *)"epsilon", 7, (const uint8_t *)"epsilon-active", 14))
    {
        bitcask_close(&db);
        return false;
    }

    bitcask_close(&db);

    if (path_exists(old_1) || path_exists(old_2) || !path_exists(active_3) ||
        !path_exists(merged_4) || path_exists(merged_4_tmp))
    {
        return false;
    }

    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }
    bool ok = expect_value_eq(&db, (const uint8_t *)"alpha", 5, (const uint8_t *)"alpha-v2", 8) &&
              expect_missing(&db, (const uint8_t *)"beta", 4) &&
              expect_value_eq(&db, (const uint8_t *)"gamma", 5, (const uint8_t *)"gamma-active", 12) &&
              expect_value_eq(&db, (const uint8_t *)"delta", 5, (const uint8_t *)"delta-active", 12) &&
              expect_value_eq(&db, (const uint8_t *)"epsilon", 7, (const uint8_t *)"epsilon-active", 14);
    bitcask_close(&db);
    return ok;
}

static bool test_merge_hint_values_consistent(void)
{
    const char *dir = "test/test-merge-hint-values";
    const char *merged_data = "test/test-merge-hint-values/04.data";
    const char *merged_hint = "test/test-merge-hint-values/04.hint";
    const char *merged_hint_tmp = "test/test-merge-hint-values/04.hint.merge";
    if (!rm_rf(dir))
    {
        return false;
    }
    if (mkdir(dir, 0755) != 0)
    {
        return false;
    }

    const seed_entry_t file1_entries[] = {
        {.key = "alpha", .value = "alpha-v1-short"},
        {.key = "beta", .value = "beta-v1"},
        {.key = "gamma", .value = "gamma-v1-short"},
        {.key = "zeta", .value = "zeta-v1-qqqqqqqqqqqqqqqq"},
        {.key = "epsilon", .value = "epsilon-v1"},
    };
    const seed_entry_t file2_entries[] = {
        {.key = "alpha", .value = "alpha-v2-0000000000000000000000000001"},
        {.key = "beta", .value = NULL},
        {.key = "gamma", .value = "gamma-v2-111111111111111111111111111111111111"},
        {.key = "delta", .value = "delta-v1-XYZ"},
        {.key = "epsilon", .value = NULL},
        {.key = "eta", .value = "eta-v1-final"},
    };

    if (!seed_datafile(dir, 1, file1_entries, sizeof(file1_entries) / sizeof(file1_entries[0])) ||
        !seed_datafile(dir, 2, file2_entries, sizeof(file2_entries) / sizeof(file2_entries[0])))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    uint8_t *alpha_before = NULL;
    uint8_t *gamma_before = NULL;
    uint8_t *delta_before = NULL;
    uint8_t *zeta_before = NULL;
    uint8_t *eta_before = NULL;
    size_t alpha_before_size = 0;
    size_t gamma_before_size = 0;
    size_t delta_before_size = 0;
    size_t zeta_before_size = 0;
    size_t eta_before_size = 0;

    if (!bitcask_get(&db, (const uint8_t *)"alpha", 5, &alpha_before, &alpha_before_size) ||
        !bitcask_get(&db, (const uint8_t *)"gamma", 5, &gamma_before, &gamma_before_size) ||
        !bitcask_get(&db, (const uint8_t *)"delta", 5, &delta_before, &delta_before_size) ||
        !bitcask_get(&db, (const uint8_t *)"zeta", 4, &zeta_before, &zeta_before_size) ||
        !bitcask_get(&db, (const uint8_t *)"eta", 3, &eta_before, &eta_before_size))
    {
        free(alpha_before);
        free(gamma_before);
        free(delta_before);
        free(zeta_before);
        free(eta_before);
        bitcask_close(&db);
        return false;
    }

    if (!expect_missing(&db, (const uint8_t *)"beta", 4) ||
        !expect_missing(&db, (const uint8_t *)"epsilon", 7))
    {
        free(alpha_before);
        free(gamma_before);
        free(delta_before);
        free(zeta_before);
        free(eta_before);
        bitcask_close(&db);
        return false;
    }

    if (!bitcask_merge(&db))
    {
        free(alpha_before);
        free(gamma_before);
        free(delta_before);
        free(zeta_before);
        free(eta_before);
        bitcask_close(&db);
        return false;
    }

    if (!expect_value_eq(&db, (const uint8_t *)"alpha", 5, alpha_before, alpha_before_size) ||
        !expect_value_eq(&db, (const uint8_t *)"gamma", 5, gamma_before, gamma_before_size) ||
        !expect_value_eq(&db, (const uint8_t *)"delta", 5, delta_before, delta_before_size) ||
        !expect_value_eq(&db, (const uint8_t *)"zeta", 4, zeta_before, zeta_before_size) ||
        !expect_value_eq(&db, (const uint8_t *)"eta", 3, eta_before, eta_before_size) ||
        !expect_missing(&db, (const uint8_t *)"beta", 4) ||
        !expect_missing(&db, (const uint8_t *)"epsilon", 7))
    {
        free(alpha_before);
        free(gamma_before);
        free(delta_before);
        free(zeta_before);
        free(eta_before);
        bitcask_close(&db);
        return false;
    }

    bitcask_close(&db);

    if (!path_exists(merged_data) || !path_exists(merged_hint) || path_exists(merged_hint_tmp))
    {
        free(alpha_before);
        free(gamma_before);
        free(delta_before);
        free(zeta_before);
        free(eta_before);
        return false;
    }

    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        free(alpha_before);
        free(gamma_before);
        free(delta_before);
        free(zeta_before);
        free(eta_before);
        return false;
    }

    bool ok = expect_value_eq(&db, (const uint8_t *)"alpha", 5, alpha_before, alpha_before_size) &&
              expect_value_eq(&db, (const uint8_t *)"gamma", 5, gamma_before, gamma_before_size) &&
              expect_value_eq(&db, (const uint8_t *)"delta", 5, delta_before, delta_before_size) &&
              expect_value_eq(&db, (const uint8_t *)"zeta", 4, zeta_before, zeta_before_size) &&
              expect_value_eq(&db, (const uint8_t *)"eta", 3, eta_before, eta_before_size) &&
              expect_missing(&db, (const uint8_t *)"beta", 4) &&
              expect_missing(&db, (const uint8_t *)"epsilon", 7);

    bitcask_close(&db);
    free(alpha_before);
    free(gamma_before);
    free(delta_before);
    free(zeta_before);
    free(eta_before);
    return ok;
}

static bool test_merge_rejected_read_only(void)
{
    const char *dir = "test/test-merge-readonly";
    if (!rm_rf(dir))
    {
        return false;
    }
    if (mkdir(dir, 0755) != 0)
    {
        return false;
    }

    const seed_entry_t file1_entries[] = {
        {.key = "k", .value = "v1"},
    };
    const seed_entry_t file2_entries[] = {
        {.key = "k", .value = "v2"},
    };

    if (!seed_datafile(dir, 1, file1_entries, sizeof(file1_entries) / sizeof(file1_entries[0])) ||
        !seed_datafile(dir, 2, file2_entries, sizeof(file2_entries) / sizeof(file2_entries[0])))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_ONLY))
    {
        return false;
    }

    if (!expect_value_eq(&db, (const uint8_t *)"k", 1, (const uint8_t *)"v2", 2))
    {
        bitcask_close(&db);
        return false;
    }

    if (bitcask_merge(&db))
    {
        bitcask_close(&db);
        return false;
    }

    bitcask_close(&db);
    return true;
}

static bool test_merge_rejected_without_inactive(void)
{
    const char *dir = "test/test-merge-no-inactive";
    if (!rm_rf(dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    if (!bitcask_put(&db, (const uint8_t *)"k", 1, (const uint8_t *)"v", 1))
    {
        bitcask_close(&db);
        return false;
    }

    if (bitcask_merge(&db))
    {
        bitcask_close(&db);
        return false;
    }

    bool ok = expect_value_eq(&db, (const uint8_t *)"k", 1, (const uint8_t *)"v", 1);
    bitcask_close(&db);
    return ok;
}

int main(void)
{
    if (!cleanup_test_dirs())
    {
        return 1;
    }

    test_case_t tests[] = {
        {.name = "basic_put_get_delete", .fn = test_basic_put_get_delete},
        {.name = "size_limits", .fn = test_size_limits},
        {.name = "zero_length_key_rejected", .fn = test_zero_length_key_rejected},
        {.name = "empty_value_is_delete_tombstone", .fn = test_empty_value_is_delete_tombstone},
        {.name = "last_write_wins_same_key", .fn = test_last_write_wins_same_key},
        {.name = "file_rotation_and_reopen", .fn = test_file_rotation_and_reopen},
        {.name = "first_rotation_edge", .fn = test_first_rotation_edge},
        {.name = "boundary_sizes_exact_max", .fn = test_boundary_sizes_exact_max},
        {.name = "get_failure_out_params", .fn = test_get_failure_out_params},
        {.name = "corrupt_header_sizes_rejected_on_open", .fn = test_corrupt_header_sizes_rejected_on_open},
        {.name = "read_only_delete_rejected", .fn = test_read_only_delete_rejected},
        {.name = "concurrent_readers", .fn = test_concurrent_readers},
        {.name = "reopen_persistence", .fn = test_reopen_persistence},
        {.name = "read_only_semantics", .fn = test_read_only_semantics},
        {.name = "crc_not_checked_on_get", .fn = test_crc_not_checked_on_get},
        {.name = "crc_rejected_on_reopen", .fn = test_crc_rejected_on_reopen},
        {.name = "merge_compacts_inactive_files", .fn = test_merge_compacts_inactive_files},
        {.name = "merge_hint_values_consistent", .fn = test_merge_hint_values_consistent},
        {.name = "merge_rejected_read_only", .fn = test_merge_rejected_read_only},
        {.name = "merge_rejected_without_inactive", .fn = test_merge_rejected_without_inactive},
    };

    size_t passed = 0;
    size_t total = sizeof(tests) / sizeof(tests[0]);

    for (size_t i = 0; i < total; i++)
    {
        bool ok = tests[i].fn();
        printf("[%s] %s\n", ok ? "PASS" : "FAIL", tests[i].name);
        if (ok)
        {
            passed++;
        }
    }

    if (!cleanup_test_dirs())
    {
        return 1;
    }

    printf("summary: %zu/%zu passed\n", passed, total);
    return passed == total ? 0 : 1;
}
