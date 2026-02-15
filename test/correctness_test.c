#include "../include/bitcask.h"

#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>

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
        "test/test-reopen",
        "test/test-readonly-existing",
        "test/test-readonly-missing",
        "test/test-crc-get",
        "test/test-crc-open",
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

static bool test_crc_rejected_on_get(void)
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
    free(out);
    bitcask_close(&db);
    return !ok;
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

int main(void)
{
    if (!cleanup_test_dirs())
    {
        return 1;
    }

    test_case_t tests[] = {
        {.name = "basic_put_get_delete", .fn = test_basic_put_get_delete},
        {.name = "size_limits", .fn = test_size_limits},
        {.name = "reopen_persistence", .fn = test_reopen_persistence},
        {.name = "read_only_semantics", .fn = test_read_only_semantics},
        {.name = "crc_rejected_on_get", .fn = test_crc_rejected_on_get},
        {.name = "crc_rejected_on_reopen", .fn = test_crc_rejected_on_reopen},
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
