#include "../include/bitcask.h"

#include <dirent.h>
#include <inttypes.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct bench_config
{
    const char *seq_dir;
    const char *mixed_dir;
    const char *rotate_dir;
    size_t writes;
    size_t reads;
    size_t mixed_ops;
    size_t keyspace;
    size_t value_size;
    uint64_t seed;
    bool keep_data;
    bool quick_rotate;
} bench_config_t;

static double elapsed_seconds(const struct timespec *start, const struct timespec *end)
{
    time_t sec = end->tv_sec - start->tv_sec;
    long nsec = end->tv_nsec - start->tv_nsec;
    return (double)sec + (double)nsec / 1000000000.0;
}

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

static uint64_t next_u64(uint64_t *state)
{
    uint64_t x = *state;
    x ^= x >> 12;
    x ^= x << 25;
    x ^= x >> 27;
    *state = x;
    return x * 2685821657736338717ULL;
}

static void encode_key_u64(uint8_t out[8], uint64_t key_id)
{
    out[0] = (uint8_t)(key_id);
    out[1] = (uint8_t)(key_id >> 8);
    out[2] = (uint8_t)(key_id >> 16);
    out[3] = (uint8_t)(key_id >> 24);
    out[4] = (uint8_t)(key_id >> 32);
    out[5] = (uint8_t)(key_id >> 40);
    out[6] = (uint8_t)(key_id >> 48);
    out[7] = (uint8_t)(key_id >> 56);
}

static void fill_value(uint8_t *value, size_t value_size, uint64_t key_id, uint64_t version)
{
    for (size_t i = 0; i < value_size; i++)
    {
        value[i] = (uint8_t)((key_id + version + i) & 0xFFu);
    }
}

static bool verify_value_edges(const uint8_t *value, size_t value_size, uint64_t key_id, uint64_t version)
{
    if (value_size == 0)
    {
        return true;
    }

    uint8_t first = (uint8_t)((key_id + version) & 0xFFu);
    uint8_t last = (uint8_t)((key_id + version + value_size - 1) & 0xFFu);
    return value[0] == first && value[value_size - 1] == last;
}

static bool parse_size_arg(const char *arg, size_t *out)
{
    if (arg == NULL || *arg == '\0')
    {
        return false;
    }
    char *end = NULL;
    unsigned long long parsed = strtoull(arg, &end, 10);
    if (end == arg || *end != '\0')
    {
        return false;
    }
    *out = (size_t)parsed;
    return true;
}

static bool parse_u64_arg(const char *arg, uint64_t *out)
{
    if (arg == NULL || *arg == '\0')
    {
        return false;
    }
    char *end = NULL;
    uint64_t parsed = strtoull(arg, &end, 10);
    if (end == arg || *end != '\0')
    {
        return false;
    }
    *out = parsed;
    return true;
}

static void print_usage(const char *argv0)
{
    printf("usage: %s [--quick] [--quick-rotate] [--keep-data] [--writes N] [--reads N] [--mixed N] [--keyspace N] [--value-size N] [--seed N]\n", argv0);
}

static bool parse_args(int argc, char **argv, bench_config_t *cfg)
{
    for (int i = 1; i < argc; i++)
    {
        if (strcmp(argv[i], "--quick") == 0)
        {
            cfg->writes = 300000;
            cfg->reads = 300000;
            cfg->mixed_ops = 600000;
            cfg->keyspace = 100000;
            continue;
        }
        if (strcmp(argv[i], "--quick-rotate") == 0)
        {
            cfg->quick_rotate = true;
            continue;
        }
        if (strcmp(argv[i], "--keep-data") == 0)
        {
            cfg->keep_data = true;
            continue;
        }
        if (strcmp(argv[i], "--writes") == 0 && i + 1 < argc)
        {
            if (!parse_size_arg(argv[++i], &cfg->writes))
            {
                return false;
            }
            continue;
        }
        if (strcmp(argv[i], "--reads") == 0 && i + 1 < argc)
        {
            if (!parse_size_arg(argv[++i], &cfg->reads))
            {
                return false;
            }
            continue;
        }
        if (strcmp(argv[i], "--mixed") == 0 && i + 1 < argc)
        {
            if (!parse_size_arg(argv[++i], &cfg->mixed_ops))
            {
                return false;
            }
            continue;
        }
        if (strcmp(argv[i], "--keyspace") == 0 && i + 1 < argc)
        {
            if (!parse_size_arg(argv[++i], &cfg->keyspace))
            {
                return false;
            }
            continue;
        }
        if (strcmp(argv[i], "--value-size") == 0 && i + 1 < argc)
        {
            if (!parse_size_arg(argv[++i], &cfg->value_size))
            {
                return false;
            }
            continue;
        }
        if (strcmp(argv[i], "--seed") == 0 && i + 1 < argc)
        {
            if (!parse_u64_arg(argv[++i], &cfg->seed))
            {
                return false;
            }
            continue;
        }
        return false;
    }
    return true;
}

static bool run_write_workload(const bench_config_t *cfg)
{
    if (!rm_rf(cfg->seq_dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, cfg->seq_dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    uint8_t key[8];
    uint8_t *value = malloc(cfg->value_size);
    if (value == NULL)
    {
        bitcask_close(&db);
        return false;
    }

    struct timespec t0;
    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (size_t i = 0; i < cfg->writes; i++)
    {
        encode_key_u64(key, (uint64_t)i);
        fill_value(value, cfg->value_size, (uint64_t)i, 1);
        if (!bitcask_put(&db, key, sizeof(key), value, cfg->value_size))
        {
            free(value);
            bitcask_close(&db);
            return false;
        }
    }

    if (!bitcask_sync(&db))
    {
        free(value);
        bitcask_close(&db);
        return false;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double sec = elapsed_seconds(&t0, &t1);
    double ops = (double)cfg->writes / sec;
    double ns_per_op = (sec * 1000000000.0) / (double)cfg->writes;
    double mib = ((double)cfg->writes * (double)cfg->value_size) / (1024.0 * 1024.0);

    printf("[write] ops=%zu value_size=%zuB time=%.3fs ops/s=%.0f ns/op=%.0f throughput=%.2f MiB/s\n",
           cfg->writes, cfg->value_size, sec, ops, ns_per_op, mib / sec);

    free(value);
    bitcask_close(&db);
    return true;
}

static bool run_read_workload(const bench_config_t *cfg)
{
    bitcask_handle_t db;
    if (!bitcask_open(&db, cfg->seq_dir, BITCASK_READ_ONLY))
    {
        return false;
    }

    uint64_t rng = cfg->seed ^ 0x9e3779b97f4a7c15ULL;
    struct timespec t0;
    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (size_t i = 0; i < cfg->reads; i++)
    {
        uint64_t idx = next_u64(&rng) % cfg->writes;
        uint8_t key[8];
        encode_key_u64(key, idx);

        uint8_t *out = NULL;
        size_t out_size = 0;
        if (!bitcask_get(&db, key, sizeof(key), &out, &out_size))
        {
            free(out);
            bitcask_close(&db);
            return false;
        }
        if (out_size != cfg->value_size || !verify_value_edges(out, out_size, idx, 1))
        {
            free(out);
            bitcask_close(&db);
            return false;
        }
        free(out);
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double sec = elapsed_seconds(&t0, &t1);
    double ops = (double)cfg->reads / sec;
    double ns_per_op = (sec * 1000000000.0) / (double)cfg->reads;
    double mib = ((double)cfg->reads * (double)cfg->value_size) / (1024.0 * 1024.0);
    printf("[read]  ops=%zu value_size=%zuB time=%.3fs ops/s=%.0f ns/op=%.0f throughput=%.2f MiB/s\n",
           cfg->reads, cfg->value_size, sec, ops, ns_per_op, mib / sec);

    bitcask_close(&db);
    return true;
}

static bool run_mixed_workload(const bench_config_t *cfg)
{
    if (!rm_rf(cfg->mixed_dir))
    {
        return false;
    }

    bitcask_handle_t db;
    if (!bitcask_open(&db, cfg->mixed_dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    uint8_t *value = malloc(cfg->value_size);
    uint8_t *exists = calloc(cfg->keyspace, sizeof(uint8_t));
    uint32_t *version = calloc(cfg->keyspace, sizeof(uint32_t));
    if (value == NULL || exists == NULL || version == NULL)
    {
        free(value);
        free(exists);
        free(version);
        bitcask_close(&db);
        return false;
    }

    uint64_t rng = cfg->seed;
    size_t writes = 0;
    size_t reads = 0;
    size_t deletes = 0;

    struct timespec t0;
    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (size_t i = 0; i < cfg->mixed_ops; i++)
    {
        uint64_t k = next_u64(&rng) % cfg->keyspace;
        uint8_t key[8];
        encode_key_u64(key, k);

        uint64_t r = next_u64(&rng) % 100;
        if (r < 50)
        {
            version[k]++;
            fill_value(value, cfg->value_size, k, version[k]);
            if (!bitcask_put(&db, key, sizeof(key), value, cfg->value_size))
            {
                free(value);
                free(exists);
                free(version);
                bitcask_close(&db);
                return false;
            }
            exists[k] = 1;
            writes++;
        }
        else if (r < 90)
        {
            uint8_t *out = NULL;
            size_t out_size = 0;
            bool ok = bitcask_get(&db, key, sizeof(key), &out, &out_size);
            if (exists[k])
            {
                if (!ok || out_size != cfg->value_size || !verify_value_edges(out, out_size, k, version[k]))
                {
                    free(out);
                    free(value);
                    free(exists);
                    free(version);
                    bitcask_close(&db);
                    return false;
                }
            }
            else if (ok)
            {
                free(out);
                free(value);
                free(exists);
                free(version);
                bitcask_close(&db);
                return false;
            }
            free(out);
            reads++;
        }
        else
        {
            if (!bitcask_delete(&db, key, sizeof(key)))
            {
                free(value);
                free(exists);
                free(version);
                bitcask_close(&db);
                return false;
            }
            exists[k] = 0;
            deletes++;
        }
    }

    if (!bitcask_sync(&db))
    {
        free(value);
        free(exists);
        free(version);
        bitcask_close(&db);
        return false;
    }

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double sec = elapsed_seconds(&t0, &t1);
    double ops = (double)cfg->mixed_ops / sec;
    double ns_per_op = (sec * 1000000000.0) / (double)cfg->mixed_ops;
    double mib = ((double)(writes + reads) * (double)cfg->value_size) / (1024.0 * 1024.0);
    printf("[mixed] ops=%zu value_size=%zuB (writes=%zu reads=%zu deletes=%zu) time=%.3fs ops/s=%.0f ns/op=%.0f throughput=%.2f MiB/s\n",
           cfg->mixed_ops, cfg->value_size, writes, reads, deletes, sec, ops, ns_per_op, mib / sec);

    free(value);
    free(exists);
    free(version);
    bitcask_close(&db);
    return true;
}

static bool has_data_suffix(const char *name)
{
    size_t len = strlen(name);
    const char *suffix = ".data";
    size_t suffix_len = 5;
    if (len < suffix_len)
    {
        return false;
    }
    return strcmp(name + (len - suffix_len), suffix) == 0;
}

static size_t count_datafiles(const char *dir)
{
    DIR *dp = opendir(dir);
    if (dp == NULL)
    {
        return 0;
    }

    size_t count = 0;
    struct dirent *entry;
    while ((entry = readdir(dp)) != NULL)
    {
        if (has_data_suffix(entry->d_name))
        {
            count++;
        }
    }
    closedir(dp);
    return count;
}

static bool run_rotation_mixed_quick(const bench_config_t *cfg)
{
    if (!rm_rf(cfg->rotate_dir))
    {
        return false;
    }

    const size_t ops = 30000;
    const size_t keyspace = 4096;
    const size_t value_size = 65536;

    bitcask_handle_t db;
    if (!bitcask_open(&db, cfg->rotate_dir, BITCASK_READ_WRITE))
    {
        return false;
    }

    uint8_t *value = malloc(value_size);
    uint8_t *exists = calloc(keyspace, sizeof(uint8_t));
    uint32_t *version = calloc(keyspace, sizeof(uint32_t));
    if (value == NULL || exists == NULL || version == NULL)
    {
        free(value);
        free(exists);
        free(version);
        bitcask_close(&db);
        return false;
    }

    uint64_t rng = cfg->seed ^ 0xfeedface12345678ULL;
    size_t writes = 0;
    size_t reads = 0;
    size_t deletes = 0;

    struct timespec t0;
    struct timespec t1;
    clock_gettime(CLOCK_MONOTONIC, &t0);

    for (size_t i = 0; i < ops; i++)
    {
        uint64_t k = next_u64(&rng) % keyspace;
        uint8_t key[8];
        encode_key_u64(key, k);

        uint64_t r = next_u64(&rng) % 100;
        if (r < 65)
        {
            version[k]++;
            fill_value(value, value_size, k, version[k]);
            if (!bitcask_put(&db, key, sizeof(key), value, value_size))
            {
                free(value);
                free(exists);
                free(version);
                bitcask_close(&db);
                return false;
            }
            exists[k] = 1;
            writes++;
        }
        else if (r < 95)
        {
            uint8_t *out = NULL;
            size_t out_size = 0;
            bool ok = bitcask_get(&db, key, sizeof(key), &out, &out_size);
            if (exists[k])
            {
                if (!ok || out_size != value_size || !verify_value_edges(out, out_size, k, version[k]))
                {
                    free(out);
                    free(value);
                    free(exists);
                    free(version);
                    bitcask_close(&db);
                    return false;
                }
            }
            else if (ok)
            {
                free(out);
                free(value);
                free(exists);
                free(version);
                bitcask_close(&db);
                return false;
            }
            free(out);
            reads++;
        }
        else
        {
            if (!bitcask_delete(&db, key, sizeof(key)))
            {
                free(value);
                free(exists);
                free(version);
                bitcask_close(&db);
                return false;
            }
            exists[k] = 0;
            deletes++;
        }
    }

    if (!bitcask_sync(&db))
    {
        free(value);
        free(exists);
        free(version);
        bitcask_close(&db);
        return false;
    }
    bitcask_close(&db);

    size_t files = count_datafiles(cfg->rotate_dir);
    if (files < 3)
    {
        free(value);
        free(exists);
        free(version);
        return false;
    }

    if (!bitcask_open(&db, cfg->rotate_dir, BITCASK_READ_ONLY))
    {
        free(value);
        free(exists);
        free(version);
        return false;
    }

    for (size_t i = 0; i < 10000; i++)
    {
        uint64_t k = next_u64(&rng) % keyspace;
        uint8_t key[8];
        encode_key_u64(key, k);

        uint8_t *out = NULL;
        size_t out_size = 0;
        bool ok = bitcask_get(&db, key, sizeof(key), &out, &out_size);
        if (exists[k])
        {
            if (!ok || out_size != value_size || !verify_value_edges(out, out_size, k, version[k]))
            {
                free(out);
                free(value);
                free(exists);
                free(version);
                bitcask_close(&db);
                return false;
            }
        }
        else if (ok)
        {
            free(out);
            free(value);
            free(exists);
            free(version);
            bitcask_close(&db);
            return false;
        }
        free(out);
    }
    bitcask_close(&db);

    clock_gettime(CLOCK_MONOTONIC, &t1);
    double sec = elapsed_seconds(&t0, &t1);
    double throughput_mib = ((double)writes * (double)value_size) / (1024.0 * 1024.0);
    printf("[rotate-mixed] ops=%zu (writes=%zu reads=%zu deletes=%zu) files=%zu time=%.3fs ops/s=%.0f write-throughput=%.2f MiB/s\n",
           ops, writes, reads, deletes, files, sec, (double)ops / sec, throughput_mib / sec);

    free(value);
    free(exists);
    free(version);
    return true;
}

int main(int argc, char **argv)
{
    bench_config_t cfg = {
        .seq_dir = "test/bench-seq",
        .mixed_dir = "test/bench-mixed",
        .rotate_dir = "test/bench-rotate",
        .writes = 3000000,
        .reads = 3000000,
        .mixed_ops = 6000000,
        .keyspace = 500000,
        .value_size = 512,
        .seed = 0x1234c0deULL,
        .keep_data = false,
        .quick_rotate = false,
    };

    if (!parse_args(argc, argv, &cfg))
    {
        print_usage(argv[0]);
        return 1;
    }
    if (cfg.writes == 0 || cfg.reads == 0 || cfg.mixed_ops == 0 || cfg.keyspace == 0 || cfg.value_size == 0)
    {
        print_usage(argv[0]);
        return 1;
    }

    if (!run_write_workload(&cfg))
    {
        return 1;
    }
    if (!run_read_workload(&cfg))
    {
        return 1;
    }
    if (!run_mixed_workload(&cfg))
    {
        return 1;
    }
    if (cfg.quick_rotate && !run_rotation_mixed_quick(&cfg))
    {
        return 1;
    }

    if (!cfg.keep_data)
    {
        if (!rm_rf(cfg.seq_dir) || !rm_rf(cfg.mixed_dir))
        {
            return 1;
        }
        if (cfg.quick_rotate && !rm_rf(cfg.rotate_dir))
        {
            return 1;
        }
    }

    printf("benchmark complete\n");
    return 0;
}
