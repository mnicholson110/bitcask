# bitcask

A minimal [Bitcask](https://riak.com/assets/bitcask-intro.pdf) key-value store in C. No external dependencies.

Behavior:
- Append-only writes
- O(1)-style reads via an in-memory hash table (keydir)
- CRC32 integrity checks
- Automatic file rotation at 1GiB
- Hintfile generation on merge for fast startup

## API

```c
bitcask_handle_t db;

bitcask_open(&db, "my_db", BITCASK_READ_WRITE);

bitcask_put(&db, key, key_len, val, val_len);
bitcask_get(&db, key, key_len, &out, &out_len);
bitcask_delete(&db, key, key_len);
bitcask_sync(&db);
bitcask_merge(&db);

bitcask_close(&db);
```

Open with `BITCASK_READ_ONLY` for read-only access, or `BITCASK_SYNC_ON_PUT` to call `fsync` after every write.

## On-disk format

Each entry is appended as:

```
| crc32 (4) | timestamp_ns (8) | key_size (4) | value_size (4) | key (key_size) | value (value_size) |
```

Hintfiles are created by `bitcask_merge` and are written as:

```
| timestamp_ns (8) | key_size (4) | value_size (4) | value_pos (4) |
```

## Build

```
make test       # build test suite (AI-generated, for now)
make bench      # build benchmarks
make bench_O3   # build benchmarks with -O3 compiler optimization flag 
make clean
```

## TODO

- [X] Compaction / merge — reclaim space from dead keys and old versions
    - [X] Clean up empty merge files
- [X] Hint files - generate hint files on merge for faster startup
    - [X] Clean up the disaster in bitcask.c from implementing this 
- [ ] Merge flags - flags for automatic merge behavior
- [ ] Fold — iterate over all live key-value pairs
- [ ] List Keys — list all live keys in the DB
- [ ] On-disk single-writer lockfile — prevent concurrent read-write opens
