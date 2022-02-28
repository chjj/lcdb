/*!
 * rdb.h - database for c
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef BTC_RDB_H
#define BTC_RDB_H

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Constants
 */

#define RDB_OK (0)
#define RDB_NOTFOUND (-1)
#define RDB_CORRUPTION (-2)
#define RDB_NOSUPPORT (-3)
#define RDB_INVALID (-4)
#define RDB_IOERR (-5)

enum rdb_compression {
  RDB_NO_COMPRESSION = 0,
  RDB_SNAPPY_COMPRESSION = 1
};

/*
 * Types
 */

typedef struct rdb_slice_s rdb_batch_t;
typedef struct rdb_bloom_s rdb_bloom_t;
typedef struct rdb_comparator_s rdb_comparator_t;
typedef struct rdb_db_s rdb_db_t;
typedef struct rdb_dbopt_s rdb_dbopt_t;
typedef struct rdb_iter_s rdb_iter_t;
typedef struct rdb_lru_s rdb_lru_t;
typedef struct rdb_readopt_s rdb_readopt_t;
typedef struct rdb_snapshot_s rdb_snapshot_t;
typedef struct rdb_writeopt_s rdb_writeopt_t;

typedef struct rdb_slice_s {
  uint8_t *data;
  size_t size;
  size_t _alloc;
} rdb_slice_t;

typedef struct rdb_range_s {
  rdb_slice_t start;
  rdb_slice_t limit;
} rdb_range_t;

/*
 * Batch
 */

rdb_batch_t *
rdb_batch_create(void);

void
rdb_batch_destroy(rdb_batch_t *batch);

void
rdb_batch_init(rdb_batch_t *batch);

void
rdb_batch_clear(rdb_batch_t *batch);

void
rdb_batch_reset(rdb_batch_t *batch);

void
rdb_batch_put(rdb_batch_t *batch,
              const rdb_slice_t *key,
              const rdb_slice_t *value);

void
rdb_batch_del(rdb_batch_t *batch, const rdb_slice_t *key);

/*
 * Bloom
 */

rdb_bloom_t *
rdb_bloom_create(int bits_per_key);

void
rdb_bloom_destroy(rdb_bloom_t *bloom);

/*
 * Cache
 */

rdb_lru_t *
rdb_lru_create(size_t capacity);

void
rdb_lru_destroy(rdb_lru_t *lru);

/*
 * Comparator
 */

struct rdb_comparator_s {
  const char *name;
  int (*compare)(const rdb_comparator_t *,
                 const rdb_slice_t *,
                 const rdb_slice_t *);
  void (*shortest_separator)(const rdb_comparator_t *,
                             rdb_slice_t *,
                             const rdb_slice_t *);
  void (*short_successor)(const rdb_comparator_t *, rdb_slice_t *);
  const rdb_comparator_t *user_comparator;
};

/*
 * Database
 */

/*
 * Iterator
 */

struct rdb_iter_s {
  void *ptr;
  struct rdb_cleanup_s {
    void (*func)(void *, void *);
    void *arg1;
    void *arg2;
    struct rdb_cleanup_s *next;
  } cleanup_head;
  const struct rdb_itertbl_s {
    void (*clear)(void *iter);
    int (*valid)(const void *iter);
    void (*seek_first)(void *iter);
    void (*seek_last)(void *iter);
    void (*seek)(void *iter, const rdb_slice_t *target);
    void (*next)(void *iter);
    void (*prev)(void *iter);
    rdb_slice_t (*key)(const void *iter);
    rdb_slice_t (*value)(const void *iter);
    int (*status)(const void *iter);
  } *table;
};

#define rdb_iter_valid(x) (x)->table->valid((x)->ptr)
#define rdb_iter_seek_first(x) (x)->table->seek_first((x)->ptr)
#define rdb_iter_seek_last(x) (x)->table->seek_last((x)->ptr)
#define rdb_iter_seek(x, y) (x)->table->seek((x)->ptr, y)
#define rdb_iter_next(x) (x)->table->next((x)->ptr)
#define rdb_iter_prev(x) (x)->table->prev((x)->ptr)
#define rdb_iter_key(x) (x)->table->key((x)->ptr)
#define rdb_iter_value(x) (x)->table->value((x)->ptr)
#define rdb_iter_status(x) (x)->table->status((x)->ptr)

void
rdb_iter_destroy(rdb_iter_t *iter);

/*
 * Options
 */

struct rdb_dbopt_s {
  const rdb_comparator_t *comparator;
  int create_if_missing;
  int error_if_exists;
  int paranoid_checks;
  size_t write_buffer_size;
  int max_open_files;
  rdb_lru_t *block_cache;
  size_t block_size;
  int block_restart_interval;
  size_t max_file_size;
  enum rdb_compression compression;
  int reuse_logs;
  const rdb_bloom_t *filter_policy;
};

struct rdb_readopt_s {
  int verify_checksums;
  int fill_cache;
  const rdb_snapshot_t *snapshot;
};

struct rdb_writeopt_s {
  int sync;
};

extern const rdb_dbopt_t *rdb_dbopt_default;
extern const rdb_readopt_t *rdb_readopt_default;
extern const rdb_writeopt_t *rdb_writeopt_default;
extern const rdb_readopt_t *rdb_iteropt_default;

/*
 * Status
 */

const char *
rdb_strerror(int code);

#ifdef __cplusplus
}
#endif

#endif /* BTC_RDB_H */
