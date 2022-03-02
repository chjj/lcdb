/*!
 * rdb.h - database for c
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_H
#define RDB_H

#ifdef __cplusplus
extern "C" {
#endif

#include <limits.h>
#include <stddef.h>

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

typedef struct rdb_s rdb_t;
typedef struct rdb_batch_s rdb_batch_t;
typedef struct rdb_bloom_s rdb_bloom_t;
typedef struct rdb_comparator_s rdb_comparator_t;
typedef struct rdb_dbopt_s rdb_dbopt_t;
typedef struct rdb_iter_s rdb_iter_t;
typedef struct rdb_lru_s rdb_lru_t;
typedef struct rdb_readopt_s rdb_readopt_t;
typedef struct rdb_snapshot_s rdb_snapshot_t;
typedef struct rdb_writeopt_s rdb_writeopt_t;

typedef struct rdb_slice_s {
  void *data;
  size_t size;
  size_t _alloc;
} rdb_slice_t;

typedef struct rdb_range_s {
  rdb_slice_t start;
  rdb_slice_t limit;
} rdb_range_t;

#if defined(_WIN32)
typedef unsigned __int64 rdb_uint64_t;
#elif ULONG_MAX >> 31 >> 31 >> 1 == 1
typedef unsigned long rdb_uint64_t;
#else
#  ifdef __GNUC__
__extension__
#  endif
typedef unsigned long long rdb_uint64_t;
#endif

/*
 * Batch
 */

struct rdb_batch_s {
  rdb_slice_t _rep;
};

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

extern const rdb_bloom_t *rdb_bloom_default;

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

extern const rdb_comparator_t *rdb_bytewise_comparator;

/*
 * Database
 */

int
rdb_open(const char *dbname, const rdb_dbopt_t *options, rdb_t **dbptr);

void
rdb_close(rdb_t *db);

int
rdb_get(rdb_t *db, const rdb_slice_t *key,
                   rdb_slice_t *value,
                   const rdb_readopt_t *options);

int
rdb_has(rdb_t *db, const rdb_slice_t *key, const rdb_readopt_t *options);

int
rdb_put(rdb_t *db, const rdb_slice_t *key,
                   const rdb_slice_t *value,
                   const rdb_writeopt_t *options);

int
rdb_del(rdb_t *db, const rdb_slice_t *key, const rdb_writeopt_t *options);

int
rdb_write(rdb_t *db, rdb_batch_t *updates, const rdb_writeopt_t *options);

const rdb_snapshot_t *
rdb_get_snapshot(rdb_t *db);

void
rdb_release_snapshot(rdb_t *db, const rdb_snapshot_t *snapshot);

rdb_iter_t *
rdb_iterator(rdb_t *db, const rdb_readopt_t *options);

int
rdb_get_property(rdb_t *db, const char *property, char **value);

void
rdb_get_approximate_sizes(rdb_t *db, const rdb_range_t *range,
                                     size_t length,
                                     rdb_uint64_t *sizes);

void
rdb_compact_range(rdb_t *db, const rdb_slice_t *begin, const rdb_slice_t *end);

int
rdb_repair_db(const char *dbname, const rdb_dbopt_t *options);

int
rdb_destroy_db(const char *dbname, const rdb_dbopt_t *options);

/*
 * Internal
 */

void
rdb_free(void *ptr);

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
 * Slice
 */

rdb_slice_t
rdb_slice(const void *xp, size_t xn);

rdb_slice_t
rdb_string(const char *xp);

#define rdb_compare rdb_slice_compare

int
rdb_compare(const rdb_slice_t *x, const rdb_slice_t *y);

/*
 * Status
 */

const char *
rdb_strerror(int code);

#ifdef __cplusplus
}
#endif

#endif /* RDB_H */
