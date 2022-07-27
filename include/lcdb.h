/*!
 * lcdb.h - database for c
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on google/leveldb:
 *   Copyright (c) 2011, The LevelDB Authors. All rights reserved.
 *   https://github.com/google/leveldb
 *
 * See LICENSE for more information.
 */

#ifndef LCDB_H
#define LCDB_H

#include <limits.h>
#include <stdarg.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Compat
 */

#ifndef LCDB_C_H
#if defined(UINT64_MAX)
typedef uint64_t ldb_uint64_t;
#elif defined(_WIN32) && !defined(__GNUC__)
typedef unsigned __int64 ldb_uint64_t;
#elif ULONG_MAX >> 31 >> 31 >> 1 == 1
typedef unsigned long ldb_uint64_t;
#else
#  if defined(__GNUC__) && __GNUC__ >= 2
__extension__
#  endif
typedef unsigned long long ldb_uint64_t;
#endif
#endif

/*
 * Constants
 */

#define LDB_OK             0
#define LDB_NOTFOUND   30001
#define LDB_CORRUPTION 30002
#define LDB_NOSUPPORT  30003
#define LDB_INVALID    30004
#define LDB_IOERR      30005

enum ldb_compression {
  LDB_NO_COMPRESSION = 0,
  LDB_SNAPPY_COMPRESSION = 1
};

/*
 * Types
 */

typedef struct ldb_s ldb_t;
typedef struct ldb_batch_s ldb_batch_t;
typedef struct ldb_bloom_s ldb_bloom_t;
typedef struct ldb_comparator_s ldb_comparator_t;
typedef struct ldb_dbopt_s ldb_dbopt_t;
typedef struct ldb_handler_s ldb_handler_t;
typedef struct ldb_iter_s ldb_iter_t;
typedef struct ldb_logger_s ldb_logger_t;
typedef struct ldb_lru_s ldb_lru_t;
typedef struct ldb_range_s ldb_range_t;
typedef struct ldb_readopt_s ldb_readopt_t;
typedef struct ldb_slice_s ldb_slice_t;
typedef struct ldb_snapshot_s ldb_snapshot_t;
typedef struct ldb_writeopt_s ldb_writeopt_t;

struct ldb_slice_s {
  void *data;
  size_t size;
  size_t dummy;
};

struct ldb_batch_s {
  ldb_slice_t dummy;
};

struct ldb_comparator_s {
  const char *name;
  int (*compare)(const ldb_comparator_t *,
                 const ldb_slice_t *,
                 const ldb_slice_t *);
  void (*shortest_separator)(const ldb_comparator_t *,
                             ldb_slice_t *,
                             const ldb_slice_t *);
  void (*short_successor)(const ldb_comparator_t *, ldb_slice_t *);
  const ldb_comparator_t *user_comparator;
  void *state;
};

struct ldb_dbopt_s {
  const ldb_comparator_t *comparator;
  int create_if_missing;
  int error_if_exists;
  int paranoid_checks;
  ldb_logger_t *info_log;
  size_t write_buffer_size;
  int max_open_files;
  ldb_lru_t *block_cache;
  size_t block_size;
  int block_restart_interval;
  size_t max_file_size;
  enum ldb_compression compression;
  int reuse_logs;
  const ldb_bloom_t *filter_policy;
  int use_mmap;
};

struct ldb_handler_s {
  void *state;
  ldb_uint64_t number;

  void (*put)(ldb_handler_t *handler,
              const ldb_slice_t *key,
              const ldb_slice_t *value);

  void (*del)(ldb_handler_t *handler,
              const ldb_slice_t *key);
};

struct ldb_range_s {
  ldb_slice_t start;
  ldb_slice_t limit;
};

struct ldb_readopt_s {
  int verify_checksums;
  int fill_cache;
  const ldb_snapshot_t *snapshot;
};

struct ldb_writeopt_s {
  int sync;
};

/*
 * Globals
 */

#ifdef _WIN32
const ldb_bloom_t *ldb_bloom_import(void);
const ldb_comparator_t *ldb_comparator_import(void);
const ldb_dbopt_t *ldb_dbopt_import(void);
const ldb_readopt_t *ldb_readopt_import(void);
const ldb_writeopt_t *ldb_writeopt_import(void);
const ldb_readopt_t *ldb_iteropt_import(void);
#define ldb_bloom_default (ldb_bloom_import())
#define ldb_bytewise_comparator (ldb_comparator_import())
#define ldb_dbopt_default (ldb_dbopt_import())
#define ldb_readopt_default (ldb_readopt_import())
#define ldb_writeopt_default (ldb_writeopt_import())
#define ldb_iteropt_default (ldb_iteropt_import())
#else
extern const ldb_bloom_t *ldb_bloom_default;
extern const ldb_comparator_t *ldb_bytewise_comparator;
extern const ldb_dbopt_t *ldb_dbopt_default;
extern const ldb_readopt_t *ldb_readopt_default;
extern const ldb_writeopt_t *ldb_writeopt_default;
extern const ldb_readopt_t *ldb_iteropt_default;
#endif

/*
 * Batch
 */

ldb_batch_t *
ldb_batch_create(void);

void
ldb_batch_destroy(ldb_batch_t *batch);

void
ldb_batch_init(ldb_batch_t *batch);

void
ldb_batch_clear(ldb_batch_t *batch);

void
ldb_batch_reset(ldb_batch_t *batch);

void
ldb_batch_put(ldb_batch_t *batch,
              const ldb_slice_t *key,
              const ldb_slice_t *value);

void
ldb_batch_del(ldb_batch_t *batch, const ldb_slice_t *key);

int
ldb_batch_iterate(const ldb_batch_t *batch, ldb_handler_t *handler);

void
ldb_batch_append(ldb_batch_t *dst, const ldb_batch_t *src);

/*
 * Bloom
 */

ldb_bloom_t *
ldb_bloom_create(int bits_per_key);

void
ldb_bloom_destroy(ldb_bloom_t *bloom);

/*
 * Cache
 */

ldb_lru_t *
ldb_lru_create(size_t capacity);

void
ldb_lru_destroy(ldb_lru_t *lru);

/*
 * Comparator
 */

#define ldb_comparator_init(cmp, name_, compare_, state_) do { \
  (cmp)->name = (name_);                                       \
  (cmp)->compare = (compare_);                                 \
  (cmp)->shortest_separator = NULL;                            \
  (cmp)->short_successor = NULL;                               \
  (cmp)->user_comparator = NULL;                               \
  (cmp)->state = (void *)(state_);                             \
} while (0)

/**
 * Static initialization of a comparator.
 *
 * Example:
 *
 *   static int
 *   compare(const ldb_comparator_t *cmp,
 *           const ldb_slice_t *x,
 *           const ldb_slice_t *y) {
 *     ...
 *   }
 *
 *   static const ldb_comparator_t comparator =
 *     ldb_comparator("leveldb.MyComparator", compare, NULL);
 */
#define ldb_comparator(name, compare, state) \
  { (name), (compare), NULL, NULL, NULL, (state) }

/*
 * Database
 */

int
ldb_open(const char *dbname, const ldb_dbopt_t *options, ldb_t **dbptr);

void
ldb_close(ldb_t *db);

int
ldb_get(ldb_t *db, const ldb_slice_t *key,
                   ldb_slice_t *value,
                   const ldb_readopt_t *options);

int
ldb_has(ldb_t *db, const ldb_slice_t *key, const ldb_readopt_t *options);

int
ldb_put(ldb_t *db, const ldb_slice_t *key,
                   const ldb_slice_t *value,
                   const ldb_writeopt_t *options);

int
ldb_del(ldb_t *db, const ldb_slice_t *key, const ldb_writeopt_t *options);

int
ldb_write(ldb_t *db, ldb_batch_t *updates, const ldb_writeopt_t *options);

const ldb_snapshot_t *
ldb_snapshot(ldb_t *db);

void
ldb_release(ldb_t *db, const ldb_snapshot_t *snapshot);

int
ldb_property(ldb_t *db, const char *property, char **value);

void
ldb_approximate_sizes(ldb_t *db, const ldb_range_t *range,
                                 size_t length,
                                 ldb_uint64_t *sizes);

void
ldb_compact(ldb_t *db, const ldb_slice_t *begin, const ldb_slice_t *end);

int
ldb_backup(ldb_t *db, const char *name);

int
ldb_compare(const ldb_t *db, const ldb_slice_t *x, const ldb_slice_t *y);

/*
 * Static
 */

int
ldb_repair(const char *dbname, const ldb_dbopt_t *options);

int
ldb_copy(const char *from, const char *to, const ldb_dbopt_t *options);

int
ldb_destroy(const char *dbname, const ldb_dbopt_t *options);

/*
 * Filesystem
 */

int
ldb_test_directory(char *result, size_t size);

int
ldb_test_filename(char *result, size_t size, const char *name);

/*
 * Internal
 */

void
ldb_free(void *ptr);

/*
 * Iterator
 */

ldb_iter_t *
ldb_iterator(ldb_t *db, const ldb_readopt_t *options);

void
ldb_iter_destroy(ldb_iter_t *iter);

int
ldb_iter_valid(const ldb_iter_t *iter);

void
ldb_iter_first(ldb_iter_t *iter);

void
ldb_iter_last(ldb_iter_t *iter);

void
ldb_iter_seek(ldb_iter_t *iter, const ldb_slice_t *target);

void
ldb_iter_next(ldb_iter_t *iter);

void
ldb_iter_prev(ldb_iter_t *iter);

ldb_slice_t
ldb_iter_key(const ldb_iter_t *iter);

ldb_slice_t
ldb_iter_value(const ldb_iter_t *iter);

int
ldb_iter_status(const ldb_iter_t *iter);

int
ldb_iter_compare(const ldb_iter_t *iter, const ldb_slice_t *key);

void
ldb_iter_seek_ge(ldb_iter_t *iter, const ldb_slice_t *target);

void
ldb_iter_seek_gt(ldb_iter_t *iter, const ldb_slice_t *target);

void
ldb_iter_seek_le(ldb_iter_t *iter, const ldb_slice_t *target);

void
ldb_iter_seek_lt(ldb_iter_t *iter, const ldb_slice_t *target);

/**
 * Alias for `ldb_iter_value`.
 */
#define ldb_iter_val ldb_iter_value

/**
 * Iterate over each key.
 *
 * Example:
 *
 *   ldb_iter_t *it = ldb_iterator(db, 0);
 *
 *   ldb_iter_each(it) {
 *     ldb_slice_t key = ldb_iter_key(it);
 *     ldb_slice_t val = ldb_iter_val(it);
 *     ...
 *   }
 */
#define ldb_iter_each(it)  \
  for (ldb_iter_first(it); \
       ldb_iter_valid(it); \
       ldb_iter_next(it))

/**
 * Iterate from start to end (both inclusive).
 *
 * Example:
 *
 *   ldb_slice_t start = ldb_string("a");
 *   ldb_slice_t end = ldb_string("z");
 *   ldb_iter_t *it = ldb_iterator(db, 0);
 *
 *   ldb_iter_range(it, &start, &end) {
 *     ldb_slice_t key = ldb_iter_key(it);
 *     ldb_slice_t val = ldb_iter_val(it);
 *     ...
 *   }
 */
#define ldb_iter_range(it, min, max)                         \
  for (ldb_iter_seek(it, min);                               \
       ldb_iter_valid(it) && ldb_iter_compare(it, max) <= 0; \
       ldb_iter_next(it))

/**
 * Iterate over each key (backwards).
 *
 * Example:
 *
 *   ldb_iter_t *it = ldb_iterator(db, 0);
 *
 *   ldb_iter_backwards(it) {
 *     ldb_slice_t key = ldb_iter_key(it);
 *     ldb_slice_t val = ldb_iter_val(it);
 *     ...
 *   }
 */
#define ldb_iter_backwards(it) \
  for (ldb_iter_last(it);      \
       ldb_iter_valid(it);     \
       ldb_iter_prev(it))

/**
 * Iterate from start to end (both inclusive).
 *
 * Example:
 *
 *   ldb_slice_t start = ldb_string("z");
 *   ldb_slice_t end = ldb_string("a");
 *   ldb_iter_t *it = ldb_iterator(db, 0);
 *
 *   ldb_iter_reverse(it, &start, &end) {
 *     ldb_slice_t key = ldb_iter_key(it);
 *     ldb_slice_t val = ldb_iter_val(it);
 *     ...
 *   }
 */
#define ldb_iter_reverse(it, max, min)                       \
  for (ldb_iter_seek_le(it, max);                            \
       ldb_iter_valid(it) && ldb_iter_compare(it, min) >= 0; \
       ldb_iter_prev(it))

/*
 * Logging
 */

ldb_logger_t *
ldb_logger_create(void (*logv)(void *, const char *, va_list), void *state);

void
ldb_logger_destroy(ldb_logger_t *logger);

/*
 * Slice
 */

ldb_slice_t
ldb_slice(const void *xp, size_t xn);

ldb_slice_t
ldb_string(const char *xp);

#if (defined(__GNUC__) && __GNUC__ >= 3) && !defined(__PCC__) \
                                         && !defined(__NWCC__)
#define ldb_slice(xp, xn) __extension__ ({ \
  ldb_slice_t _z;                          \
                                           \
  _z.data = (void *)(xp);                  \
  _z.size = (xn);                          \
  _z.dummy = 0;                            \
                                           \
  _z;                                      \
})
#endif

/*
 * Status
 */

const char *
ldb_strerror(int code);

#ifdef __cplusplus
}
#endif

#endif /* LCDB_H */
