/*!
 * leveldb_wrapper.c - wrap leveldb to look like rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <leveldb/c.h>

/*
 * Macros
 */

#if defined(_WIN32)
#  define RDB_EXTERN __declspec(dllexport)
#elif defined(__GNUC__) && __GNUC__ >= 4
#  define RDB_EXTERN __attribute__((visibility("default")))
#else
#  define RDB_EXTERN
#endif

#define lengthof(x) (sizeof(x) / sizeof((x)[0]))

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

typedef struct rdb_slice_s {
  void *data;
  size_t size;
  size_t dummy;
} rdb_slice_t;

typedef struct rdb_range_s {
  rdb_slice_t start;
  rdb_slice_t limit;
} rdb_range_t;

typedef struct rdb_s rdb_t;
typedef struct rdb_batch_s rdb_batch_t;
typedef leveldb_filterpolicy_t rdb_bloom_t;
typedef struct rdb_comparator_s rdb_comparator_t;
typedef struct rdb_dbopt_s rdb_dbopt_t;
typedef struct rdb_handler_s rdb_handler_t;
typedef struct rdb_itertbl_s rdb_itertbl_t;
typedef struct rdb_iter_s rdb_iter_t;
typedef struct rdb_logger_s rdb_logger_t;
typedef leveldb_cache_t rdb_lru_t;
typedef struct rdb_readopt_s rdb_readopt_t;
typedef leveldb_snapshot_t rdb_snapshot_t;
typedef struct rdb_writeopt_s rdb_writeopt_t;

struct rdb_s {
  leveldb_comparator_t *cmp;
  leveldb_options_t *options;
  leveldb_readoptions_t *read_options;
  leveldb_writeoptions_t *write_options;
  leveldb_readoptions_t *iter_options;
  leveldb_t *level;
};

struct rdb_batch_s {
  struct {
    leveldb_writebatch_t *rep;
    size_t dummy1;
    size_t dummy2;
  } props;
};

struct rdb_comparator_s {
  const char *name;
  int (*compare)(const rdb_comparator_t *,
                 const rdb_slice_t *,
                 const rdb_slice_t *);
  void (*dummy1)(void);
  void (*dummy2)(void);
  void *dummy3;
};

struct rdb_dbopt_s {
  rdb_comparator_t *comparator;
  int create_if_missing;
  int error_if_exists;
  int paranoid_checks;
  rdb_logger_t *info_log;
  size_t write_buffer_size;
  int max_open_files;
  rdb_lru_t *block_cache;
  size_t block_size;
  int block_restart_interval;
  size_t max_file_size;
  enum rdb_compression compression;
  int reuse_logs;
  rdb_bloom_t *filter_policy;
  int use_mmap;
};

struct rdb_handler_s {
  void *state;
  uint64_t number;

  void (*put)(rdb_handler_t *handler,
              const rdb_slice_t *key,
              const rdb_slice_t *value);

  void (*del)(rdb_handler_t *handler,
              const rdb_slice_t *key);
};

struct rdb_itertbl_s {
  void (*clear)(leveldb_iterator_t *iter);
  int (*valid)(const leveldb_iterator_t *iter);
  void (*seek_first)(leveldb_iterator_t *iter);
  void (*seek_last)(leveldb_iterator_t *iter);
  void (*seek)(leveldb_iterator_t *iter, const rdb_slice_t *target);
  void (*next)(leveldb_iterator_t *iter);
  void (*prev)(leveldb_iterator_t *iter);
  rdb_slice_t (*key)(const leveldb_iterator_t *iter);
  rdb_slice_t (*value)(const leveldb_iterator_t *iter);
  int (*status)(const leveldb_iterator_t *iter);
};

struct rdb_iter_s {
  leveldb_iterator_t *rep;
  struct {
    void (*dummy1)(void);
    leveldb_readoptions_t *options;
    void *dummy2;
    void *dummy3;
  } props;
  const rdb_itertbl_t *table;
};

struct rdb_readopt_s {
  int verify_checksums;
  int fill_cache;
  const rdb_snapshot_t *snapshot;
};

struct rdb_writeopt_s {
  int sync;
};

/*
 * Globals
 */

RDB_EXTERN extern const rdb_bloom_t *rdb_bloom_default;
RDB_EXTERN extern const rdb_comparator_t *rdb_bytewise_comparator;
RDB_EXTERN extern const rdb_dbopt_t *rdb_dbopt_default;
RDB_EXTERN extern const rdb_readopt_t *rdb_readopt_default;
RDB_EXTERN extern const rdb_writeopt_t *rdb_writeopt_default;
RDB_EXTERN extern const rdb_readopt_t *rdb_iteropt_default;

/*
 * Helpers
 */

static void *
safe_malloc(size_t size) {
  void *ptr = malloc(size);

  if (ptr == NULL)
    abort(); /* LCOV_EXCL_LINE */

  return ptr;
}

static void
safe_free(void *ptr) {
  if (ptr == NULL) {
    abort(); /* LCOV_EXCL_LINE */
    return;
  }

  free(ptr);
}

static int
convert_error(char *err) {
  /* https://github.com/google/leveldb/blob/f57513a/include/leveldb/status.h */
  /* https://github.com/google/leveldb/blob/f57513a/util/status.cc#L38 */
  char *p;

  if (err == NULL)
    return RDB_OK;

  p = strchr(err, ':');

  if (p != NULL)
    *p = '\0';

  if (strcmp(err, "OK") == 0)
    return RDB_OK;

  if (strcmp(err, "NotFound") == 0)
    return RDB_NOTFOUND;

  if (strcmp(err, "Corruption") == 0)
    return RDB_CORRUPTION;

  if (strcmp(err, "Not implemented") == 0)
    return RDB_NOSUPPORT;

  if (strcmp(err, "Invalid argument") == 0)
    return RDB_INVALID;

  if (strcmp(err, "IO error") == 0)
    return RDB_IOERR;

  return RDB_INVALID;
}

static int
handle_error(char *err) {
  int rc = convert_error(err);

  if (err != NULL)
    safe_free(err);

  return rc;
}

static void
comparator_destructor(void *state) {
  (void)state;
}

static int
comparator_compare(void *state, const char *a, size_t alen,
                                const char *b, size_t blen) {
  const rdb_comparator_t *cmp = state;
  rdb_slice_t x, y;

  x.data = (void *)a;
  x.size = alen;

  y.data = (void *)b;
  y.size = blen;

  return cmp->compare(cmp, &x, &y);
}

const char *
comparator_name(void *state) {
  const rdb_comparator_t *cmp = state;
  return cmp->name;
}

static leveldb_comparator_t *
convert_comparator(rdb_comparator_t *x) {
  if (x == NULL)
    return NULL;

  return leveldb_comparator_create(x, comparator_destructor,
                                      comparator_compare,
                                      comparator_name);
}

static leveldb_options_t *
convert_dbopt(const rdb_dbopt_t *x, leveldb_comparator_t *cmp) {
  leveldb_options_t *z = leveldb_options_create();

  if (cmp != NULL)
    leveldb_options_set_comparator(z, cmp);

  leveldb_options_set_create_if_missing(z, x->create_if_missing);
  leveldb_options_set_error_if_exists(z, x->error_if_exists);
  leveldb_options_set_paranoid_checks(z, x->paranoid_checks);
  leveldb_options_set_write_buffer_size(z, x->write_buffer_size);
  leveldb_options_set_max_open_files(z, x->max_open_files);

  if (x->block_cache != NULL)
    leveldb_options_set_cache(z, x->block_cache);

  leveldb_options_set_block_size(z, x->block_size);
  leveldb_options_set_block_restart_interval(z, x->block_restart_interval);
  leveldb_options_set_max_file_size(z, x->max_file_size);
  leveldb_options_set_compression(z, x->compression);

  if (x->filter_policy != NULL)
    leveldb_options_set_filter_policy(z, x->filter_policy);

  return z;
}

static leveldb_readoptions_t *
convert_readopt(const rdb_readopt_t *x) {
  leveldb_readoptions_t *z = leveldb_readoptions_create();

  leveldb_readoptions_set_verify_checksums(z, x->verify_checksums);
  leveldb_readoptions_set_fill_cache(z, x->fill_cache);
  leveldb_readoptions_set_snapshot(z, x->snapshot);

  return z;
}

static leveldb_writeoptions_t *
convert_writeopt(const rdb_writeopt_t *x) {
  leveldb_writeoptions_t *z = leveldb_writeoptions_create();

  leveldb_writeoptions_set_sync(z, x->sync);

  return z;
}

/*
 * Batch
 */

RDB_EXTERN rdb_batch_t *
rdb_batch_create(void) {
  rdb_batch_t *batch = safe_malloc(sizeof(rdb_batch_t));
  batch->props.rep = leveldb_writebatch_create();
  return batch;
}

RDB_EXTERN void
rdb_batch_destroy(rdb_batch_t *batch) {
  leveldb_writebatch_destroy(batch->props.rep);
  safe_free(batch);
}

RDB_EXTERN void
rdb_batch_init(rdb_batch_t *batch) {
  batch->props.rep = leveldb_writebatch_create();
}

RDB_EXTERN void
rdb_batch_clear(rdb_batch_t *batch) {
  leveldb_writebatch_destroy(batch->props.rep);
}

RDB_EXTERN void
rdb_batch_reset(rdb_batch_t *batch) {
  leveldb_writebatch_destroy(batch->props.rep);

  batch->props.rep = leveldb_writebatch_create();
}

static void
size_put(void *state, const char *k, size_t klen,
                      const char *v, size_t vlen) {
  (void)k;
  (void)v;

  *((size_t *)state) += klen;
  *((size_t *)state) += vlen;
}

static void
size_del(void *state, const char *k, size_t klen) {
  (void)k;

  *((size_t *)state) += klen;
}

RDB_EXTERN size_t
rdb_batch_approximate_size(const rdb_batch_t *batch) {
  size_t result = 0;
  leveldb_writebatch_iterate(batch->props.rep, &result, size_put, size_del);
  return result;
}

RDB_EXTERN void
rdb_batch_put(rdb_batch_t *batch,
              const rdb_slice_t *key,
              const rdb_slice_t *value) {
  leveldb_writebatch_put(batch->props.rep, key->data, key->size,
                                           value->data, value->size);
}

RDB_EXTERN void
rdb_batch_del(rdb_batch_t *batch, const rdb_slice_t *key) {
  leveldb_writebatch_delete(batch->props.rep, key->data, key->size);
}

static void
batch_put(void *state, const char *k, size_t klen,
                       const char *v, size_t vlen) {
  rdb_handler_t *handler = state;
  rdb_slice_t key, val;

  key.data = (void *)k;
  key.size = klen;

  val.data = (void *)v;
  val.size = vlen;

  handler->put(handler, &key, &val);
}

static void
batch_del(void *state, const char *k, size_t klen) {
  rdb_handler_t *handler = state;
  rdb_slice_t key;

  key.data = (void *)k;
  key.size = klen;

  handler->del(handler, &key);
}

RDB_EXTERN int
rdb_batch_iterate(const rdb_batch_t *batch, rdb_handler_t *handler) {
  leveldb_writebatch_iterate(batch->props.rep, handler, batch_put, batch_del);
  return RDB_OK;
}

RDB_EXTERN void
rdb_batch_append(rdb_batch_t *dst, const rdb_batch_t *src) {
  leveldb_writebatch_append(dst->props.rep, src->props.rep);
}

/*
 * Bloom
 */

RDB_EXTERN rdb_bloom_t *
rdb_bloom_create(int bits_per_key) {
  return leveldb_filterpolicy_create_bloom(bits_per_key);
}

RDB_EXTERN void
rdb_bloom_destroy(rdb_bloom_t *bloom) {
  leveldb_filterpolicy_destroy(bloom);
}

const rdb_bloom_t *rdb_bloom_default = NULL;

/*
 * Cache
 */

RDB_EXTERN rdb_lru_t *
rdb_lru_create(size_t capacity) {
  return leveldb_cache_create_lru(capacity);
}

RDB_EXTERN void
rdb_lru_destroy(rdb_lru_t *lru) {
  leveldb_cache_destroy(lru);
}

/*
 * Comparator
 */

const rdb_comparator_t *rdb_bytewise_comparator = NULL;

/*
 * Database
 */

RDB_EXTERN void
rdb_close(rdb_t *db);

RDB_EXTERN int
rdb_open(const char *dbname, const rdb_dbopt_t *options, rdb_t **dbptr) {
  rdb_t *db = safe_malloc(sizeof(rdb_t));
  char *err = NULL;
  int rc;

  if (options == NULL)
    options = rdb_dbopt_default;

  db->cmp = convert_comparator(options->comparator);
  db->options = convert_dbopt(options, db->cmp);
  db->read_options = convert_readopt(rdb_readopt_default);
  db->write_options = convert_writeopt(rdb_writeopt_default);
  db->iter_options = convert_readopt(rdb_iteropt_default);
  db->level = leveldb_open(db->options, dbname, &err);

  rc = handle_error(err);

  if (rc == RDB_OK) {
    *dbptr = db;
  } else {
    rdb_close(db);
    *dbptr = NULL;
  }

  return rc;
}

RDB_EXTERN void
rdb_close(rdb_t *db) {
  if (db->level != NULL)
    leveldb_close(db->level);

  if (db->cmp != NULL)
    leveldb_comparator_destroy(db->cmp);

  leveldb_options_destroy(db->options);
  leveldb_readoptions_destroy(db->read_options);
  leveldb_writeoptions_destroy(db->write_options);
  leveldb_readoptions_destroy(db->iter_options);

  safe_free(db);
}

RDB_EXTERN int
rdb_get(rdb_t *db, const rdb_slice_t *key,
                   rdb_slice_t *value,
                   const rdb_readopt_t *options) {
  leveldb_readoptions_t *opt = db->read_options;
  int rc = RDB_OK;
  char *err = NULL;
  char *vp = NULL;
  size_t vn = 0;

  if (options != NULL)
    opt = convert_readopt(options);

  vp = leveldb_get(db->level, opt, key->data, key->size, &vn, &err);

  if (err != NULL) {
    rc = handle_error(err);
    goto done;
  }

  if (vp == NULL)
    rc = RDB_NOTFOUND;

  value->data = (void *)vp;
  value->size = vn;
  value->dummy = 0;

done:
  if (options != NULL)
    leveldb_readoptions_destroy(opt);

  return rc;
}

RDB_EXTERN int
rdb_has(rdb_t *db, const rdb_slice_t *key, const rdb_readopt_t *options) {
  rdb_slice_t val;
  int rc;

  rc = rdb_get(db, key, &val, options);

  if (rc == RDB_OK)
    leveldb_free(val.data);

  return rc;
}

RDB_EXTERN int
rdb_put(rdb_t *db, const rdb_slice_t *key,
                   const rdb_slice_t *value,
                   const rdb_writeopt_t *options) {
  leveldb_writeoptions_t *opt = db->write_options;
  char *err = NULL;

  if (options != NULL)
    opt = convert_writeopt(options);

  leveldb_put(db->level,
              opt,
              key->data,
              key->size,
              value->data,
              value->size,
              &err);

  if (options != NULL)
    leveldb_writeoptions_destroy(opt);

  return handle_error(err);
}

RDB_EXTERN int
rdb_del(rdb_t *db, const rdb_slice_t *key, const rdb_writeopt_t *options) {
  leveldb_writeoptions_t *opt = db->write_options;
  char *err = NULL;

  if (options != NULL)
    opt = convert_writeopt(options);

  leveldb_delete(db->level, opt, key->data, key->size, &err);

  if (options != NULL)
    leveldb_writeoptions_destroy(opt);

  return handle_error(err);
}

RDB_EXTERN int
rdb_write(rdb_t *db, rdb_batch_t *updates, const rdb_writeopt_t *options) {
  leveldb_writeoptions_t *opt = db->write_options;
  char *err = NULL;

  if (options != NULL)
    opt = convert_writeopt(options);

  leveldb_write(db->level, opt, updates->props.rep, &err);

  if (options != NULL)
    leveldb_writeoptions_destroy(opt);

  return handle_error(err);
}

RDB_EXTERN const rdb_snapshot_t *
rdb_get_snapshot(rdb_t *db) {
  return leveldb_create_snapshot(db->level);
}

RDB_EXTERN void
rdb_release_snapshot(rdb_t *db, const rdb_snapshot_t *snapshot) {
  leveldb_release_snapshot(db->level, snapshot);
}

RDB_EXTERN int
rdb_get_property(rdb_t *db, const char *property, char **value) {
  *value = leveldb_property_value(db->level, property);
  return *value != NULL;
}

RDB_EXTERN void
rdb_get_approximate_sizes(rdb_t *db, const rdb_range_t *range,
                                     size_t length,
                                     uint64_t *sizes) {
  const char **start_keys = safe_malloc(length * sizeof(char *));
  const char **limit_keys = safe_malloc(length * sizeof(char *));
  size_t *start_lens = safe_malloc(length * sizeof(size_t));
  size_t *limit_lens = safe_malloc(length * sizeof(size_t));
  size_t i;

  for (i = 0; i < length; i++) {
    start_keys[i] = range[i].start.data;
    start_lens[i] = range[i].start.size;
    limit_keys[i] = range[i].limit.data;
    limit_lens[i] = range[i].limit.size;
  }

  leveldb_approximate_sizes(db->level, length,
                            start_keys, start_lens,
                            limit_keys, limit_lens,
                            sizes);

  safe_free(start_keys);
  safe_free(start_lens);
  safe_free(limit_keys);
  safe_free(limit_lens);
}

RDB_EXTERN void
rdb_compact_range(rdb_t *db, const rdb_slice_t *begin, const rdb_slice_t *end) {
  static const rdb_slice_t empty = {NULL, 0, 0};

  if (begin == NULL)
    begin = &empty;

  if (end == NULL)
    end = &empty;

  leveldb_compact_range(db->level, begin->data, begin->size,
                                   end->data, end->size);
}

RDB_EXTERN int
rdb_repair_db(const char *dbname, const rdb_dbopt_t *options) {
  leveldb_comparator_t *cmp;
  leveldb_options_t *opt;
  char *err = NULL;

  if (options == NULL)
    options = rdb_dbopt_default;

  cmp = convert_comparator(options->comparator);
  opt = convert_dbopt(options, cmp);

  leveldb_repair_db(opt, dbname, &err);

  if (cmp != NULL)
    leveldb_comparator_destroy(cmp);

  leveldb_options_destroy(opt);

  return handle_error(err);
}

RDB_EXTERN int
rdb_destroy_db(const char *dbname, const rdb_dbopt_t *options) {
  leveldb_comparator_t *cmp;
  leveldb_options_t *opt;
  char *err = NULL;

  if (options == NULL)
    options = rdb_dbopt_default;

  cmp = convert_comparator(options->comparator);
  opt = convert_dbopt(options, cmp);

  leveldb_destroy_db(opt, dbname, &err);

  if (cmp != NULL)
    leveldb_comparator_destroy(cmp);

  leveldb_options_destroy(opt);

  return handle_error(err);
}

/*
 * Filesystem
 */

RDB_EXTERN int
rdb_test_directory(char *result, size_t size) {
  leveldb_env_t *env = leveldb_create_default_env();
  char *path = leveldb_env_get_test_directory(env);
  int ret = 0;
  size_t len;

  if (path == NULL) {
    leveldb_env_destroy(env);
    return 0;
  }

  len = strlen(path);

  if (size >= len + 1) {
    memcpy(result, path, len + 1);
    ret = 1;
  }

  leveldb_env_destroy(env);
  safe_free(path);

  return ret;
}

/*
 * Internal
 */

RDB_EXTERN void
rdb_free(void *ptr) {
  leveldb_free(ptr);
}

/*
 * Iterator
 */

static int
iter_valid(const leveldb_iterator_t *iter) {
  return leveldb_iter_valid(iter);
}

static void
iter_seek_first(leveldb_iterator_t *iter) {
  leveldb_iter_seek_to_first(iter);
}

static void
iter_seek_last(leveldb_iterator_t *iter) {
  leveldb_iter_seek_to_last(iter);
}

static void
iter_seek(leveldb_iterator_t *iter, const rdb_slice_t *target) {
  leveldb_iter_seek(iter, target->data, target->size);
}

static void
iter_next(leveldb_iterator_t *iter) {
  leveldb_iter_next(iter);
}

static void
iter_prev(leveldb_iterator_t *iter) {
  leveldb_iter_prev(iter);
}

static rdb_slice_t
iter_key(const leveldb_iterator_t *iter) {
  rdb_slice_t key = {NULL, 0, 0};
  key.data = (void *)leveldb_iter_key(iter, &key.size);
  return key;
}

static rdb_slice_t
iter_value(const leveldb_iterator_t *iter) {
  rdb_slice_t value = {NULL, 0, 0};
  value.data = (void *)leveldb_iter_value(iter, &value.size);
  return value;
}

static int
iter_status(const leveldb_iterator_t *iter) {
  char *err = NULL;

  leveldb_iter_get_error(iter, &err);

  return handle_error(err);
}

static const rdb_itertbl_t iter_table = {
  /* .clear = */ NULL,
  /* .valid = */ iter_valid,
  /* .seek_first = */ iter_seek_first,
  /* .seek_last = */ iter_seek_last,
  /* .seek = */ iter_seek,
  /* .next = */ iter_next,
  /* .prev = */ iter_prev,
  /* .key = */ iter_key,
  /* .value = */ iter_value,
  /* .status = */ iter_status
};

RDB_EXTERN rdb_iter_t *
rdb_iterator(rdb_t *db, const rdb_readopt_t *options) {
  rdb_iter_t *iter = safe_malloc(sizeof(rdb_iter_t));
  leveldb_readoptions_t *opt = db->iter_options;

  if (options != NULL) {
    opt = convert_readopt(options);
    iter->props.options = opt;
  } else {
    iter->props.options = NULL;
  }

  iter->rep = leveldb_create_iterator(db->level, opt);
  iter->table = &iter_table;

  return iter;
}

RDB_EXTERN void
rdb_iter_destroy(rdb_iter_t *iter) {
  leveldb_iter_destroy(iter->rep);

  if (iter->props.options != NULL)
    leveldb_readoptions_destroy(iter->props.options);

  safe_free(iter);
}

/*
 * Options
 */

static const rdb_dbopt_t db_options = {
  /* .comparator = */ NULL,
  /* .create_if_missing = */ 0,
  /* .error_if_exists = */ 0,
  /* .paranoid_checks = */ 0,
  /* .info_log = */ NULL,
  /* .write_buffer_size = */ 4 * 1024 * 1024,
  /* .max_open_files = */ 1000,
  /* .block_cache = */ NULL,
  /* .block_size = */ 4 * 1024,
  /* .block_restart_interval = */ 16,
  /* .max_file_size = */ 2 * 1024 * 1024,
  /* .compression = */ RDB_NO_COMPRESSION,
  /* .reuse_logs = */ 0,
  /* .filter_policy = */ NULL,
  /* .use_mmap = */ 1
};

static const rdb_readopt_t read_options = {
  /* .verify_checksums = */ 0,
  /* .fill_cache = */ 1,
  /* .snapshot = */ NULL
};

static const rdb_writeopt_t write_options = {
  /* .sync = */ 0
};

static const rdb_readopt_t iter_options = {
  /* .verify_checksums = */ 0,
  /* .fill_cache = */ 0,
  /* .snapshot = */ NULL
};

const rdb_dbopt_t *rdb_dbopt_default = &db_options;
const rdb_readopt_t *rdb_readopt_default = &read_options;
const rdb_writeopt_t *rdb_writeopt_default = &write_options;
const rdb_readopt_t *rdb_iteropt_default = &iter_options;

/*
 * Slice
 */

RDB_EXTERN rdb_slice_t
rdb_slice(const void *xp, size_t xn) {
  rdb_slice_t ret;
  ret.data = (void *)xp;
  ret.size = xn;
  ret.dummy = 0;
  return ret;
}

RDB_EXTERN rdb_slice_t
rdb_string(const char *xp) {
  rdb_slice_t ret;
  ret.data = (void *)xp;
  ret.size = strlen(xp);
  ret.dummy = 0;
  return ret;
}

RDB_EXTERN int
rdb_slice_compare(const rdb_slice_t *x, const rdb_slice_t *y) {
  size_t n = x->size < y->size ? x->size : y->size;
  int r = n ? memcmp(x->data, y->data, n) : 0;

  if (r == 0) {
    if (x->size < y->size)
      r = -1;
    else if (x->size > y->size)
      r = +1;
  }

  return r;
}

/*
 * Status
 */

static const char *rdb_errmsg[] = {
  /* .RDB_OK = */ "OK",
  /* .RDB_NOTFOUND = */ "NotFound",
  /* .RDB_CORRUPTION = */ "Corruption",
  /* .RDB_NOSUPPORT = */ "Not implemented",
  /* .RDB_INVALID = */ "Invalid argument",
  /* .RDB_IOERR = */ "IO error"
};

RDB_EXTERN const char *
rdb_strerror(int code) {
  if (code < 0)
    code = -code;

  if (code >= (int)lengthof(rdb_errmsg))
    code = -RDB_INVALID;

  return rdb_errmsg[code];
}
