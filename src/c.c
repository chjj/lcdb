/*!
 * c.c - wrapper for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "table/iterator.h"

#include "util/bloom.h"
#include "util/buffer.h"
#include "util/cache.h"
#include "util/comparator.h"
#include "util/env.h"
#include "util/extern.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/slice.h"
#include "util/status.h"

#include "db_impl.h"
#include "write_batch.h"

/* Must be included last (for stdint and RDB_EXTERN). */
#include <rdb_c.h>

/*
 * Macros
 */

#define container_of(ptr, type, member) \
  ((type *)((char *)(ptr) - offsetof(type, member)))

/*
 * Types
 */

struct leveldb_logger_s {
  void *rep;
};

struct leveldb_comparator_s {
  rdb_comparator_t rep;
  void *state;
  void (*destructor)(void*);
  int (*compare)(void *, const char *a, size_t alen,
                         const char *b, size_t blen);
};

struct leveldb_filterpolicy_s {
  rdb_bloom_t rep;
  void *state;
  void (*destructor)(void *);
  void (*key_add)(void *, char *data,
                          const char *key, size_t length,
                          size_t bits);
  uint8_t (*key_match)(void *, const char *key, size_t length,
                               const char *filter, size_t filter_length);
};

struct leveldb_env_s {
  void *rep;
  int is_default;
};

typedef struct iterate_opts_s {
  void *state;
  void (*put)(void *, const char *k, size_t klen, const char *v, size_t vlen);
  void (*del)(void *, const char *k, size_t klen);
} iterate_opts_t;

/*
 * Helpers
 */

static int
save_error(char **errptr, int status) {
  const char *msg;
  size_t len;

  assert(errptr != NULL);

  if (status == RDB_OK)
    return 0;

  if (*errptr != NULL)
    free(*errptr);

  msg = rdb_strerror(status);
  len = strlen(msg);

  *errptr = malloc(len + 1);

  if (*errptr == NULL)
    abort(); /* LCOV_EXCL_LINE */

  memcpy(*errptr, msg, len + 1);

  return 1;
}

/*
 * LevelDB
 */

leveldb_t *
leveldb_open(const leveldb_options_t *options,
             const char *name, char **errptr) {
  rdb_t *db;

  if (save_error(errptr, rdb_open(name, options, &db)))
    return NULL;

  return db;
}

void
leveldb_close(leveldb_t *db) {
  rdb_close(db);
}

void
leveldb_put(leveldb_t *db, const leveldb_writeoptions_t *options,
                           const char *key, size_t keylen,
                           const char *val, size_t vallen,
                           char **errptr) {
  rdb_slice_t k = rdb_slice((const uint8_t *)key, keylen);
  rdb_slice_t v = rdb_slice((const uint8_t *)val, vallen);

  save_error(errptr, rdb_put(db, &k, &v, options));
}

void
leveldb_delete(leveldb_t *db, const leveldb_writeoptions_t *options,
                              const char *key, size_t keylen,
                              char **errptr) {
  rdb_slice_t k = rdb_slice((const uint8_t *)key, keylen);

  save_error(errptr, rdb_del(db, &k, options));
}

void
leveldb_write(leveldb_t *db, const leveldb_writeoptions_t *options,
                             leveldb_writebatch_t *batch,
                             char **errptr) {
  save_error(errptr, rdb_write(db, batch, options));
}

char *
leveldb_get(leveldb_t *db, const leveldb_readoptions_t *options,
                           const char *key, size_t keylen,
                           size_t *vallen, char **errptr) {
  rdb_slice_t k = rdb_slice((const uint8_t *)key, keylen);
  rdb_slice_t v;
  char *result;
  int rc;

  rc = rdb_get(db, &k, &v, options);

  if (rc == RDB_OK) {
    *vallen = v.size;
    result = (char *)v.data;
  } else {
    *vallen = 0;
    result = NULL;

    if (rc != RDB_NOTFOUND)
      save_error(errptr, rc);
  }

  return result;
}

leveldb_iterator_t *
leveldb_create_iterator(leveldb_t *db, const leveldb_readoptions_t *options) {
  return rdb_iterator(db, options);
}

const leveldb_snapshot_t *
leveldb_create_snapshot(leveldb_t *db) {
  return rdb_get_snapshot(db);
}

void
leveldb_release_snapshot(leveldb_t *db, const leveldb_snapshot_t *snapshot) {
  rdb_release_snapshot(db, snapshot);
}

char *
leveldb_property_value(leveldb_t *db, const char *propname) {
  char *result;

  if (rdb_get_property(db, propname, &result))
    return result;

  return NULL;
}

void
leveldb_approximate_sizes(leveldb_t *db, int num_ranges,
                          const char *const *range_start_key,
                          const size_t *range_start_key_len,
                          const char *const *range_limit_key,
                          const size_t *range_limit_key_len,
                          uint64_t *sizes) {
  rdb_range_t *ranges = rdb_malloc(num_ranges * sizeof(rdb_range_t));
  int i;

  for (i = 0; i < num_ranges; i++) {
    rdb_slice_set(&ranges[i].start, (const uint8_t *)range_start_key[i],
                                                     range_start_key_len[i]);

    rdb_slice_set(&ranges[i].limit, (const uint8_t *)range_limit_key[i],
                                                     range_limit_key_len[i]);
  }

  rdb_get_approximate_sizes(db, ranges, num_ranges, sizes);
  rdb_free(ranges);
}

void
leveldb_compact_range(leveldb_t *db,
                      const char *start_key, size_t start_key_len,
                      const char *limit_key, size_t limit_key_len) {
  rdb_slice_t start, limit;

  rdb_slice_set(&start, (const uint8_t *)start_key, start_key_len);
  rdb_slice_set(&limit, (const uint8_t *)limit_key, limit_key_len);

  rdb_compact_range(db, &start, &limit);
}

void
leveldb_destroy_db(const leveldb_options_t *options,
                   const char *name, char **errptr) {
  save_error(errptr, rdb_destroy_db(name, options));
}

void
leveldb_repair_db(const leveldb_options_t *options,
                  const char *name, char **errptr) {
  save_error(errptr, rdb_repair_db(name, options));
}

void
leveldb_iter_destroy(leveldb_iterator_t *iter) {
  rdb_iter_destroy(iter);
}

uint8_t
leveldb_iter_valid(const leveldb_iterator_t *iter) {
  return rdb_iter_valid(iter);
}

void
leveldb_iter_seek_to_first(leveldb_iterator_t *iter) {
  rdb_iter_seek_first(iter);
}

void
leveldb_iter_seek_to_last(leveldb_iterator_t *iter) {
  rdb_iter_seek_last(iter);
}

void
leveldb_iter_seek(leveldb_iterator_t *iter, const char *k, size_t klen) {
  rdb_slice_t key = rdb_slice((const uint8_t *)k, klen);

  rdb_iter_seek(iter, &key);
}

void
leveldb_iter_next(leveldb_iterator_t *iter) {
  rdb_iter_next(iter);
}

void
leveldb_iter_prev(leveldb_iterator_t *iter) {
  rdb_iter_prev(iter);
}

const char *
leveldb_iter_key(const leveldb_iterator_t *iter, size_t *klen) {
  rdb_slice_t key = rdb_iter_key(iter);
  *klen = key.size;
  return (const char *)key.data;
}

const char *
leveldb_iter_value(const leveldb_iterator_t *iter, size_t *vlen) {
  rdb_slice_t val = rdb_iter_value(iter);
  *vlen = val.size;
  return (const char *)val.data;
}

void
leveldb_iter_get_error(const leveldb_iterator_t *iter, char **errptr) {
  save_error(errptr, rdb_iter_status(iter));
}

leveldb_writebatch_t *
leveldb_writebatch_create(void) {
  return rdb_batch_create();
}

void
leveldb_writebatch_destroy(leveldb_writebatch_t *b) {
  rdb_batch_destroy(b);
}

void
leveldb_writebatch_clear(leveldb_writebatch_t *b) {
  rdb_batch_reset(b);
}

void
leveldb_writebatch_put(leveldb_writebatch_t *b,
                       const char *key, size_t klen,
                       const char *val, size_t vlen) {
  rdb_slice_t k = rdb_slice((const uint8_t *)key, klen);
  rdb_slice_t v = rdb_slice((const uint8_t *)val, vlen);

  rdb_batch_put(b, &k, &v);
}

void
leveldb_writebatch_delete(leveldb_writebatch_t *b,
                          const char *key, size_t klen) {
  rdb_slice_t k = rdb_slice((const uint8_t *)key, klen);

  rdb_batch_del(b, &k);
}

static void
handle_put(rdb_handler_t *h, const rdb_slice_t *key, const rdb_slice_t *value) {
  iterate_opts_t *opt = h->state;

  opt->put(opt->state, (const char *)key->data, key->size,
                       (const char *)value->data, value->size);
}

static void
handle_del(rdb_handler_t *h, const rdb_slice_t *key) {
  iterate_opts_t *opt = h->state;

  opt->del(opt->state, (const char *)key->data, key->size);
}

void
leveldb_writebatch_iterate(const leveldb_writebatch_t *b, void *state,
                           void (*put)(void *, const char *k, size_t klen,
                                               const char *v, size_t vlen),
                           void (*del)(void *, const char *k, size_t klen)) {
  rdb_handler_t handler;
  iterate_opts_t opt;

  opt.state = state;
  opt.put = put;
  opt.del = del;

  handler.state = &opt;
  handler.put = handle_put;
  handler.del = handle_del;

  rdb_batch_iterate(b, &handler);
}

void
leveldb_writebatch_append(leveldb_writebatch_t *destination,
                          const leveldb_writebatch_t *source) {
  rdb_batch_append(destination, source);
}

leveldb_options_t *
leveldb_options_create(void) {
  rdb_dbopt_t *options = rdb_malloc(sizeof(rdb_dbopt_t));
  *options = *rdb_dbopt_default;
  return options;
}

void
leveldb_options_destroy(leveldb_options_t *options) {
  rdb_free(options);
}

void
leveldb_options_set_comparator(leveldb_options_t *opt,
                               leveldb_comparator_t *cmp) {
  opt->comparator = &cmp->rep;
}

void
leveldb_options_set_filter_policy(leveldb_options_t *opt,
                                  leveldb_filterpolicy_t *policy) {
  opt->filter_policy = &policy->rep;
}

void
leveldb_options_set_create_if_missing(leveldb_options_t *opt, uint8_t v) {
  opt->create_if_missing = v;
}

void
leveldb_options_set_error_if_exists(leveldb_options_t *opt, uint8_t v) {
  opt->error_if_exists = v;
}

void
leveldb_options_set_paranoid_checks(leveldb_options_t *opt, uint8_t v) {
  opt->paranoid_checks = v;
}

void
leveldb_options_set_env(leveldb_options_t *opt, leveldb_env_t *env) {
  (void)opt;
  (void)env;
}

void
leveldb_options_set_info_log(leveldb_options_t *opt, leveldb_logger_t *l) {
  (void)opt;
  (void)l;
}

void
leveldb_options_set_write_buffer_size(leveldb_options_t *opt, size_t s) {
  opt->write_buffer_size = s;
}

void
leveldb_options_set_max_open_files(leveldb_options_t *opt, int n) {
  opt->max_open_files = n;
}

void
leveldb_options_set_cache(leveldb_options_t *opt, leveldb_cache_t *c) {
  opt->block_cache = c;
}

void
leveldb_options_set_block_size(leveldb_options_t *opt, size_t s) {
  opt->block_size = s;
}

void
leveldb_options_set_block_restart_interval(leveldb_options_t *opt, int n) {
  opt->block_restart_interval = n;
}

void
leveldb_options_set_max_file_size(leveldb_options_t *opt, size_t s) {
  opt->max_file_size = s;
}

void
leveldb_options_set_compression(leveldb_options_t *opt, int t) {
  opt->compression = (enum rdb_compression)t;
}

static int
slice_compare(const rdb_comparator_t *comparator,
              const rdb_slice_t *x,
              const rdb_slice_t *y) {
  leveldb_comparator_t *cmp = container_of(comparator,
                                           leveldb_comparator_t,
                                           rep);

  return (*cmp->compare)(cmp->state, (const char *)x->data, x->size,
                                     (const char *)y->data, y->size);
}

static void
shortest_separator(const rdb_comparator_t *comparator,
                   rdb_buffer_t *start,
                   const rdb_slice_t *limit) {
  (void)comparator;
  (void)start;
  (void)limit;
}

static void
short_successor(const rdb_comparator_t *comparator, rdb_buffer_t *key) {
  (void)comparator;
  (void)key;
}

leveldb_comparator_t *
leveldb_comparator_create(void *state,
                          void (*destructor)(void *),
                          int (*compare)(void *, const char *a, size_t alen,
                                                 const char *b, size_t blen),
                          const char *(*name)(void *)) {
  leveldb_comparator_t *cmp = rdb_malloc(sizeof(leveldb_comparator_t));

  cmp->rep.name = name(state);
  cmp->rep.compare = slice_compare;
  cmp->rep.shortest_separator = shortest_separator;
  cmp->rep.short_successor = short_successor;
  cmp->rep.user_comparator = NULL;

  cmp->state = state;
  cmp->destructor = destructor;
  cmp->compare = compare;

  return cmp;
}

void
leveldb_comparator_destroy(leveldb_comparator_t *cmp) {
  if (cmp->destructor != NULL)
    cmp->destructor(cmp->state);

  rdb_free(cmp);
}

static void
bloom_add(const rdb_bloom_t *bloom,
          uint8_t *data,
          const rdb_slice_t *key,
          size_t bits) {
  leveldb_filterpolicy_t *fp = container_of(bloom,
                                            leveldb_filterpolicy_t,
                                            rep);

  fp->key_add(fp->state, (char *)data,
                         (const char *)key->data, key->size,
                         bits);
}

static int
bloom_match(const rdb_bloom_t *bloom,
            const rdb_slice_t *filter,
            const rdb_slice_t *key) {
  leveldb_filterpolicy_t *fp = container_of(bloom,
                                            leveldb_filterpolicy_t,
                                            rep);

  return fp->key_match(fp->state, (const char *)key->data, key->size,
                                  (const char *)filter->data, filter->size);
}

leveldb_filterpolicy_t *
leveldb_filterpolicy_create(void *state,
                            void (*destructor)(void *),
                            void (*key_add)(void *, char *data,
                                                    const char *key,
                                                    size_t length,
                                                    size_t bits),
                            uint8_t (*key_match)(void *,
                                                 const char *key,
                                                 size_t length,
                                                 const char *filter,
                                                 size_t filter_length),
                            const char *(*name)(void *)) {
  leveldb_filterpolicy_t *policy = rdb_malloc(sizeof(leveldb_filterpolicy_t));

  rdb_bloom_init(&policy->rep, 0);

  policy->rep.name = name(state);
  policy->rep.add = bloom_add;
  policy->rep.match = bloom_match;

  policy->state = state;
  policy->destructor = destructor;
  policy->key_add = key_add;
  policy->key_match = key_match;

  return policy;
}

void
leveldb_filterpolicy_destroy(leveldb_filterpolicy_t *filter) {
  if (filter->destructor != NULL)
    filter->destructor(filter->state);

  rdb_free(filter);
}

leveldb_filterpolicy_t *
leveldb_filterpolicy_create_bloom(int bits_per_key) {
  leveldb_filterpolicy_t *policy = rdb_malloc(sizeof(leveldb_filterpolicy_t));

  rdb_bloom_init(&policy->rep, bits_per_key);

  policy->state = NULL;
  policy->destructor = NULL;
  policy->key_add = NULL;
  policy->key_match = NULL;

  return policy;
}

leveldb_readoptions_t *
leveldb_readoptions_create(void) {
  rdb_readopt_t *options = rdb_malloc(sizeof(rdb_readopt_t));
  *options = *rdb_readopt_default;
  return options;
}

void
leveldb_readoptions_destroy(leveldb_readoptions_t *opt) {
  rdb_free(opt);
}

void
leveldb_readoptions_set_verify_checksums(leveldb_readoptions_t *opt,
                                         uint8_t v) {
  opt->verify_checksums = v;
}

void
leveldb_readoptions_set_fill_cache(leveldb_readoptions_t *opt, uint8_t v) {
  opt->fill_cache = v;
}

void
leveldb_readoptions_set_snapshot(leveldb_readoptions_t *opt,
                                 const leveldb_snapshot_t *snap) {
  opt->snapshot = snap;
}

leveldb_writeoptions_t *
leveldb_writeoptions_create(void) {
  rdb_writeopt_t *options = rdb_malloc(sizeof(rdb_writeopt_t));
  *options = *rdb_writeopt_default;
  return options;
}

void
leveldb_writeoptions_destroy(leveldb_writeoptions_t *opt) {
  rdb_free(opt);
}

void
leveldb_writeoptions_set_sync(leveldb_writeoptions_t *opt,
                              uint8_t v) {
  opt->sync = v;
}

leveldb_cache_t *
leveldb_cache_create_lru(size_t capacity) {
  return rdb_lru_create(capacity);
}

void
leveldb_cache_destroy(leveldb_cache_t *cache) {
  rdb_lru_destroy(cache);
}

leveldb_env_t *
leveldb_create_default_env(void) {
  leveldb_env_t *env = rdb_malloc(sizeof(leveldb_env_t));
  env->rep = NULL;
  env->is_default = 1;
  return env;
}

void
leveldb_env_destroy(leveldb_env_t *env) {
  rdb_free(env);
}

char *
leveldb_env_get_test_directory(leveldb_env_t *env) {
  (void)env;
  return NULL;
}

void
leveldb_free(void *ptr) {
  rdb_free(ptr);
}

int
leveldb_major_version() {
  return 0;
}

int
leveldb_minor_version() {
  return 0;
}
