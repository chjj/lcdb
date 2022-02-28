/*!
 * table_cache.c - sstable cache for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

#include "table/iterator.h"
#include "table/table.h"

#include "util/cache.h"
#include "util/coding.h"
#include "util/env.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/slice.h"
#include "util/status.h"

#include "filename.h"
#include "table_cache.h"

/*
 * Types
 */

struct rdb_tcache_s {
  const char *prefix;
  const rdb_dbopt_t *options;
  rdb_lru_t *lru;
};

typedef struct rdb_entry_s {
  rdb_rfile_t *file;
  rdb_table_t *table;
} rdb_entry_t;

/*
 * Helpers
 */

static void
delete_entry(const rdb_slice_t *key, void *value) {
  rdb_entry_t *entry = (rdb_entry_t *)value;

  (void)key;

  rdb_table_destroy(entry->table);
  rdb_rfile_destroy(entry->file);
  rdb_free(entry);
}

static void
unref_entry(void *arg1, void *arg2) {
  rdb_lru_t *lru = (rdb_lru_t *)arg1;
  rdb_lruhandle_t *h = (rdb_lruhandle_t *)arg2;

  rdb_lru_release(lru, h);
}

/*
 * TableCache
 */

rdb_tcache_t *
rdb_tcache_create(const char *prefix, const rdb_dbopt_t *options, int entries) {
  rdb_tcache_t *cache = rdb_malloc(sizeof(rdb_tcache_t));

  cache->prefix = prefix;
  cache->options = options;
  cache->lru = rdb_lru_create(entries);

  return cache;
}

void
rdb_tcache_destroy(rdb_tcache_t *cache) {
  rdb_lru_destroy(cache->lru);
}

static int
find_table(rdb_tcache_t *cache,
           uint64_t file_number,
           uint64_t file_size,
           rdb_lruhandle_t **handle) {
  rdb_slice_t key;
  int rc = RDB_OK;
  uint8_t buf[8];

  rdb_fixed64_write(buf, file_number);

  rdb_slice_set(&key, buf, sizeof(buf));

  *handle = rdb_lru_lookup(cache->lru, &key);

  if (*handle == NULL) {
    char fname[RDB_PATH_MAX];
    rdb_rfile_t *file = NULL;
    rdb_table_t *table = NULL;

    if (!rdb_table_filename(fname, sizeof(fname), cache->prefix, file_number))
      return RDB_INVALID;

    rc = rdb_randfile_create(fname, &file);

    if (rc != RDB_OK) {
      if (!rdb_sstable_filename(fname, sizeof(fname), cache->prefix, file_number))
        return RDB_INVALID;

      if (rdb_randfile_create(fname, &file) == RDB_OK)
        rc = RDB_OK;
    }

    if (rc == RDB_OK)
      rc = rdb_table_open(cache->options, file, file_size, &table);

    if (rc != RDB_OK) {
      assert(table == NULL);

      rdb_rfile_destroy(file);

      /* We do not cache error results so that if the error is transient,
         or somebody repairs the file, we recover automatically. */
    } else {
      rdb_entry_t *entry = rdb_malloc(sizeof(rdb_entry_t));

      entry->file = file;
      entry->table = table;

      *handle = rdb_lru_insert(cache->lru, &key, entry, 1, &delete_entry);
    }
  }

  return rc;
}

rdb_iter_t *
rdb_tcache_iterate(rdb_tcache_t *cache,
                   const rdb_readopt_t *options,
                   uint64_t file_number,
                   uint64_t file_size,
                   rdb_table_t **tableptr) {
  rdb_lruhandle_t *handle = NULL;
  rdb_table_t *table;
  rdb_iter_t *result;
  int rc;

  if (tableptr != NULL)
    *tableptr = NULL;

  rc = find_table(cache, file_number, file_size, &handle);

  if (rc != RDB_OK)
    return rdb_emptyiter_create(rc);

  table = ((rdb_entry_t *)rdb_lru_value(handle))->table;
  result = rdb_tableiter_create(table, options);

  rdb_iter_register_cleanup(result, &unref_entry, cache->lru, handle);

  if (tableptr != NULL)
    *tableptr = table;

  return result;
}

int
rdb_tcache_get(rdb_tcache_t *cache,
               const rdb_readopt_t *options,
               uint64_t file_number,
               uint64_t file_size,
               const rdb_slice_t *k,
               void *arg,
               void (*handle_result)(void *,
                                     const rdb_slice_t *,
                                     const rdb_slice_t *)) {
  rdb_lruhandle_t *handle = NULL;
  int rc;

  rc = find_table(cache, file_number, file_size, &handle);

  if (rc == RDB_OK) {
    rdb_table_t *table = ((rdb_entry_t *)rdb_lru_value(handle))->table;

    rc = rdb_table_internal_get(table, options, k, arg, handle_result);

    rdb_lru_release(cache->lru, handle);
  }

  return rc;
}

void
rdb_tcache_evict(rdb_tcache_t *cache, uint64_t file_number) {
  rdb_slice_t key;
  uint8_t buf[8];

  rdb_fixed64_write(buf, file_number);
  rdb_slice_set(&key, buf, sizeof(buf));

  rdb_lru_erase(cache->lru, &key);
}
