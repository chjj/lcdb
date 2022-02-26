/*!
 * table_cache.h - sstable cache for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_TABLE_CACHE_H
#define RDB_TABLE_CACHE_H

#include <stdint.h>

#include "table/table.h"
#include "util/options.h"
#include "util/types.h"

/*
 * Types
 */

struct rdb_iter_s;

typedef struct rdb_tcache_s rdb_tcache_t;

/*
 * TableCache
 */

rdb_tcache_t *
rdb_tcache_create(const char *prefix, const rdb_dbopt_t *options, int entries);

void
rdb_tcache_destroy(rdb_tcache_t *cache);

/* Return an iterator for the specified file number (the corresponding
 * file length must be exactly "file_size" bytes).  If "tableptr" is
 * non-null, also sets "*tableptr" to point to the Table object
 * underlying the returned iterator, or to nullptr if no Table object
 * underlies the returned iterator.  The returned "*tableptr" object is owned
 * by the cache and should not be deleted, and is valid for as long as the
 * returned iterator is live.
 */
struct rdb_iter_s *
rdb_tcache_iterate(rdb_tcache_t *cache,
                   const rdb_readopt_t *options,
                   uint64_t file_number,
                   uint64_t file_size,
                   rdb_table_t **tableptr);

/* If a seek to internal key "k" in specified file finds an entry,
   call (*handle_result)(arg, found_key, found_value). */
int
rdb_tcache_get(rdb_tcache_t *cache,
               const rdb_readopt_t *options,
               uint64_t file_number,
               uint64_t file_size,
               const rdb_slice_t *k,
               void *arg,
               void (*handle_result)(void *,
                                     const rdb_slice_t *,
                                     const rdb_slice_t *));

/* Evict any entry for the specified file number. */
void
rdb_tcache_evict(rdb_tcache_t *cache, uint64_t file_number);

#endif /* RDB_TABLE_CACHE_H */
