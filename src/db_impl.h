/*!
 * db_impl.h - database implementation for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_DB_IMPL_H
#define RDB_DB_IMPL_H

#include <stddef.h>
#include <stdint.h>

#include "util/bloom.h"
#include "util/comparator.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/types.h"

#include "snapshot.h"
#include "write_batch.h"

/*
 * Types
 */

struct rdb_iter_s;

typedef struct rdb_s rdb_t;

/*
 * Helpers
 */

rdb_dbopt_t
rdb_sanitize_options(const char *dbname,
                     const rdb_comparator_t *icmp,
                     const rdb_bloom_t *ipolicy,
                     const rdb_dbopt_t *src);

/*
 * API
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

struct rdb_iter_s *
rdb_iterator(rdb_t *db, const rdb_readopt_t *options);

int
rdb_get_property(rdb_t *db, const char *property, char **value);

void
rdb_get_approximate_sizes(rdb_t *db, const rdb_range_t *range,
                                     size_t length,
                                     uint64_t *sizes);

void
rdb_compact_range(rdb_t *db, const rdb_slice_t *begin,
                             const rdb_slice_t *end);

/*
 * Static
 */

int
rdb_repair_db(const char *dbname, const rdb_dbopt_t *options);

int
rdb_destroy_db(const char *dbname, const rdb_dbopt_t *options);

/*
 * Testing
 */

int
rdb_test_compact_memtable(rdb_t *db);

void
rdb_test_compact_range(rdb_t *db, int level,
                                  const rdb_slice_t *begin,
                                  const rdb_slice_t *end);

struct rdb_iter_s *
rdb_test_internal_iterator(rdb_t *db);

int64_t
rdb_test_max_next_level_overlapping_bytes(rdb_t *db);

/*
 * Internal
 */

void
rdb_record_read_sample(rdb_t *db, const rdb_slice_t *key);

#endif /* RDB_DB_IMPL_H */
