/*!
 * db_impl.h - database implementation for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_DB_IMPL_H
#define RDB_DB_IMPL_H

#include <stddef.h>
#include <stdint.h>

#include "util/extern.h"
#include "util/options.h"
#include "util/types.h"

/*
 * Types
 */

struct rdb_bloom_s;
struct rdb_batch_s;
struct rdb_comparator_s;
struct rdb_iter_s;
struct rdb_snapshot_s;

typedef struct rdb_s rdb_t;

/*
 * Helpers
 */

rdb_dbopt_t
rdb_sanitize_options(const char *dbname,
                     const struct rdb_comparator_s *icmp,
                     const struct rdb_bloom_s *ipolicy,
                     const rdb_dbopt_t *src);

/*
 * API
 */

RDB_EXTERN int
rdb_open(const char *dbname, const rdb_dbopt_t *options, rdb_t **dbptr);

RDB_EXTERN void
rdb_close(rdb_t *db);

RDB_EXTERN int
rdb_get(rdb_t *db, const rdb_slice_t *key,
                   rdb_slice_t *value,
                   const rdb_readopt_t *options);

RDB_EXTERN int
rdb_has(rdb_t *db, const rdb_slice_t *key, const rdb_readopt_t *options);

RDB_EXTERN int
rdb_put(rdb_t *db, const rdb_slice_t *key,
                   const rdb_slice_t *value,
                   const rdb_writeopt_t *options);

RDB_EXTERN int
rdb_del(rdb_t *db, const rdb_slice_t *key, const rdb_writeopt_t *options);

RDB_EXTERN int
rdb_write(rdb_t *db, struct rdb_batch_s *updates, const rdb_writeopt_t *options);

RDB_EXTERN const struct rdb_snapshot_s *
rdb_get_snapshot(rdb_t *db);

RDB_EXTERN void
rdb_release_snapshot(rdb_t *db, const struct rdb_snapshot_s *snapshot);

RDB_EXTERN struct rdb_iter_s *
rdb_iterator(rdb_t *db, const rdb_readopt_t *options);

RDB_EXTERN int
rdb_get_property(rdb_t *db, const char *property, char **value);

RDB_EXTERN void
rdb_get_approximate_sizes(rdb_t *db, const rdb_range_t *range,
                                     size_t length,
                                     uint64_t *sizes);

RDB_EXTERN void
rdb_compact_range(rdb_t *db, const rdb_slice_t *begin, const rdb_slice_t *end);

/*
 * Static
 */

RDB_EXTERN int
rdb_repair_db(const char *dbname, const rdb_dbopt_t *options);

RDB_EXTERN int
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
