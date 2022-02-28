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

typedef struct rdb_impl_s rdb_impl_t;

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
rdb_impl_open(const char *dbname, const rdb_dbopt_t *options, rdb_impl_t **dbptr);

void
rdb_impl_close(rdb_impl_t *impl);

int
rdb_impl_get(rdb_impl_t *impl,
             const rdb_slice_t *key,
             rdb_slice_t *value,
             const rdb_readopt_t *options);

int
rdb_impl_has(rdb_impl_t *impl,
             const rdb_slice_t *key,
             const rdb_readopt_t *options);

int
rdb_impl_put(rdb_impl_t *impl,
             const rdb_slice_t *key,
             const rdb_slice_t *value,
             const rdb_writeopt_t *options);

int
rdb_impl_del(rdb_impl_t *impl, const rdb_slice_t *key, const rdb_writeopt_t *options);

int
rdb_impl_write(rdb_impl_t *impl,
               rdb_batch_t *updates,
               const rdb_writeopt_t *options);

const rdb_snapshot_t *
rdb_impl_get_snapshot(rdb_impl_t *impl);

void
rdb_impl_release_snapshot(rdb_impl_t *impl, const rdb_snapshot_t *snapshot);

struct rdb_iter_s *
rdb_impl_iterator(rdb_impl_t *impl, const rdb_readopt_t *options);

int
rdb_impl_get_property(rdb_impl_t *impl, const char *property, char **value);

void
rdb_impl_get_approximate_sizes(rdb_impl_t *impl,
                               const rdb_range_t *range,
                               size_t length,
                               uint64_t *sizes);

void
rdb_impl_compact_range(rdb_impl_t *impl,
                       const rdb_slice_t *begin,
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
rdb_impl_test_compact_memtable(rdb_impl_t *impl);

void
rdb_impl_test_compact_range(rdb_impl_t *impl,
                            int level,
                            const rdb_slice_t *begin,
                            const rdb_slice_t *end);

struct rdb_iter_s *
rdb_impl_test_internal_iterator(rdb_impl_t *impl);

int64_t
rdb_impl_test_max_next_level_overlapping_bytes(rdb_impl_t *impl);

/*
 * Internal
 */

void
rdb_impl_record_read_sample(rdb_impl_t *impl, const rdb_slice_t *key);

#endif /* RDB_DB_IMPL_H */
