/*!
 * db_iter.h - database iterator for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_DB_ITER_H
#define RDB_DB_ITER_H

#include <stdint.h>

struct rdb_comparator_s;
struct rdb_impl_s;
struct rdb_iter_s;

struct rdb_iter_s *
rdb_dbiter_create(struct rdb_impl_s *db,
                  const struct rdb_comparator_s *user_comparator,
                  struct rdb_iter_s *internal_iter,
                  uint64_t sequence,
                  uint32_t seed);

#endif /* RDB_DB_ITER_H */
