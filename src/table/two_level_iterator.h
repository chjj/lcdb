/*!
 * two_level_iterator.h - two-level iterator for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_TWO_LEVEL_ITERATOR_H
#define RDB_TWO_LEVEL_ITERATOR_H

#include "../util/types.h"

/*
 * Types
 */

struct rdb_iter_s;
struct rdb_readopt_s;

typedef struct rdb_iter_s *(*rdb_blockfunc_f)(void *,
                                              const struct rdb_readopt_s *,
                                              const rdb_slice_t *);

/*
 * Two-Level Iterator
 */

/* Return a new two level iterator. A two-level iterator contains an
 * index iterator whose values point to a sequence of blocks where
 * each block is itself a sequence of key,value pairs. The returned
 * two-level iterator yields the concatenation of all key/value pairs
 * in the sequence of blocks. Takes ownership of "index_iter" and
 * will delete it when no longer needed.
 *
 * Uses a supplied function to convert an index_iter value into
 * an iterator over the contents of the corresponding block.
 */
struct rdb_iter_s *
rdb_twoiter_create(struct rdb_iter_s *index_iter,
                   rdb_blockfunc_f block_function,
                   void *arg,
                   const struct rdb_readopt_s *options);

#endif /* RDB_TWO_LEVEL_ITERATOR_H */
