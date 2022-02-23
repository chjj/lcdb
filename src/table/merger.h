/*!
 * merger.h - merging iterator for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_MERGER_H
#define RDB_MERGER_H

struct rdb_comparator_s;
struct rdb_iter_s;

/* Return an iterator that provided the union of the data in
 * children[0,n-1].  Takes ownership of the child iterators and
 * will delete them when the result iterator is deleted.
 *
 * The result does no duplicate suppression.  I.e., if a particular
 * key is present in K child iterators, it will be yielded K times.
 *
 * REQUIRES: n >= 0
 */
struct rdb_iter_s *
rdb_mergeiter_create(const struct rdb_comparator_s *comparator,
                     struct rdb_iter_s **children,
                     int n);

#endif /* RDB_MERGER_H */
