/*!
 * iterator_wrapper.h - iterator wrapper for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_ITERATOR_WRAPPER_H
#define RDB_ITERATOR_WRAPPER_H

#include <assert.h>
#include <stddef.h>

#include "../util/internal.h"
#include "../util/types.h"

#include "iterator.h"

/* A internal wrapper class with an interface similar to Iterator that
   caches the valid() and key() results for an underlying iterator.
   This can help avoid virtual function calls and also gives better
   cache locality. */

/*
 * Iterator Wrapper
 */

typedef struct rdb_wrapiter_s {
  rdb_iter_t *iter;
  int valid;
  rdb_slice_t key;
} rdb_wrapiter_t;

RDB_UNUSED static void
rdb_wrapiter_update(rdb_wrapiter_t *wrap) {
  wrap->valid = rdb_iter_valid(wrap->iter);

  if (wrap->valid)
    wrap->key = rdb_iter_key(wrap->iter);
}

RDB_UNUSED static void
rdb_wrapiter_init(rdb_wrapiter_t *wrap, rdb_iter_t *iter) {
  wrap->iter = iter;
  wrap->valid = 0;

  if (wrap->iter != NULL)
    rdb_wrapiter_update(wrap);
}

RDB_UNUSED static void
rdb_wrapiter_clear(rdb_wrapiter_t *wrap) {
  if (wrap->iter != NULL)
    rdb_iter_destroy(wrap->iter);
}

/* Takes ownership of "iter" and will delete it when destroyed, or
   when Set() is invoked again. */
RDB_UNUSED static void
rdb_wrapiter_set(rdb_wrapiter_t *wrap, rdb_iter_t *iter) {
  if (wrap->iter != NULL)
    rdb_iter_destroy(wrap->iter);

  wrap->iter = iter;

  if (wrap->iter == NULL)
    wrap->valid = 0;
  else
    rdb_wrapiter_update(wrap);
}

RDB_UNUSED static int
rdb_wrapiter_valid(const rdb_wrapiter_t *wrap) {
  return wrap->valid;
}

RDB_UNUSED static void
rdb_wrapiter_seek(rdb_wrapiter_t *wrap, const rdb_slice_t *k) {
  assert(wrap->iter != NULL);
  rdb_iter_seek(wrap->iter, k);
  rdb_wrapiter_update(wrap);
}

RDB_UNUSED static void
rdb_wrapiter_seek_first(rdb_wrapiter_t *wrap) {
  assert(wrap->iter != NULL);
  rdb_iter_seek_first(wrap->iter);
  rdb_wrapiter_update(wrap);
}

RDB_UNUSED static void
rdb_wrapiter_seek_last(rdb_wrapiter_t *wrap) {
  assert(wrap->iter != NULL);
  rdb_iter_seek_last(wrap->iter);
  rdb_wrapiter_update(wrap);
}

RDB_UNUSED static void
rdb_wrapiter_next(rdb_wrapiter_t *wrap) {
  assert(wrap->iter != NULL);
  rdb_iter_next(wrap->iter);
  rdb_wrapiter_update(wrap);
}

RDB_UNUSED static void
rdb_wrapiter_prev(rdb_wrapiter_t *wrap) {
  assert(wrap->iter != NULL);
  rdb_iter_prev(wrap->iter);
  rdb_wrapiter_update(wrap);
}

RDB_UNUSED static rdb_slice_t
rdb_wrapiter_key(const rdb_wrapiter_t *wrap) {
  assert(rdb_wrapiter_valid(wrap));
  return wrap->key;
}

RDB_UNUSED static rdb_slice_t
rdb_wrapiter_value(const rdb_wrapiter_t *wrap) {
  assert(rdb_wrapiter_valid(wrap));
  return rdb_iter_value(wrap->iter);
}

RDB_UNUSED static int
rdb_wrapiter_status(const rdb_wrapiter_t *wrap) {
  assert(wrap->iter != NULL);
  return rdb_iter_status(wrap->iter);
}

#endif /* RDB_ITERATOR_WRAPPER_H */
