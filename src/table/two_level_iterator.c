/*!
 * two_level_iterator.c - two-level iterator for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stddef.h>

#include "../util/buffer.h"
#include "../util/internal.h"
#include "../util/options.h"
#include "../util/slice.h"
#include "../util/status.h"

#include "iterator.h"
#include "iterator_wrapper.h"
#include "two_level_iterator.h"

/*
 * Two-Level Iterator
 */

typedef struct rdb_twoiter_s {
  rdb_blockfunc_f block_function;
  void *arg;
  rdb_readopt_t options;
  int status;
  rdb_wrapiter_t index_iter;
  rdb_wrapiter_t data_iter; /* May be NULL. */
  /* If data_iter is non-null, then "data_block_handle_" holds the
    "index_value" passed to block_function to create the data_iter. */
  rdb_buffer_t data_block_handle; /* XXX */
} rdb_twoiter_t;

static int
rdb_twoiter_valid(const rdb_twoiter_t *iter) {
  return rdb_wrapiter_valid(&iter->data_iter);
}

static rdb_slice_t
rdb_twoiter_key(const rdb_twoiter_t *iter) {
  assert(rdb_twoiter_valid(iter));
  return rdb_wrapiter_key(&iter->data_iter);
}

static rdb_slice_t
rdb_twoiter_value(const rdb_twoiter_t *iter) {
  assert(rdb_twoiter_valid(iter));
  return rdb_wrapiter_value(&iter->data_iter);
}

static int
rdb_twoiter_status(const rdb_twoiter_t *iter) {
  int rc;

  if ((rc = rdb_wrapiter_status(&iter->index_iter)))
    return rc;

  if (iter->data_iter.iter != NULL) {
    if ((rc = rdb_wrapiter_status(&iter->data_iter)))
      return rc;
  }

  return iter->status;
}

static void
rdb_twoiter_saverr(rdb_twoiter_t *iter, int status) {
  if (iter->status == RDB_OK && status != RDB_OK)
    iter->status = status;
}

static void
rdb_twoiter_init(rdb_twoiter_t *iter,
               rdb_iter_t *index_iter,
               rdb_blockfunc_f block_function,
               void *arg,
               const rdb_readopt_t *options) {
  iter->block_function = block_function;
  iter->arg = arg;
  iter->options = *options;
  iter->status = RDB_OK;

  rdb_wrapiter_init(&iter->index_iter, index_iter);
  rdb_wrapiter_init(&iter->data_iter, NULL);
  rdb_buffer_init(&iter->data_block_handle);
}

static void
rdb_twoiter_clear(rdb_twoiter_t *iter) {
  rdb_wrapiter_clear(&iter->index_iter);
  rdb_wrapiter_clear(&iter->data_iter);
  rdb_buffer_clear(&iter->data_block_handle);
}

static void
rdb_twoiter_set_data_iter(rdb_twoiter_t *iter, rdb_iter_t *data_iter) {
  if (iter->data_iter.iter != NULL)
    rdb_twoiter_saverr(iter, rdb_wrapiter_status(&iter->data_iter));

  rdb_wrapiter_set(&iter->data_iter, data_iter);
}

static void
rdb_twoiter_init_data_block(rdb_twoiter_t *iter) {
  if (!rdb_wrapiter_valid(&iter->index_iter)) {
    rdb_twoiter_set_data_iter(iter, NULL);
  } else {
    rdb_slice_t handle = rdb_wrapiter_value(&iter->index_iter);

    if (iter->data_iter.iter != NULL
        && rdb_slice_equal(&handle, &iter->data_block_handle)) {
      /* data_iter is already constructed with this iterator, so
         no need to change anything. */
    } else {
      rdb_iter_t *data_iter = iter->block_function(iter->arg,
                                                   &iter->options,
                                                   &handle);

      rdb_buffer_copy(&iter->data_block_handle, &handle);
      /* rdb_buffer_set(&iter->data_block_handle, handle.data, handle.size); */

      rdb_twoiter_set_data_iter(iter, data_iter);
    }
  }
}

static void
rdb_twoiter_skip_forward(rdb_twoiter_t *iter) {
  while (iter->data_iter.iter == NULL || !rdb_wrapiter_valid(&iter->data_iter)) {
    /* Move to next block. */
    if (!rdb_wrapiter_valid(&iter->index_iter)) {
      rdb_twoiter_set_data_iter(iter, NULL);
      return;
    }

    rdb_wrapiter_next(&iter->index_iter);
    rdb_twoiter_init_data_block(iter);

    if (iter->data_iter.iter != NULL)
      rdb_wrapiter_seek_first(&iter->data_iter);
  }
}

static void
rdb_twoiter_skip_backward(rdb_twoiter_t *iter) {
  while (iter->data_iter.iter == NULL || !rdb_wrapiter_valid(&iter->data_iter)) {
    /* Move to next block. */
    if (!rdb_wrapiter_valid(&iter->index_iter)) {
      rdb_twoiter_set_data_iter(iter, NULL);
      return;
    }

    rdb_wrapiter_prev(&iter->index_iter);
    rdb_twoiter_init_data_block(iter);

    if (iter->data_iter.iter != NULL)
      rdb_wrapiter_seek_first(&iter->data_iter);
  }
}

static void
rdb_twoiter_seek(rdb_twoiter_t *iter, const rdb_slice_t *target) {
  rdb_wrapiter_seek(&iter->index_iter, target);
  rdb_twoiter_init_data_block(iter);

  if (iter->data_iter.iter != NULL)
    rdb_wrapiter_seek(&iter->data_iter, target);

  rdb_twoiter_skip_forward(iter);
}

static void
rdb_twoiter_seek_first(rdb_twoiter_t *iter) {
  rdb_wrapiter_seek_first(&iter->index_iter);
  rdb_twoiter_init_data_block(iter);

  if (iter->data_iter.iter != NULL)
    rdb_wrapiter_seek_first(&iter->data_iter);

  rdb_twoiter_skip_forward(iter);
}

static void
rdb_twoiter_seek_last(rdb_twoiter_t *iter) {
  rdb_wrapiter_seek_last(&iter->index_iter);
  rdb_twoiter_init_data_block(iter);

  if (iter->data_iter.iter != NULL)
    rdb_wrapiter_seek_last(&iter->data_iter);

  rdb_twoiter_skip_backward(iter);
}

static void
rdb_twoiter_next(rdb_twoiter_t *iter) {
  assert(rdb_twoiter_valid(iter));
  rdb_wrapiter_next(&iter->data_iter);
  rdb_twoiter_skip_forward(iter);
}

static void
rdb_twoiter_prev(rdb_twoiter_t *iter) {
  assert(rdb_twoiter_valid(iter));
  rdb_wrapiter_prev(&iter->data_iter);
  rdb_twoiter_skip_backward(iter);
}

RDB_ITERATOR_FUNCTIONS(rdb_twoiter);

rdb_iter_t *
rdb_twoiter_create(rdb_iter_t *index_iter,
                   rdb_blockfunc_f block_function,
                   void *arg,
                   const rdb_readopt_t *options) {
  rdb_twoiter_t *iter = rdb_malloc(sizeof(rdb_twoiter_t));

  rdb_twoiter_init(iter, index_iter, block_function, arg, options);

  return rdb_iter_create(iter, &rdb_twoiter_table);
}
