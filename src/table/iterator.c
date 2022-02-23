/*!
 * iterator.c - iterator for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stddef.h>

#include "../util/internal.h"
#include "../util/types.h"

#include "iterator.h"

/*
 * Iterator
 */

static void
rdb_iter_init(rdb_iter_t *iter, void *ptr, const rdb_itertbl_t *table) {
  iter->ptr = ptr;
  iter->cleanup_head.func = NULL;
  iter->cleanup_head.next = NULL;
  iter->table = table;
}

static void
rdb_iter_clear(rdb_iter_t *iter) {
  if (!rdb_cleanup_empty(&iter->cleanup_head)) {
    rdb_cleanup_t *node, *next;

    rdb_cleanup_run(&iter->cleanup_head);

    for (node = iter->cleanup_head.next; node != NULL; node = next) {
      next = node->next;
      rdb_cleanup_run(node);
      rdb_free(node);
    }
  }

  iter->table->clear(iter->ptr);

  rdb_free(iter->ptr);
}

rdb_iter_t *
rdb_iter_create(void *ptr, const rdb_itertbl_t *table) {
  rdb_iter_t *iter = rdb_malloc(sizeof(rdb_iter_t));
  rdb_iter_init(iter, ptr, table);
  return iter;
}

void
rdb_iter_destroy(rdb_iter_t *iter) {
  rdb_iter_clear(iter);
  rdb_free(iter);
}

void
rdb_iter_register_cleanup(rdb_iter_t *iter,
                          rdb_cleanup_f func,
                          void *arg1,
                          void *arg2) {
  rdb_cleanup_t *node;

  if (rdb_cleanup_empty(&iter->cleanup_head)) {
    node = &iter->cleanup_head;
  } else {
    node = rdb_malloc(sizeof(rdb_cleanup_t));
    node->next = iter->cleanup_head.next;
    iter->cleanup_head.next = node;
  }

  node->func = func;
  node->arg1 = arg1;
  node->arg2 = arg2;
}

/*
 * Empty Iterator
 */

typedef struct rdb_emptyiter_s {
  int status;
} rdb_emptyiter_t;

static void
rdb_emptyiter_clear(rdb_emptyiter_t *iter) {
  (void)iter;
}

static int
rdb_emptyiter_valid(const rdb_emptyiter_t *iter) {
  (void)iter;
  return 0;
}

static void
rdb_emptyiter_seek(rdb_emptyiter_t *iter, const rdb_slice_t *target) {
  (void)iter;
  (void)target;
}

static void
rdb_emptyiter_seek_first(rdb_emptyiter_t *iter) {
  (void)iter;
}

static void
rdb_emptyiter_seek_last(rdb_emptyiter_t *iter) {
  (void)iter;
}

static void
rdb_emptyiter_next(rdb_emptyiter_t *iter) {
  (void)iter;
}

static void
rdb_emptyiter_prev(rdb_emptyiter_t *iter) {
  (void)iter;
}

static rdb_slice_t
rdb_emptyiter_key(const rdb_emptyiter_t *iter) {
  rdb_slice_t ret = {NULL, 0, 0};
  (void)iter;
  assert(0);
  return ret;
}

static rdb_slice_t
rdb_emptyiter_value(const rdb_emptyiter_t *iter) {
  rdb_slice_t ret = {NULL, 0, 0};
  (void)iter;
  assert(0);
  return ret;
}

static int
rdb_emptyiter_status(const rdb_emptyiter_t *iter) {
  return iter->status;
}

RDB_ITERATOR_FUNCTIONS(rdb_emptyiter);

rdb_iter_t *
rdb_emptyiter_create(int status) {
  rdb_emptyiter_t *iter = rdb_malloc(sizeof(rdb_emptyiter_t));

  iter->status = status;

  return rdb_iter_create(iter, &rdb_emptyiter_table);
}
