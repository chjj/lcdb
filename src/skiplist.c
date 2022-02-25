/*!
 * skiplist.c - skiplist for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "util/arena.h"
#include "util/atomic.h"
#include "util/coding.h" /* don't need */
#include "util/comparator.h"
#include "util/internal.h"
#include "util/random.h"
#include "util/slice.h"

#include "skiplist.h"

/* Thread safety
 * -------------
 *
 * Writes require external synchronization, most likely a mutex.
 * Reads require a guarantee that the SkipList will not be destroyed
 * while the read is in progress. Apart from that, reads progress
 * without any internal locking or synchronization.
 *
 * Invariants:
 *
 * (1) Allocated nodes are never deleted until the SkipList is
 * destroyed. This is trivially guaranteed by the code since we
 * never delete any skip list nodes.
 *
 * (2) The contents of a Node except for the next/prev pointers are
 * immutable after the Node has been linked into the SkipList.
 * Only insert() modifies the list, and it is careful to initialize
 * a node and use release-stores to publish the nodes in one or
 * more lists.
 *
 * ... prev vs. next pointer ordering ...
 */

/*
 * Constants
 */

#define RDB_MAX_HEIGHT 12

/*
 * SkipList::Node
 */

static void
rdb_skipnode_init(rdb_skipnode_t *node, const uint8_t *key) {
  node->key = key;
}

static rdb_skipnode_t *
rdb_skipnode_next(rdb_skipnode_t *node, int n) {
  assert(n >= 0);
  /* Use an 'acquire load' so that we observe a fully initialized
     version of the returned Node. */
  return rdb_atomic_load_ptr(&node->next[n], rdb_order_acquire);
}

static void
rdb_skipnode_setnext(rdb_skipnode_t *node, int n, rdb_skipnode_t *x) {
  assert(n >= 0);
  /* Use a 'release store' so that anybody who reads through this
     pointer observes a fully initialized version of the inserted node. */
  rdb_atomic_store_ptr(&node->next[n], x, rdb_order_release);
}

/* No-barrier variants that can be safely used in a few locations. */
static rdb_skipnode_t *
rdb_skipnode_next_nb(rdb_skipnode_t *node, int n) {
  assert(n >= 0);
  return rdb_atomic_load_ptr(&node->next[n], rdb_order_relaxed);
}

static void
rdb_skipnode_setnext_nb(rdb_skipnode_t *node, int n, rdb_skipnode_t *x) {
  assert(n >= 0);
  rdb_atomic_store_ptr(&node->next[n], x, rdb_order_relaxed);
}

static rdb_skipnode_t *
rdb_skipnode_create(rdb_skiplist_t *list, const uint8_t *key, int height) {
  size_t size = (sizeof(rdb_skipnode_t)
               + sizeof(rdb_atomic_ptr(rdb_skipnode_t)) * (height - 1));

  rdb_skipnode_t *node = rdb_arena_alloc_aligned(list->arena, size);

  memset(node, 0, size);

  rdb_skipnode_init(node, key);

  return node;
}

/*
 * SkipList
 */

void
rdb_skiplist_init(rdb_skiplist_t *list,
                  const rdb_comparator_t *cmp,
                  rdb_arena_t *arena) {
  int i;

  list->comparator = cmp;
  list->arena = arena;
  list->head = rdb_skipnode_create(list, NULL, RDB_MAX_HEIGHT);
  list->max_height = 1;

  rdb_rand_init(&list->rnd, 0xdeadbeef);

  for (i = 0; i < RDB_MAX_HEIGHT; i++)
    rdb_skipnode_setnext(list->head, i, NULL);
}

static int
rdb_skiplist_maxheight(const rdb_skiplist_t *list) {
  return rdb_atomic_load(&list->max_height, rdb_order_relaxed);
}

/* MemTable::KeyComparator::operator() */
static int
rdb_skiplist_compare(const rdb_skiplist_t *list,
                     const uint8_t *xp,
                     const uint8_t *yp) {
  /* Internal keys are encoded as length-prefixed strings. */
  rdb_slice_t x = rdb_slice_decode(xp);
  rdb_slice_t y = rdb_slice_decode(yp);

  return rdb_compare(list->comparator, &x, &y);
}

static int
rdb_skiplist_equal(const rdb_skiplist_t *list,
                   const uint8_t *xp,
                   const uint8_t *yp) {
  return rdb_skiplist_compare(list, xp, yp) == 0;
}

static int
rdb_skiplist_randheight(rdb_skiplist_t *list) {
  /* Increase height with probability 1 in 4. */
  int height = 1;

  while (height < RDB_MAX_HEIGHT && rdb_rand_one_in(&list->rnd, 4))
    height++;

  assert(height > 0);
  assert(height <= RDB_MAX_HEIGHT);

  return height;
}

/* Return true if key is greater than the data stored in "n". */
static int
rdb_skiplist_key_after_node(const rdb_skiplist_t *list,
                            const uint8_t *key,
                            rdb_skipnode_t *node) {
  /* A null node is considered infinite. */
  return (node != NULL) && (rdb_skiplist_compare(list, node->key, key) < 0);
}

/* Return the earliest node that comes at or after key.
 * Return nullptr if there is no such node.
 *
 * If prev is non-null, fills prev[level] with pointer to previous
 * node at "level" for every level in [0..max_height_-1].
 */
static rdb_skipnode_t *
rdb_skiplist_find_gte(const rdb_skiplist_t *list,
                      const uint8_t *key,
                      rdb_skipnode_t **prev) {
  int level = rdb_skiplist_maxheight(list) - 1;
  rdb_skipnode_t *x = list->head;

  for (;;) {
    rdb_skipnode_t *next = rdb_skipnode_next(x, level);

    if (rdb_skiplist_key_after_node(list, key, next)) {
      /* Keep searching in this list. */
      x = next;
    } else {
      if (prev != NULL)
        prev[level] = x;

      if (level == 0)
        return next;

      /* Switch to next list. */
      level--;
    }
  }
}

/* Return the latest node with a key < key. */
/* Return head if there is no such node. */
static rdb_skipnode_t *
rdb_skiplist_find_lt(const rdb_skiplist_t *list, const uint8_t *key) {
  int level = rdb_skiplist_maxheight(list) - 1;
  rdb_skipnode_t *x = list->head;
  rdb_skipnode_t *next;

  for (;;) {
    assert(x == list->head || rdb_skiplist_compare(list, x->key, key) < 0);

    next = rdb_skipnode_next(x, level);

    if (next == NULL || rdb_skiplist_compare(list, next->key, key) >= 0) {
      if (level == 0)
        return x;

      /* Switch to next list. */
      level--;
    } else {
      x = next;
    }
  }
}

/* Return the last node in the list. */
/* Return head if list is empty. */
static rdb_skipnode_t *
rdb_skiplist_find_last(const rdb_skiplist_t *list) {
  int level = rdb_skiplist_maxheight(list) - 1;
  rdb_skipnode_t *x = list->head;

  for (;;) {
    rdb_skipnode_t *next = rdb_skipnode_next(x, level);

    if (next == NULL) {
      if (level == 0)
        return x;

      /* Switch to next list. */
      level--;
    } else {
      x = next;
    }
  }
}

void
rdb_skiplist_insert(rdb_skiplist_t *list, const uint8_t *key) {
  rdb_skipnode_t *prev[RDB_MAX_HEIGHT];
  rdb_skipnode_t *x = rdb_skiplist_find_gte(list, key, prev);
  int i, height;

  /* Our data structure does not allow duplicate insertion. */
  assert(x == NULL || !rdb_skiplist_equal(list, key, x->key));

  height = rdb_skiplist_randheight(list);

  if (height > rdb_skiplist_maxheight(list)) {
    for (i = rdb_skiplist_maxheight(list); i < height; i++)
      prev[i] = list->head;

    /* It is ok to mutate max_height without any synchronization
       with concurrent readers. A concurrent reader that observes
       the new value of max_height will see either the old value of
       new level pointers from head (NULL), or a new value set in
       the loop below. In the former case the reader will
       immediately drop to the next level since NULL sorts after all
       keys. In the latter case the reader will use the new node. */
    rdb_atomic_store(&list->max_height, height, rdb_order_relaxed);
  }

  x = rdb_skipnode_create(list, key, height);

  for (i = 0; i < height; i++) {
    /* rdb_skipnode_setnext_nb() suffices since we will add a
       barrier when we publish a pointer to "x" in prev[i]. */
    rdb_skipnode_setnext_nb(x, i, rdb_skipnode_next_nb(prev[i], i));
    rdb_skipnode_setnext(prev[i], i, x);
  }
}

int
rdb_skiplist_contains(const rdb_skiplist_t *list, const uint8_t *key) {
  rdb_skipnode_t *x = rdb_skiplist_find_gte(list, key, NULL);

  if (x != NULL && rdb_skiplist_equal(list, key, x->key))
    return 1;

  return 0;
}

/*
 * SkipList::Iterator
 */

void
rdb_skipiter_init(rdb_skipiter_t *iter, const rdb_skiplist_t *list) {
  iter->list = list;
  iter->node = NULL;
}

int
rdb_skipiter_valid(const rdb_skipiter_t *iter) {
  return iter->node != NULL;
}

const uint8_t *
rdb_skipiter_key(const rdb_skipiter_t *iter) {
  assert(rdb_skipiter_valid(iter));
  return iter->node->key;
}

void
rdb_skipiter_next(rdb_skipiter_t *iter) {
  assert(rdb_skipiter_valid(iter));

  iter->node = rdb_skipnode_next(iter->node, 0);
}

void
rdb_skipiter_prev(rdb_skipiter_t *iter) {
  /* Instead of using explicit "prev" links, we just
     search for the last node that falls before key. */
  assert(rdb_skipiter_valid(iter));

  iter->node = rdb_skiplist_find_lt(iter->list, iter->node->key);

  if (iter->node == iter->list->head)
    iter->node = NULL;
}

void
rdb_skipiter_seek(rdb_skipiter_t *iter, const uint8_t *target) {
  iter->node = rdb_skiplist_find_gte(iter->list, target, NULL);
}

void
rdb_skipiter_seek_first(rdb_skipiter_t *iter) {
  iter->node = rdb_skipnode_next(iter->list->head, 0);
}

void
rdb_skipiter_seek_last(rdb_skipiter_t *iter) {
  iter->node = rdb_skiplist_find_last(iter->list);

  if (iter->node == iter->list->head)
    iter->node = NULL;
}
