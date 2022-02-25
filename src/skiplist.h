/*!
 * skiplist.h - skiplist for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_SKIPLIST_H
#define RDB_SKIPLIST_H

#include <stdint.h>

#include "util/atomic.h"
#include "util/random.h"

/*
 * Types
 */

struct rdb_arena_s;
struct rdb_comparator_s;

typedef struct rdb_skipnode_s {
  const uint8_t *key;
  /* Array of length equal to the node height.
     next[0] is lowest level link. */
  rdb_atomic_ptr(struct rdb_skipnode_s) next[1];
} rdb_skipnode_t;

typedef struct rdb_skiplist_s {
  /* Immutable after construction. */
  const struct rdb_comparator_s *comparator;
  struct rdb_arena_s *arena;
  rdb_skipnode_t *head;

  /* Modified only by insert(). Read racily by readers, but stale
     values are ok. */
  rdb_atomic(int) max_height; /* Height of the entire list. */

  /* Read/written only by insert(). */
  rdb_rand_t rnd;
} rdb_skiplist_t;

typedef struct rdb_skipiter_s {
  const rdb_skiplist_t *list;
  rdb_skipnode_t *node;
} rdb_skipiter_t;

/*
 * SkipList
 */

/* Create a new SkipList object that will use "cmp" for comparing keys,
 * and will allocate memory using "*arena". Objects allocated in the arena
 * must remain allocated for the lifetime of the skiplist object.
 */
void
rdb_skiplist_init(rdb_skiplist_t *list,
                  const struct rdb_comparator_s *cmp,
                  struct rdb_arena_s *arena);

/* Insert key into the list. */
/* REQUIRES: nothing that compares equal to key is currently in the list. */
void
rdb_skiplist_insert(rdb_skiplist_t *list, const uint8_t *key);

/* Returns true iff an entry that compares equal to key is in the list. */
int
rdb_skiplist_contains(const rdb_skiplist_t *list, const uint8_t *key);

/*
 * SkipList::Iterator
 */

/* Initialize an iterator over the specified list. */
/* The returned iterator is not valid. */
void
rdb_skipiter_init(rdb_skipiter_t *iter, const rdb_skiplist_t *list);

/* Returns true iff the iterator is positioned at a valid node. */
int
rdb_skipiter_valid(const rdb_skipiter_t *iter);

/* Returns the key at the current position. */
/* REQUIRES: valid() */
const uint8_t *
rdb_skipiter_key(const rdb_skipiter_t *iter);

/* Advances to the next position. */
/* REQUIRES: valid() */
void
rdb_skipiter_next(rdb_skipiter_t *iter);

/* Advances to the previous position. */
/* REQUIRES: valid() */
void
rdb_skipiter_prev(rdb_skipiter_t *iter);

/* Advance to the first entry with a key >= target */
void
rdb_skipiter_seek(rdb_skipiter_t *iter, const uint8_t *target);

/* Position at the first entry in list. */
/* Final state of iterator is valid() iff list is not empty. */
void
rdb_skipiter_seek_first(rdb_skipiter_t *iter);

/* Position at the last entry in list. */
/* Final state of iterator is valid() iff list is not empty. */
void
rdb_skipiter_seek_last(rdb_skipiter_t *iter);

#endif /* RDB_SKIPLIST_H */
