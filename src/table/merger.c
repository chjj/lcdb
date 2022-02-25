/*!
 * merger.c - merging iterator for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stddef.h>

#include "../util/comparator.h"
#include "../util/internal.h"
#include "../util/slice.h"
#include "../util/status.h"

#include "iterator.h"
#include "iterator_wrapper.h"
#include "merger.h"

/*
 * Constants
 */

enum rdb_direction {
  RDB_FORWARD,
  RDB_REVERSE
};

/*
 * Merging Iterator
 */

typedef struct rdb_mergeiter_s {
  /* We might want to use a heap in case there are lots of children.
     For now we use a simple array since we expect a very small number
     of children in leveldb. */
  const rdb_comparator_t *comparator;
  rdb_wrapiter_t *children;
  int n;
  rdb_wrapiter_t *current;
  enum rdb_direction direction;
} rdb_mergeiter_t;

static void
rdb_mergeiter_init(rdb_mergeiter_t *mi,
                   const rdb_comparator_t *comparator,
                   rdb_iter_t **children,
                   int n) {
  int i;

  mi->comparator = comparator;
  mi->children = rdb_malloc(n * sizeof(rdb_wrapiter_t));
  mi->n = n;
  mi->current = NULL;
  mi->direction = RDB_FORWARD;

  for (i = 0; i < n; i++)
    rdb_wrapiter_init(&mi->children[i], children[i]);
}

static void
rdb_mergeiter_clear(rdb_mergeiter_t *mi) {
  int i;

  for (i = 0; i < mi->n; i++)
    rdb_wrapiter_clear(&mi->children[i]);

  rdb_free(mi->children);
}

static int
rdb_mergeiter_valid(const rdb_mergeiter_t *mi) {
  return (mi->current != NULL);
}

static rdb_slice_t
rdb_mergeiter_key(const rdb_mergeiter_t *mi) {
  assert(rdb_mergeiter_valid(mi));
  return rdb_wrapiter_key(mi->current);
}

static rdb_slice_t
rdb_mergeiter_value(const rdb_mergeiter_t *mi) {
  assert(rdb_mergeiter_valid(mi));
  return rdb_wrapiter_value(mi->current);
}

static int
rdb_mergeiter_status(const rdb_mergeiter_t *mi) {
  int rc = RDB_OK;
  int i;

  for (i = 0; i < mi->n; i++) {
    if ((rc = rdb_wrapiter_status(&mi->children[i])))
      break;
  }

  return rc;
}

static void
rdb_mergeiter_find_smallest(rdb_mergeiter_t *mi) {
  rdb_wrapiter_t *smallest = NULL;
  int i;

  for (i = 0; i < mi->n; i++) {
    rdb_wrapiter_t *child = &mi->children[i];

    if (rdb_wrapiter_valid(child)) {
      if (smallest == NULL) {
        smallest = child;
      } else {
        rdb_slice_t child_key = rdb_wrapiter_key(child);
        rdb_slice_t smallest_key = rdb_wrapiter_key(smallest);

        if (rdb_compare(mi->comparator, &child_key, &smallest_key) < 0)
          smallest = child;
      }
    }
  }

  mi->current = smallest;
}

static void
rdb_mergeiter_find_largest(rdb_mergeiter_t *mi) {
  rdb_wrapiter_t *largest = NULL;
  int i;

  for (i = mi->n - 1; i >= 0; i--) {
    rdb_wrapiter_t *child = &mi->children[i];

    if (rdb_wrapiter_valid(child)) {
      if (largest == NULL) {
        largest = child;
      } else {
        rdb_slice_t child_key = rdb_wrapiter_key(child);
        rdb_slice_t largest_key = rdb_wrapiter_key(largest);

        if (rdb_compare(mi->comparator, &child_key, &largest_key) > 0)
          largest = child;
      }
    }
  }

  mi->current = largest;
}

static void
rdb_mergeiter_seek_first(rdb_mergeiter_t *mi) {
  int i;

  for (i = 0; i < mi->n; i++)
    rdb_wrapiter_seek_first(&mi->children[i]);

  rdb_mergeiter_find_smallest(mi);

  mi->direction = RDB_FORWARD;
}

static void
rdb_mergeiter_seek_last(rdb_mergeiter_t *mi) {
  int i;

  for (i = 0; i < mi->n; i++)
    rdb_wrapiter_seek_last(&mi->children[i]);

  rdb_mergeiter_find_largest(mi);

  mi->direction = RDB_REVERSE;
}

static void
rdb_mergeiter_seek(rdb_mergeiter_t *mi, const rdb_slice_t *target) {
  int i;

  for (i = 0; i < mi->n; i++)
    rdb_wrapiter_seek(&mi->children[i], target);

  rdb_mergeiter_find_smallest(mi);

  mi->direction = RDB_FORWARD;
}

static void
rdb_mergeiter_next(rdb_mergeiter_t *mi) {
  assert(rdb_mergeiter_valid(mi));

  /* Ensure that all children are positioned after key().
     If we are moving in the forward direction, it is already
     true for all of the non-current children since current is
     the smallest child and key() == current->key(). Otherwise,
     we explicitly position the non-current children. */
  if (mi->direction != RDB_FORWARD) {
    int i;

    for (i = 0; i < mi->n; i++) {
      rdb_wrapiter_t *child = &mi->children[i];

      if (child != mi->current) {
        rdb_slice_t mi_key = rdb_mergeiter_key(mi);

        rdb_wrapiter_seek(child, &mi_key);

        if (rdb_wrapiter_valid(child)) {
          rdb_slice_t child_key = rdb_wrapiter_key(child);

          if (rdb_compare(mi->comparator, &mi_key, &child_key) == 0)
            rdb_wrapiter_next(child);
        }
      }
    }

    mi->direction = RDB_FORWARD;
  }

  rdb_wrapiter_next(mi->current);
  rdb_mergeiter_find_smallest(mi);
}

static void
rdb_mergeiter_prev(rdb_mergeiter_t *mi) {
  assert(rdb_mergeiter_valid(mi));

  /* Ensure that all children are positioned before key().
     If we are moving in the reverse direction, it is already
     true for all of the non-current children since current is
     the largest child and key() == current->key(). Otherwise,
     we explicitly position the non-current children. */
  if (mi->direction != RDB_REVERSE) {
    int i;

    for (i = 0; i < mi->n; i++) {
      rdb_wrapiter_t *child = &mi->children[i];

      if (child != mi->current) {
        rdb_slice_t mi_key = rdb_mergeiter_key(mi);

        rdb_wrapiter_seek(child, &mi_key);

        if (rdb_wrapiter_valid(child)) {
          /* Child is at first entry >= key(). Step back one to be < key(). */
          rdb_wrapiter_prev(child);
        } else {
          /* Child has no entries >= key(). Position at last entry. */
          rdb_wrapiter_seek_last(child);
        }
      }
    }

    mi->direction = RDB_REVERSE;
  }

  rdb_wrapiter_prev(mi->current);
  rdb_mergeiter_find_largest(mi);
}

RDB_ITERATOR_FUNCTIONS(rdb_mergeiter);

rdb_iter_t *
rdb_mergeiter_create(const rdb_comparator_t *comparator,
                     rdb_iter_t **children,
                     int n) {
  rdb_mergeiter_t *iter;

  assert(n >= 0);

  if (n == 0)
    return rdb_emptyiter_create(RDB_OK);

  if (n == 1)
    return children[0];

  iter = rdb_malloc(sizeof(rdb_mergeiter_t));

  rdb_mergeiter_init(iter, comparator, children, n);

  return rdb_iter_create(iter, &rdb_mergeiter_table);
}
