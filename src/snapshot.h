/*!
 * snapshot.h - snapshots for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_SNAPSHOT_H
#define RDB_SNAPSHOT_H

#include <assert.h>
#include <stddef.h>

#include "util/internal.h"

#include "dbformat.h"

/*
 * Types
 */

struct rdb_snaplist_s;

/* Snapshots are kept in a doubly-linked list in the DB. */
/* Each snapshot corresponds to a particular sequence number. */
typedef struct rdb_snapshot_s {
  /* rdb_snapshot_t is kept in a doubly-linked circular list. The
     rdb_snaplist_t implementation operates on the next/previous
     fields directly. */
  rdb_seqnum_t sequence;
  struct rdb_snapshot_s *prev;
  struct rdb_snapshot_s *next;
#ifndef NDEBUG
  struct rdb_snaplist_s *list;
#endif
} rdb_snapshot_t;

typedef struct rdb_snaplist_s {
  /* Dummy head of doubly-linked list of snapshots. */
  rdb_snapshot_t head;
} rdb_snaplist_t;

/*
 * Snapshot List
 */

RDB_UNUSED static void
rdb_snaplist_init(rdb_snaplist_t *list) {
  list->head.sequence = 0;
  list->head.prev = &list->head;
  list->head.next = &list->head;
#ifndef NDEBUG
  list->head.list = NULL;
#endif
}

RDB_UNUSED static int
rdb_snaplist_empty(const rdb_snaplist_t *list) {
  return list->head.next == &list->head;
}

RDB_UNUSED static rdb_snapshot_t *
rdb_snaplist_oldest(const rdb_snaplist_t *list) {
  assert(!rdb_snaplist_empty(list));
  return list->head.next;
}

RDB_UNUSED static rdb_snapshot_t *
rdb_snaplist_newest(const rdb_snaplist_t *list) {
  assert(!rdb_snaplist_empty(list));
  return list->head.prev;
}

/* Creates a snapshot and appends it to the end of the list. */
RDB_UNUSED static rdb_snapshot_t *
rdb_snaplist_new(rdb_snaplist_t *list, rdb_seqnum_t sequence) {
  rdb_snapshot_t *snap;

  assert(rdb_snaplist_empty(list)
      || rdb_snaplist_newest(list)->sequence <= sequence);

  snap = rdb_malloc(sizeof(rdb_snapshot_t));

  snap->sequence = sequence;
  snap->next = &list->head;
  snap->prev = list->head.prev;
  snap->prev->next = snap;
  snap->next->prev = snap;

#ifndef NDEBUG
  snap->list = list;
#endif

  return snap;
}

/* Removes a snapshot from this list.
 *
 * The snapshot must have been created by calling rdb_snaplist_new
 * on this list.
 *
 * The snapshot pointer should not be const, because its memory is
 * deallocated. However, that would force us to change release_snapshot(),
 * which is in the API, and currently takes a const snapshot.
 */
RDB_UNUSED static void
rdb_snaplist_delete(rdb_snaplist_t *list, const rdb_snapshot_t *snap) {
#ifndef NDEBUG
  assert(snap->list == list);
#else
  (void)list;
#endif

  snap->prev->next = snap->next;
  snap->next->prev = snap->prev;

  rdb_free((void *)snap);
}

#endif /* RDB_SNAPSHOT_H */
