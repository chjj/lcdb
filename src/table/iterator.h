/*!
 * iterator.h - iterator for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_ITERATOR_H
#define RDB_ITERATOR_H

#include <stddef.h>

#include "../util/types.h"

/*
 * Types
 */

/* Clients are allowed to register function/arg1/arg2 triples that
 * will be invoked when this iterator is destroyed.
 *
 * Note that unlike all of the preceding methods, this method is
 * not abstract and therefore clients should not override it.
 */
typedef void (*rdb_cleanup_f)(void *arg1, void *arg2);

/* Cleanup functions are stored in a single-linked list.
   The list's head node is inlined in the iterator. */
typedef struct rdb_cleanup_s {
  /* The head node is used if the function pointer is not null. */
  rdb_cleanup_f func;
  void *arg1;
  void *arg2;
  struct rdb_cleanup_s *next;
} rdb_cleanup_t;

typedef struct rdb_itertbl_s rdb_itertbl_t;

typedef struct rdb_iter_s {
  /* The underlying iterator. */
  void *ptr;

  /* Cleanup functions are stored in a single-linked list.
     The list's head node is inlined in the iterator. */
  rdb_cleanup_t cleanup_head;

  /* Iterator function table. */
  const rdb_itertbl_t *table;
} rdb_iter_t;

/*
 * Function Table
 */

struct rdb_itertbl_s {
  /* Clear iterator. */
  void (*clear)(void *iter);

  /* An iterator is either positioned at a key/value pair, or
     not valid. This method returns true iff the iterator is valid. */
  int (*valid)(const void *iter);

  /* Position at the first key in the source. The iterator is valid()
     after this call iff the source is not empty. */
  void (*seek_first)(void *iter);

  /* Position at the last key in the source. The iterator is
     valid() after this call iff the source is not empty. */
  void (*seek_last)(void *iter);

  /* Position at the first key in the source that is at or past target.
     The iterator is valid() after this call iff the source contains
     an entry that comes at or past target. */
  void (*seek)(void *iter, const rdb_slice_t *target);

  /* Moves to the next entry in the source. After this call, valid() is
     true iff the iterator was not positioned at the last entry in the source.
     REQUIRES: valid() */
  void (*next)(void *iter);

  /* Moves to the previous entry in the source. After this call, valid() is
     true iff the iterator was not positioned at the first entry in source.
     REQUIRES: valid() */
  void (*prev)(void *iter);

  /* Return the key for the current entry. The underlying storage for
     the returned slice is valid only until the next modification of
     the iterator.
     REQUIRES: valid() */
  rdb_slice_t (*key)(const void *iter);

  /* Return the value for the current entry. The underlying storage for
     the returned slice is valid only until the next modification of
     the iterator.
     REQUIRES: valid() */
  rdb_slice_t (*value)(const void *iter);

  /* If an error has occurred, return it. Else return an ok status. */
  int (*status)(const void *iter);
};

#define RDB_ITERATOR_FUNCTIONS(name)                           \
                                                               \
static void                                                    \
name ## _clear_wrapped(void *iter) {                           \
  name ## _clear((name ## _t *)iter);                          \
}                                                              \
                                                               \
static int                                                     \
name ## _valid_wrapped(const void *iter) {                     \
  return name ## _valid((const name ## _t *)iter);             \
}                                                              \
                                                               \
static void                                                    \
name ## _seek_wrapped(void *iter, const rdb_slice_t *target) { \
  name ## _seek((name ## _t *)iter, target);                   \
}                                                              \
                                                               \
static void                                                    \
name ## _seek_first_wrapped(void *iter) {                      \
  name ## _seek_first((name ## _t *)iter);                     \
}                                                              \
                                                               \
static void                                                    \
name ## _seek_last_wrapped(void *iter) {                       \
  name ## _seek_last((name ## _t *)iter);                      \
}                                                              \
                                                               \
static void                                                    \
name ## _next_wrapped(void *iter) {                            \
  name ## _next((name ## _t *)iter);                           \
}                                                              \
                                                               \
static void                                                    \
name ## _prev_wrapped(void *iter) {                            \
  name ## _prev((name ## _t *)iter);                           \
}                                                              \
                                                               \
static rdb_slice_t                                             \
name ## _key_wrapped(const void *iter) {                       \
  return name ## _key((const name ## _t *)iter);               \
}                                                              \
                                                               \
static rdb_slice_t                                             \
name ## _value_wrapped(const void *iter) {                     \
  return name ## _value((const name ## _t *)iter);             \
}                                                              \
                                                               \
static int                                                     \
name ## _status_wrapped(const void *iter) {                    \
  return name ## _status((const name ## _t *)iter);            \
}                                                              \
                                                               \
static const rdb_itertbl_t name ## _table = {                  \
  /* .clear = */ name ## _clear_wrapped,                       \
  /* .valid = */ name ## _valid_wrapped,                       \
  /* .seek_first = */ name ## _seek_first_wrapped,             \
  /* .seek_last = */ name ## _seek_last_wrapped,               \
  /* .seek = */ name ## _seek_wrapped,                         \
  /* .next = */ name ## _next_wrapped,                         \
  /* .prev = */ name ## _prev_wrapped,                         \
  /* .key = */ name ## _key_wrapped,                           \
  /* .value = */ name ## _value_wrapped,                       \
  /* .status = */ name ## _status_wrapped                      \
}

/*
 * Cleanup
 */

/* True if the node is not used. Only head nodes might be unused. */
#define rdb_cleanup_empty(x) ((x)->func == NULL)
/* Invokes the cleanup function. */
#define rdb_cleanup_run(x) (x)->func((x)->arg1, (x)->arg2)

/*
 * Iterator
 */

#define rdb_iter_valid(x) (x)->table->valid((x)->ptr)
#define rdb_iter_seek_first(x) (x)->table->seek_first((x)->ptr)
#define rdb_iter_seek_last(x) (x)->table->seek_last((x)->ptr)
#define rdb_iter_seek(x, y) (x)->table->seek((x)->ptr, y)
#define rdb_iter_next(x) (x)->table->next((x)->ptr)
#define rdb_iter_prev(x) (x)->table->prev((x)->ptr)
#define rdb_iter_key(x) (x)->table->key((x)->ptr)
#define rdb_iter_value(x) (x)->table->value((x)->ptr)
#define rdb_iter_status(x) (x)->table->status((x)->ptr)

rdb_iter_t *
rdb_iter_create(void *ptr, const rdb_itertbl_t *table);

void
rdb_iter_destroy(rdb_iter_t *iter);

void
rdb_iter_register_cleanup(rdb_iter_t *iter,
                          rdb_cleanup_f func,
                          void *arg1,
                          void *arg2);

/*
 * Empty Iterator
 */

/* Return an empty iterator with the specified status. */
rdb_iter_t *
rdb_emptyiter_create(int status);

#endif /* RDB_ITERATOR_H */
