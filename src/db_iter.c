/*!
 * db_iter.c - database iterator for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

#include "table/iterator.h"

#include "util/buffer.h"
#include "util/comparator.h"
#include "util/internal.h"
#include "util/random.h"
#include "util/slice.h"
#include "util/status.h"

#include "db_impl.h"
#include "db_iter.h"
#include "dbformat.h"

/*
 * Constants
 */

/* Which direction is the iterator currently moving?
 *
 * (1) When moving forward, the internal iterator is positioned at
 *     the exact entry that yields this->key(), this->value()
 *
 * (2) When moving backwards, the internal iterator is positioned
 *     just before all entries whose user key == this->key().
 */
enum rdb_direction { RDB_FORWARD, RDB_REVERSE };

/*
 * Types
 */

/* Memtables and sstables that make the DB representation contain
   (userkey,seq,type) => uservalue entries. DBIter
   combines multiple entries for the same userkey found in the DB
   representation into a single entry while accounting for sequence
   numbers, deletion markers, overwrites, etc. */
typedef struct rdb_dbiter_s {
  rdb_t *db;
  const rdb_comparator_t *ucmp;
  rdb_iter_t *iter;
  rdb_seqnum_t sequence;
  int status;
  rdb_buffer_t saved_key;   /* == current key when direction==RDB_REVERSE */
  rdb_buffer_t saved_value; /* == current value when direction==RDB_REVERSE */
  enum rdb_direction direction;
  int valid;
  rdb_rand_t rnd;
  size_t bytes_until_read_sampling;
} rdb_dbiter_t;

/*
 * Helpers
 */

/* Picks the number of bytes that can be
   read until a compaction is scheduled. */
static size_t
random_compaction_period(rdb_dbiter_t *iter) {
  return rdb_rand_uniform(&iter->rnd, 2 * RDB_READ_BYTES_PERIOD);
}

static RDB_INLINE int
parse_key(rdb_dbiter_t *iter, rdb_pkey_t *ikey) {
  rdb_slice_t k = rdb_iter_key(iter->iter);
  rdb_slice_t v = rdb_iter_value(iter->iter);
  size_t bytes_read = k.size + v.size;

  while (iter->bytes_until_read_sampling < bytes_read) {
    iter->bytes_until_read_sampling += random_compaction_period(iter);
    rdb_record_read_sample(iter->db, &k);
  }

  assert(iter->bytes_until_read_sampling >= bytes_read);

  iter->bytes_until_read_sampling -= bytes_read;

  if (!rdb_pkey_import(ikey, &k)) {
    iter->status = RDB_CORRUPTION; /* "corrupted internal key in DBIter" */
    return 0;
  }

  return 1;
}

static RDB_INLINE void
clear_saved_value(rdb_dbiter_t *iter) {
  if (iter->saved_value.alloc > 1048576) {
    rdb_buffer_clear(&iter->saved_value);
    rdb_buffer_init(&iter->saved_value);
  } else {
    rdb_buffer_reset(&iter->saved_value);
  }
}

static void
find_next_user_entry(rdb_dbiter_t *iter, int skipping, rdb_buffer_t *skip) {
  /* Loop until we hit an acceptable entry to yield. */
  assert(rdb_iter_valid(iter->iter));
  assert(iter->direction == RDB_FORWARD);

  do {
    rdb_pkey_t ikey;

    if (parse_key(iter, &ikey) && ikey.sequence <= iter->sequence) {
      switch (ikey.type) {
        case RDB_TYPE_DELETION:
          /* Arrange to skip all upcoming entries for this key since
             they are hidden by this deletion. */
          rdb_buffer_copy(skip, &ikey.user_key);
          skipping = 1;
          break;
        case RDB_TYPE_VALUE:
          if (skipping && rdb_compare(iter->ucmp, &ikey.user_key, skip) <= 0) {
            /* Entry hidden. */
          } else {
            iter->valid = 1;
            rdb_buffer_reset(&iter->saved_key);
            return;
          }
          break;
      }
    }

    rdb_iter_next(iter->iter);
  } while (rdb_iter_valid(iter->iter));

  rdb_buffer_reset(&iter->saved_key);

  iter->valid = 0;
}

static void
find_prev_user_entry(rdb_dbiter_t *iter) {
  rdb_valtype_t value_type = RDB_TYPE_DELETION;

  assert(iter->direction == RDB_REVERSE);

  if (rdb_iter_valid(iter->iter)) {
    do {
      rdb_pkey_t ikey;

      if (parse_key(iter, &ikey) && ikey.sequence <= iter->sequence) {
        if ((value_type != RDB_TYPE_DELETION) &&
            rdb_compare(iter->ucmp, &ikey.user_key, &iter->saved_key) < 0) {
          /* We encountered a non-deleted value in entries for previous keys. */
          break;
        }

        value_type = ikey.type;

        if (value_type == RDB_TYPE_DELETION) {
          rdb_buffer_reset(&iter->saved_key);
          clear_saved_value(iter);
        } else {
          rdb_slice_t key = rdb_iter_key(iter->iter);
          rdb_slice_t ukey = rdb_extract_user_key(&key);
          rdb_slice_t value = rdb_iter_value(iter->iter);

          if (iter->saved_value.alloc > value.size + 1048576) {
            rdb_buffer_clear(&iter->saved_value);
            rdb_buffer_init(&iter->saved_value);
          }

          rdb_buffer_copy(&iter->saved_key, &ukey);

          rdb_buffer_copy(&iter->saved_value, &value);
        }
      }

      rdb_iter_prev(iter->iter);
    } while (rdb_iter_valid(iter->iter));
  }

  if (value_type == RDB_TYPE_DELETION) {
    /* End. */
    iter->valid = 0;
    rdb_buffer_reset(&iter->saved_key);
    clear_saved_value(iter);
    iter->direction = RDB_FORWARD;
  } else {
    iter->valid = 1;
  }
}

/*
 * DBIter
 */

static void
rdb_dbiter_init(rdb_dbiter_t *iter,
                rdb_t *db,
                const rdb_comparator_t *ucmp,
                rdb_iter_t *internal_iter,
                rdb_seqnum_t sequence,
                uint32_t seed) {
  iter->db = db;
  iter->ucmp = ucmp;
  iter->iter = internal_iter;
  iter->sequence = sequence;
  iter->status = RDB_OK;

  rdb_buffer_init(&iter->saved_key);
  rdb_buffer_init(&iter->saved_value);

  iter->direction = RDB_FORWARD;
  iter->valid = 0;

  rdb_rand_init(&iter->rnd, seed);

  iter->bytes_until_read_sampling = random_compaction_period(iter);
}

static void
rdb_dbiter_clear(rdb_dbiter_t *iter) {
  rdb_iter_destroy(iter->iter);
  rdb_buffer_clear(&iter->saved_key);
  rdb_buffer_clear(&iter->saved_value);
}

static int
rdb_dbiter_valid(const rdb_dbiter_t *iter) {
  return iter->valid;
}

static rdb_slice_t
rdb_dbiter_key(const rdb_dbiter_t *iter) {
  rdb_slice_t key = rdb_iter_key(iter->iter);
  assert(iter->valid);
  return (iter->direction == RDB_FORWARD) ? rdb_extract_user_key(&key)
                                          : iter->saved_key;
}

static rdb_slice_t
rdb_dbiter_value(const rdb_dbiter_t *iter) {
  assert(iter->valid);
  return (iter->direction == RDB_FORWARD) ? rdb_iter_value(iter->iter)
                                          : iter->saved_value;
}

static int
rdb_dbiter_status(const rdb_dbiter_t *iter) {
  if (iter->status == RDB_OK)
    return rdb_iter_status(iter->iter);

  return iter->status;
}

static void
rdb_dbiter_next(rdb_dbiter_t *iter) {
  assert(iter->valid);

  if (iter->direction == RDB_REVERSE) { /* Switch directions? */
    iter->direction = RDB_FORWARD;
    /* iter->iter is pointing just before the entries for key(),
       so advance into the range of entries for key() and then
       use the normal skipping code below. */
    if (!rdb_iter_valid(iter->iter))
      rdb_iter_seek_first(iter->iter);
    else
      rdb_iter_next(iter->iter);

    if (!rdb_iter_valid(iter->iter)) {
      iter->valid = 0;
      rdb_buffer_reset(&iter->saved_key);
      return;
    }

    /* iter->saved_key already contains the key to skip past. */
  } else {
    /* Store in iter->saved_key the current key so we skip it below. */
    rdb_slice_t key = rdb_iter_key(iter->iter);
    rdb_slice_t ukey = rdb_extract_user_key(&key);

    rdb_buffer_copy(&iter->saved_key, &ukey);

    /* iter->iter is pointing to current key. We can now
       safely move to the next to avoid checking current key. */
    rdb_iter_next(iter->iter);

    if (!rdb_iter_valid(iter->iter)) {
      iter->valid = 0;
      rdb_buffer_reset(&iter->saved_key);
      return;
    }
  }

  find_next_user_entry(iter, 1, &iter->saved_key);
}

static void
rdb_dbiter_prev(rdb_dbiter_t *iter) {
  assert(iter->valid);

  if (iter->direction == RDB_FORWARD) { /* Switch directions? */
    /* iter->iter is pointing at the current entry. Scan backwards until
       the key changes so we can use the normal reverse scanning code. */
    rdb_slice_t key = rdb_iter_key(iter->iter);
    rdb_slice_t ukey = rdb_extract_user_key(&key);

    assert(rdb_iter_valid(iter->iter)); /* Otherwise iter->valid
                                           would have been false. */

    rdb_buffer_copy(&iter->saved_key, &ukey);

    for (;;) {
      rdb_iter_prev(iter->iter);

      if (!rdb_iter_valid(iter->iter)) {
        iter->valid = 0;
        rdb_buffer_reset(&iter->saved_key);
        clear_saved_value(iter);
        return;
      }

      key = rdb_iter_key(iter->iter);
      ukey = rdb_extract_user_key(&key);

      if (rdb_compare(iter->ucmp, &ukey, &iter->saved_key) < 0)
        break;
    }

    iter->direction = RDB_REVERSE;
  }

  find_prev_user_entry(iter);
}

static void
rdb_dbiter_seek(rdb_dbiter_t *iter, const rdb_slice_t *target) {
  rdb_pkey_t pkey;

  iter->direction = RDB_FORWARD;

  clear_saved_value(iter);

  rdb_buffer_reset(&iter->saved_key);

  rdb_pkey_init(&pkey, target, iter->sequence, RDB_VALTYPE_SEEK);
  rdb_pkey_export(&iter->saved_key, &pkey);

  rdb_iter_seek(iter->iter, &iter->saved_key);

  if (rdb_iter_valid(iter->iter))
    find_next_user_entry(iter, 0, &iter->saved_key);
  else
    iter->valid = 0;
}

static void
rdb_dbiter_seek_first(rdb_dbiter_t *iter) {
  iter->direction = RDB_FORWARD;

  clear_saved_value(iter);

  rdb_iter_seek_first(iter->iter);

  if (rdb_iter_valid(iter->iter))
    find_next_user_entry(iter, 0, &iter->saved_key);
  else
    iter->valid = 0;
}

static void
rdb_dbiter_seek_last(rdb_dbiter_t *iter) {
  iter->direction = RDB_REVERSE;

  clear_saved_value(iter);

  rdb_iter_seek_last(iter->iter);

  find_prev_user_entry(iter);
}

RDB_ITERATOR_FUNCTIONS(rdb_dbiter);

rdb_iter_t *
rdb_dbiter_create(rdb_t *db,
                  const rdb_comparator_t *user_comparator,
                  rdb_iter_t *internal_iter,
                  rdb_seqnum_t sequence,
                  uint32_t seed) {
  rdb_dbiter_t *iter = rdb_malloc(sizeof(rdb_dbiter_t));
  rdb_dbiter_init(iter, db, user_comparator, internal_iter, sequence, seed);
  return rdb_iter_create(iter, &rdb_dbiter_table);
}
