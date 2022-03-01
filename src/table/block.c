/*!
 * block.c - block for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include "../util/buffer.h"
#include "../util/coding.h"
#include "../util/comparator.h"
#include "../util/internal.h"
#include "../util/slice.h"
#include "../util/status.h"

#include "block.h"
#include "format.h"
#include "iterator.h"

/*
 * Block
 */

rdb_block_t *
rdb_block_create(const rdb_blockcontents_t *contents) {
  rdb_block_t *block = rdb_malloc(sizeof(rdb_block_t));
  rdb_block_init(block, contents);
  return block;
}

void
rdb_block_destroy(rdb_block_t *block) {
  rdb_block_clear(block);
  rdb_free(block);
}

static uint32_t
rdb_block_restarts(const rdb_block_t *block) {
  assert(block->size >= 4);
  return rdb_fixed32_decode(block->data + block->size - 4);
}

void
rdb_block_init(rdb_block_t *block, const rdb_blockcontents_t *contents) {
  block->data = contents->data.data;
  block->size = contents->data.size;
  block->restart_offset = 0;
  block->owned = contents->heap_allocated;

  if (block->size < 4) {
    block->size = 0; /* Error marker. */
  } else {
    size_t max_restarts_allowed = (block->size - 4) / 4;

    if (rdb_block_restarts(block) > max_restarts_allowed) {
      /* The size is too small for rdb_block_restarts(). */
      block->size = 0;
    } else {
      block->restart_offset = block->size - (1 + rdb_block_restarts(block)) * 4;
    }
  }
}

void
rdb_block_clear(rdb_block_t *block) {
  if (block->owned)
    rdb_free((void *)block->data);
}

/* Helper routine: decode the next block entry starting at "xp",
 * storing the number of shared key bytes, non_shared key bytes,
 * and the length of the value in "*shared", "*non_shared", and
 * "*value_length", respectively. Will not dereference past "limit".
 *
 * If any errors are detected, returns NULL. Otherwise, returns a
 * pointer to the key delta (just past the three decoded values).
 */
static const uint8_t *
rdb_decode_entry(uint32_t *shared,
                 uint32_t *non_shared,
                 uint32_t *value_length,
                 const uint8_t *xp,
                 const uint8_t *limit) {
  size_t xn;

  if (limit < xp)
    return NULL;

  xn = limit - xp;

  if (xn < 3)
    return NULL;

  *shared = xp[0];
  *non_shared = xp[1];
  *value_length = xp[2];

  if ((*shared | *non_shared | *value_length) < 128) {
    /* Fast path: all three values are encoded in one byte each. */
    xp += 3;
    xn -= 3;
  } else {
    if (!rdb_varint32_read(shared, &xp, &xn))
      return NULL;

    if (!rdb_varint32_read(non_shared, &xp, &xn))
      return NULL;

    if (!rdb_varint32_read(value_length, &xp, &xn))
      return NULL;
  }

  if (xn < (*non_shared + *value_length))
    return NULL;

  return xp;
}

/*
 * Block Iterator
 */

typedef struct rdb_blockiter_s {
  const rdb_comparator_t *comparator;
  const uint8_t *data;    /* Underlying block contents. */
  uint32_t restarts;      /* Offset of restart array (list of fixed32). */
  uint32_t num_restarts;  /* Number of uint32_t entries in restart array. */

  /* current is offset in data of current entry. >= restarts if !valid. */
  uint32_t current;
  uint32_t restart_index; /* Index of restart block in which current falls. */
  rdb_buffer_t key;
  rdb_slice_t value;
  int status;
} rdb_blockiter_t;

static int
rdb_blockiter_compare(const rdb_blockiter_t *iter,
                      const rdb_slice_t *x,
                      const rdb_slice_t *y) {
  return rdb_compare(iter->comparator, x, y);
}

/* Return the offset in iter->data just past the end of the current entry. */
static uint32_t
rdb_blockiter_next_entry_offset(const rdb_blockiter_t *iter) {
  return (iter->value.data + iter->value.size) - iter->data;
}

static uint32_t
rdb_blockiter_restart_point(const rdb_blockiter_t *iter, uint32_t index) {
  assert(index < iter->num_restarts);
  return rdb_fixed32_decode(iter->data + iter->restarts + index * 4);
}

static void
rdb_blockiter_seek_restart(rdb_blockiter_t *iter, uint32_t index) {
  uint32_t offset;

  rdb_buffer_reset(&iter->key);

  iter->restart_index = index;

  /* iter->current will be fixed by parse_next_key() */

  /* parse_next_key() starts at the end of iter->value,
     so set iter->value accordingly */
  offset = rdb_blockiter_restart_point(iter, index);

  rdb_slice_set(&iter->value, iter->data + offset, 0);
}

static void
rdb_blockiter_init(rdb_blockiter_t *iter,
                   const rdb_comparator_t *comparator,
                   const uint8_t *data,
                   uint32_t restarts,
                   uint32_t num_restarts) {
  assert(num_restarts > 0);

  iter->comparator = comparator;
  iter->data = data;
  iter->restarts = restarts;
  iter->num_restarts = num_restarts;
  iter->current = iter->restarts;
  iter->restart_index = iter->num_restarts;

  rdb_buffer_init(&iter->key);
  rdb_slice_init(&iter->value);

  iter->status = RDB_OK;
}

static void
rdb_blockiter_corruption(rdb_blockiter_t *iter) {
  iter->current = iter->restarts;
  iter->restart_index = iter->num_restarts;
  iter->status = RDB_CORRUPTION; /* "bad entry in block" */

  rdb_buffer_reset(&iter->key);
  rdb_slice_reset(&iter->value);
}

static void
rdb_blockiter_clear(rdb_blockiter_t *iter) {
  rdb_buffer_clear(&iter->key);
}

static int
rdb_blockiter_valid(const rdb_blockiter_t *iter) {
  return iter->current < iter->restarts;
}

static int
rdb_blockiter_status(const rdb_blockiter_t *iter) {
  return iter->status;
}

static rdb_slice_t
rdb_blockiter_key(const rdb_blockiter_t *iter) {
  assert(rdb_blockiter_valid(iter));
  return iter->key;
}

static rdb_slice_t
rdb_blockiter_value(const rdb_blockiter_t *iter) {
  assert(rdb_blockiter_valid(iter));
  return iter->value;
}

static int
rdb_blockiter_parse_next_key(rdb_blockiter_t *iter);

static void
rdb_blockiter_next(rdb_blockiter_t *iter) {
  assert(rdb_blockiter_valid(iter));

  rdb_blockiter_parse_next_key(iter);
}

static void
rdb_blockiter_prev(rdb_blockiter_t *iter) {
  uint32_t original = iter->current;

  assert(rdb_blockiter_valid(iter));

  /* Scan backwards to a restart point before iter->current. */
  while (rdb_blockiter_restart_point(iter, iter->restart_index) >= original) {
    if (iter->restart_index == 0) {
      /* No more entries. */
      iter->current = iter->restarts;
      iter->restart_index = iter->num_restarts;
      return;
    }

    iter->restart_index--;
  }

  rdb_blockiter_seek_restart(iter, iter->restart_index);

  do {
    /* Loop until end of current entry hits the start of original entry. */
  } while (rdb_blockiter_parse_next_key(iter)
        && rdb_blockiter_next_entry_offset(iter) < original);
}

static void
rdb_blockiter_seek(rdb_blockiter_t *iter, const rdb_slice_t *target) {
  /* Binary search in restart array to find the
     last restart point with a key < target. */
  uint32_t left = 0;
  uint32_t right = iter->num_restarts - 1;
  int current_key_compare = 0;
  int skip_seek;

  if (rdb_blockiter_valid(iter)) {
    /* If we're already scanning, use the current position as a starting
       point. This is beneficial if the key we're seeking to is ahead of the
       current position. */
    current_key_compare = rdb_blockiter_compare(iter, &iter->key, target);

    if (current_key_compare < 0) {
      /* iter->key is smaller than target. */
      left = iter->restart_index;
    } else if (current_key_compare > 0) {
      right = iter->restart_index;
    } else {
      /* We're seeking to the key we're already at. */
      return;
    }
  }

  while (left < right) {
    uint32_t mid = (left + right + 1) / 2;
    uint32_t region_offset = rdb_blockiter_restart_point(iter, mid);
    uint32_t shared, non_shared, value_length;
    const uint8_t *key_ptr = rdb_decode_entry(&shared,
                                              &non_shared,
                                              &value_length,
                                              iter->data + region_offset,
                                              iter->data + iter->restarts);
    rdb_slice_t mid_key;

    if (key_ptr == NULL || (shared != 0)) {
      rdb_blockiter_corruption(iter);
      return;
    }

    rdb_slice_set(&mid_key, key_ptr, non_shared);

    if (rdb_blockiter_compare(iter, &mid_key, target) < 0) {
      /* Key at "mid" is smaller than "target".  Therefore all
         blocks before "mid" are uninteresting. */
      left = mid;
    } else {
      /* Key at "mid" is >= "target".  Therefore all blocks at or
         after "mid" are uninteresting. */
      right = mid - 1;
    }
  }

  /* We might be able to use our current position within the restart block.
     This is true if we determined the key we desire is in the current block
     and is after than the current key. */
  assert(current_key_compare == 0 || rdb_blockiter_valid(iter));

  skip_seek = (left == iter->restart_index && current_key_compare < 0);

  if (!skip_seek)
    rdb_blockiter_seek_restart(iter, left);

  /* Linear search (within restart block) for first key >= target. */
  for (;;) {
    if (!rdb_blockiter_parse_next_key(iter))
      return;

    if (rdb_blockiter_compare(iter, &iter->key, target) >= 0)
      return;
  }
}

static void
rdb_blockiter_seek_first(rdb_blockiter_t *iter) {
  rdb_blockiter_seek_restart(iter, 0);
  rdb_blockiter_parse_next_key(iter);
}

static void
rdb_blockiter_seek_last(rdb_blockiter_t *iter) {
  rdb_blockiter_seek_restart(iter, iter->num_restarts - 1);

  while (rdb_blockiter_parse_next_key(iter)
      && rdb_blockiter_next_entry_offset(iter) < iter->restarts) {
    /* Keep skipping. */
  }
}

static int
rdb_blockiter_parse_next_key(rdb_blockiter_t *iter) {
  uint32_t shared, non_shared, value_length;
  const uint8_t *p, *limit;

  iter->current = rdb_blockiter_next_entry_offset(iter);

  p = iter->data + iter->current;
  limit = iter->data + iter->restarts; /* Restarts come right after data. */

  if (p >= limit) {
    /* No more entries to return. Mark as invalid. */
    iter->current = iter->restarts;
    iter->restart_index = iter->num_restarts;
    return 0;
  }

  /* Decode next entry. */
  p = rdb_decode_entry(&shared, &non_shared, &value_length, p, limit);

  if (p == NULL || iter->key.size < shared) {
    rdb_blockiter_corruption(iter);
    return 0;
  }

  rdb_buffer_resize(&iter->key, shared);
  rdb_buffer_append(&iter->key, p, non_shared);

  rdb_slice_set(&iter->value, p + non_shared, value_length);

  while (iter->restart_index + 1 < iter->num_restarts
      && rdb_blockiter_restart_point(iter, iter->restart_index + 1) < iter->current) {
    ++iter->restart_index;
  }

  return 1;
}

RDB_ITERATOR_FUNCTIONS(rdb_blockiter);

rdb_iter_t *
rdb_blockiter_create(const rdb_block_t *block,
                     const rdb_comparator_t *comparator) {
  rdb_blockiter_t *iter;
  uint32_t num_restarts;

  if (block->size < 4)
    return rdb_emptyiter_create(RDB_CORRUPTION); /* "bad block contents" */

  num_restarts = rdb_block_restarts(block);

  if (num_restarts == 0)
    return rdb_emptyiter_create(RDB_OK);

  iter = rdb_malloc(sizeof(rdb_blockiter_t));

  rdb_blockiter_init(iter,
                     comparator,
                     block->data,
                     block->restart_offset,
                     num_restarts);

  return rdb_iter_create(iter, &rdb_blockiter_table);
}
