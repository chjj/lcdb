/*!
 * block_builder.c - block builder for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

/* BlockBuilder generates blocks where keys are prefix-compressed:
 *
 * When we store a key, we drop the prefix shared with the previous
 * string. This helps reduce the space requirement significantly.
 * Furthermore, once every K keys, we do not apply the prefix
 * compression and store the entire key. We call this a "restart
 * point". The tail end of the block stores the offsets of all of the
 * restart points, and can be used to do a binary search when looking
 * for a particular key. Values are stored as-is (without compression)
 * immediately following the corresponding key.
 *
 * An entry for a particular key-value pair has the form:
 *     shared_bytes: varint32
 *     unshared_bytes: varint32
 *     value_length: varint32
 *     key_delta: char[unshared_bytes]
 *     value: char[value_length]
 * shared_bytes == 0 for restart points.
 *
 * The trailer of the block has the form:
 *     restarts: uint32[num_restarts]
 *     num_restarts: uint32
 * restarts[i] contains the offset within the block of the ith restart point.
 */

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include "../util/array.h"
#include "../util/buffer.h"
#include "../util/comparator.h"
#include "../util/internal.h"
#include "../util/options.h"
#include "../util/slice.h"
#include "../util/vector.h"

#include "block_builder.h"

/*
 * Block Builder
 */

void
rdb_blockbuilder_init(rdb_blockbuilder_t *bb, const rdb_dbopt_t *options) {
  assert(options->block_restart_interval >= 1);

  bb->options = options;
  bb->counter = 0;
  bb->finished = 0;

  rdb_buffer_init(&bb->buffer);
  rdb_array_init(&bb->restarts);
  rdb_buffer_init(&bb->last_key);

  rdb_array_push(&bb->restarts, 0); /* First restart point is at offset 0. */
}

void
rdb_blockbuilder_clear(rdb_blockbuilder_t *bb) {
  rdb_buffer_clear(&bb->buffer);
  rdb_array_clear(&bb->restarts);
  rdb_buffer_clear(&bb->last_key);
}

void
rdb_blockbuilder_reset(rdb_blockbuilder_t *bb) {
  rdb_buffer_reset(&bb->buffer);
  rdb_array_reset(&bb->restarts);

  rdb_array_push(&bb->restarts, 0); /* First restart point is at offset 0. */

  bb->counter = 0;
  bb->finished = 0;

  rdb_buffer_reset(&bb->last_key);
}

void
rdb_blockbuilder_add(rdb_blockbuilder_t *bb,
                     const rdb_slice_t *key,
                     const rdb_slice_t *value) {
  rdb_slice_t last = bb->last_key;
  size_t shared, non_shared;

  assert(!bb->finished);
  assert(bb->counter <= bb->options->block_restart_interval);
  assert(rdb_blockbuilder_empty(bb) /* No values yet? */
         || bb->options->comparator->compare(key, &last) > 0);

  shared = 0;

  if (bb->counter < bb->options->block_restart_interval) {
    /* See how much sharing to do with previous string. */
    size_t min_length = RDB_MIN(last.size, key->size);

    while (shared < min_length && last.data[shared] == key->data[shared])
      shared++;
  } else {
    /* Restart compression. */
    rdb_array_push(&bb->restarts, bb->buffer.size);
    bb->counter = 0;
  }

  non_shared = key->size - shared;

  /* Add "<shared><non_shared><value_size>" to buffer. */
  rdb_buffer_varint32(&bb->buffer, shared);
  rdb_buffer_varint32(&bb->buffer, non_shared);
  rdb_buffer_varint32(&bb->buffer, value->size);

  /* Add string delta to buffer followed by value. */
  rdb_buffer_append(&bb->buffer, key->data + shared, non_shared);
  rdb_buffer_append(&bb->buffer, value->data, value->size);

  /* Update state. */
  rdb_buffer_resize(&bb->last_key, shared);
  rdb_buffer_append(&bb->last_key, key->data + shared, non_shared);
  assert(rdb_slice_equal(&bb->last_key, key));
  bb->counter++;
}

rdb_slice_t
rdb_blockbuilder_finish(rdb_blockbuilder_t *bb) {
  /* Append restart array. */
  size_t i;

  for (i = 0; i < bb->restarts.length; i++)
    rdb_buffer_fixed32(&bb->buffer, bb->restarts.items[i]);

  rdb_buffer_fixed32(&bb->buffer, bb->restarts.length);

  bb->finished = 1;

  return bb->buffer;
}

size_t
rdb_blockbuilder_size_estimate(const rdb_blockbuilder_t *bb) {
  return (bb->buffer.size                         /* Raw data buffer */
        + bb->restarts.length * sizeof(uint32_t)  /* Restart array */
        + sizeof(uint32_t));                      /* Restart array length */
}
