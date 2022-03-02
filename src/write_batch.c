/*!
 * write_batch.c - write batch for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "util/buffer.h"
#include "util/coding.h"
#include "util/internal.h"
#include "util/slice.h"
#include "util/status.h"

#include "dbformat.h"
#include "memtable.h"
#include "write_batch.h"

/* rdb_batch_t::rep :=
 *    sequence: fixed64
 *    count: fixed32
 *    data: record[count]
 * record :=
 *    RDB_TYPE_VALUE varstring varstring |
 *    RDB_TYPE_DELETION varstring
 * varstring :=
 *    len: varint32
 *    data: uint8[len]
 */

/*
 * Constants
 */

/* Header has an 8-byte sequence number followed by a 4-byte count. */
#define RDB_HEADER 12

/*
 * Batch
 */

rdb_batch_t *
rdb_batch_create(void) {
  rdb_batch_t *batch = rdb_malloc(sizeof(rdb_batch_t));
  rdb_batch_init(batch);
  return batch;
}

void
rdb_batch_destroy(rdb_batch_t *batch) {
  rdb_batch_clear(batch);
  rdb_free(batch);
}

void
rdb_batch_init(rdb_batch_t *batch) {
  rdb_buffer_init(&batch->rep);
  rdb_batch_reset(batch);
}

void
rdb_batch_clear(rdb_batch_t *batch) {
  rdb_buffer_clear(&batch->rep);
}

void
rdb_batch_reset(rdb_batch_t *batch) {
  rdb_buffer_resize(&batch->rep, RDB_HEADER);

  memset(batch->rep.data, 0, RDB_HEADER);
}

size_t
rdb_batch_approximate_size(const rdb_batch_t *batch) {
  return batch->rep.size;
}

int
rdb_batch_iterate(const rdb_batch_t *batch, rdb_handler_t *handler) {
  rdb_slice_t input = batch->rep;
  rdb_slice_t key, value;
  int found = 0;

  if (input.size < RDB_HEADER)
    return RDB_CORRUPTION; /* "malformed WriteBatch (too small)" */

  rdb_slice_eat(&input, RDB_HEADER);

  while (input.size > 0) {
    int tag = input.data[0];

    rdb_slice_eat(&input, 1);

    found++;

    switch (tag) {
      case RDB_TYPE_VALUE: {
        if (!rdb_slice_slurp(&key, &input))
          return RDB_CORRUPTION; /* "bad WriteBatch Put" */

        if (!rdb_slice_slurp(&value, &input))
          return RDB_CORRUPTION; /* "bad WriteBatch Put" */

        handler->put(handler, &key, &value);

        break;
      }

      case RDB_TYPE_DELETION: {
        if (!rdb_slice_slurp(&key, &input))
          return RDB_CORRUPTION; /* "bad WriteBatch Delete" */

        handler->del(handler, &key);

        break;
      }

      default: {
        return RDB_CORRUPTION; /* "unknown WriteBatch tag" */
      }
    }
  }

  if (found != rdb_batch_count(batch))
    return RDB_CORRUPTION; /* "WriteBatch has wrong count" */

  return RDB_OK;
}

int
rdb_batch_count(const rdb_batch_t *batch) {
  return rdb_fixed32_decode(batch->rep.data + 8);
}

void
rdb_batch_set_count(rdb_batch_t *batch, int count) {
  rdb_fixed32_write(batch->rep.data + 8, count);
}

rdb_seqnum_t
rdb_batch_sequence(const rdb_batch_t *batch) {
  return rdb_fixed64_decode(batch->rep.data);
}

void
rdb_batch_set_sequence(rdb_batch_t *batch, rdb_seqnum_t seq) {
  rdb_fixed64_write(batch->rep.data, seq);
}

void
rdb_batch_put(rdb_batch_t *batch,
              const rdb_slice_t *key,
              const rdb_slice_t *value) {
  rdb_batch_set_count(batch, rdb_batch_count(batch) + 1);
  rdb_buffer_push(&batch->rep, RDB_TYPE_VALUE);
  rdb_slice_export(&batch->rep, key);
  rdb_slice_export(&batch->rep, value);
}

void
rdb_batch_del(rdb_batch_t *batch, const rdb_slice_t *key) {
  rdb_batch_set_count(batch, rdb_batch_count(batch) + 1);
  rdb_buffer_push(&batch->rep, RDB_TYPE_DELETION);
  rdb_slice_export(&batch->rep, key);
}

void
rdb_batch_append(rdb_batch_t *dst, const rdb_batch_t *src) {
  assert(src->rep.size >= RDB_HEADER);

  rdb_batch_set_count(dst, rdb_batch_count(dst) + rdb_batch_count(src));

  rdb_buffer_append(&dst->rep, src->rep.data + RDB_HEADER,
                               src->rep.size - RDB_HEADER);
}

static void
memtable_put(rdb_handler_t *handler,
             const rdb_slice_t *key,
             const rdb_slice_t *value) {
  rdb_memtable_t *table = handler->state;
  rdb_seqnum_t seq = handler->number;

  rdb_memtable_add(table, seq, RDB_TYPE_VALUE, key, value);

  handler->number++;
}

static void
memtable_del(rdb_handler_t *handler, const rdb_slice_t *key) {
  static const rdb_slice_t value = {NULL, 0, 0};
  rdb_memtable_t *table = handler->state;
  rdb_seqnum_t seq = handler->number;

  rdb_memtable_add(table, seq, RDB_TYPE_DELETION, key, &value);

  handler->number++;
}

int
rdb_batch_insert_into(const rdb_batch_t *batch, rdb_memtable_t *table) {
  rdb_handler_t handler;

  handler.state = table;
  handler.number = rdb_batch_sequence(batch);
  handler.put = memtable_put;
  handler.del = memtable_del;

  return rdb_batch_iterate(batch, &handler);
}

void
rdb_batch_set_contents(rdb_batch_t *batch, const rdb_slice_t *contents) {
  assert(contents->size >= RDB_HEADER);

  rdb_buffer_copy(&batch->rep, contents);
}

rdb_slice_t
rdb_batch_contents(const rdb_batch_t *batch) {
  return batch->rep;
}

size_t
rdb_batch_size(const rdb_batch_t *batch) {
  return batch->rep.size;
}
