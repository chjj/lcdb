/*!
 * write_batch.h - write batch for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_WRITE_BATCH_H
#define RDB_WRITE_BATCH_H

#include <stddef.h>
#include <stdint.h>

#include "util/extern.h"
#include "util/types.h"

/* Batch holds a collection of updates to apply atomically to a DB.
 *
 * The updates are applied in the order in which they are added
 * to the batch. For example, the value of "key" will be "v3"
 * after the following batch is written:
 *
 *    batch.Put("key", "v1");
 *    batch.Delete("key");
 *    batch.Put("key", "v2");
 *    batch.Put("key", "v3");
 *
 * Multiple threads can invoke const methods on a batch without
 * external synchronization, but if any of the threads may call a
 * non-const method, all threads accessing the same batch must use
 * external synchronization.
 */

/*
 * Types
 */

struct rdb_memtable_s;

typedef uint64_t rdb__seqnum_t;

typedef struct rdb_handler_s {
  void *state;
  uint64_t number;

  void (*put)(struct rdb_handler_s *handler,
              const rdb_slice_t *key,
              const rdb_slice_t *value);

  void (*del)(struct rdb_handler_s *handler,
              const rdb_slice_t *key);
} rdb_handler_t;

typedef struct rdb_batch_s {
  rdb_buffer_t rep; /* See comment in write_batch.c for the format of rep. */
} rdb_batch_t;

/*
 * Batch
 */

RDB_EXTERN rdb_batch_t *
rdb_batch_create(void);

RDB_EXTERN void
rdb_batch_destroy(rdb_batch_t *batch);

RDB_EXTERN void
rdb_batch_init(rdb_batch_t *batch);

RDB_EXTERN void
rdb_batch_clear(rdb_batch_t *batch);

/* Clear all updates buffered in this batch. */
RDB_EXTERN void
rdb_batch_reset(rdb_batch_t *batch);

/* The size of the database changes caused by this batch.
 *
 * This number is tied to implementation details, and may change across
 * releases. It is intended for usage metrics.
 */
size_t
rdb_batch_approximate_size(const rdb_batch_t *batch);

/* Support for iterating over the contents of a batch. */
RDB_EXTERN int
rdb_batch_iterate(const rdb_batch_t *batch, rdb_handler_t *handler);

/* Return the number of entries in the batch. */
int
rdb_batch_count(const rdb_batch_t *batch);

/* Set the count for the number of entries in the batch. */
void
rdb_batch_set_count(rdb_batch_t *batch, int count);

/* Return the sequence number for the start of this batch. */
rdb__seqnum_t
rdb_batch_sequence(const rdb_batch_t *batch);

/* Store the specified number as the sequence number for the start of
   this batch. */
void
rdb_batch_set_sequence(rdb_batch_t *batch, rdb__seqnum_t seq);

/* Store the mapping "key->value" in the database. */
RDB_EXTERN void
rdb_batch_put(rdb_batch_t *batch,
              const rdb_slice_t *key,
              const rdb_slice_t *value);

/* If the database contains a mapping for "key", erase it. Else do nothing. */
RDB_EXTERN void
rdb_batch_del(rdb_batch_t *batch, const rdb_slice_t *key);

/* Copies the operations in "src" to this batch.
 *
 * This runs in O(source size) time. However, the constant factor is better
 * than calling iterate() over the source batch with a Handler that replicates
 * the operations into this batch.
 */
RDB_EXTERN void
rdb_batch_append(rdb_batch_t *dst, const rdb_batch_t *src);

int
rdb_batch_insert_into(const rdb_batch_t *batch, struct rdb_memtable_s *table);

void
rdb_batch_set_contents(rdb_batch_t *batch, const rdb_slice_t *contents);

rdb_slice_t
rdb_batch_contents(const rdb_batch_t *batch);

size_t
rdb_batch_size(const rdb_batch_t *batch);

#endif /* RDB_WRITE_BATCH_H */
