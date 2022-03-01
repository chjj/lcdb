/*!
 * block_builder.h - block builder for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_BLOCK_BUILDER_H
#define RDB_BLOCK_BUILDER_H

#include <stddef.h>
#include <stdint.h>

#include "../util/types.h"

/*
 * Types
 */

struct rdb_dbopt_s;

typedef struct rdb_blockbuilder_s {
  const rdb_dbopt_t *options;
  rdb_buffer_t buffer;          /* Destination buffer. */
  rdb_array_t restarts;         /* Restart points (uint32_t). */
  int counter;                  /* Number of entries emitted since restart. */
  int finished;                 /* Has Finish() been called? */
  rdb_buffer_t last_key;
} rdb_blockbuilder_t;

/*
 * Block Builder
 */

void
rdb_blockbuilder_init(rdb_blockbuilder_t *bb,
                      const struct rdb_dbopt_s *options);

void
rdb_blockbuilder_clear(rdb_blockbuilder_t *bb);

/* Reset the contents as if the block builder was just constructed. */
void
rdb_blockbuilder_reset(rdb_blockbuilder_t *bb);

/* REQUIRES: finish() has not been called since the last call to reset(). */
/* REQUIRES: key is larger than any previously added key. */
void
rdb_blockbuilder_add(rdb_blockbuilder_t *bb,
                     const rdb_slice_t *key,
                     const rdb_slice_t *value);

/* Finish building the block and return a slice that refers to the
   block contents.  The returned slice will remain valid for the
   lifetime of this builder or until reset() is called. */
rdb_slice_t
rdb_blockbuilder_finish(rdb_blockbuilder_t *bb);

/* Returns an estimate of the current (uncompressed) size of the block
   we are building. */
size_t
rdb_blockbuilder_size_estimate(const rdb_blockbuilder_t *bb);

/* Return true iff no entries have been added since the last reset(). */
#define rdb_blockbuilder_empty(bb) ((bb)->buffer.size == 0)

#endif /* RDB_BLOCK_BUILDER_H */
