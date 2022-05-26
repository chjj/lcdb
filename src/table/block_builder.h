/*!
 * block_builder.h - block builder for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on google/leveldb:
 *   Copyright (c) 2011, The LevelDB Authors. All rights reserved.
 *   https://github.com/google/leveldb
 *
 * See LICENSE for more information.
 */

#ifndef LDB_BLOCK_BUILDER_H
#define LDB_BLOCK_BUILDER_H

#include <stddef.h>
#include <stdint.h>

#include "../util/types.h"

/*
 * Types
 */

struct ldb_dbopt_s;

typedef struct ldb_blockgen_s {
  const ldb_dbopt_t *options;
  ldb_buffer_t buffer;          /* Destination buffer. */
  ldb_array_t restarts;         /* Restart points (uint32_t). */
  int counter;                  /* Number of entries emitted since restart. */
  int finished;                 /* Has finish() been called? */
  ldb_buffer_t last_key;
} ldb_blockgen_t;

/*
 * BlockBuilder
 */

void
ldb_blockgen_init(ldb_blockgen_t *bb,
                  const struct ldb_dbopt_s *options);

void
ldb_blockgen_clear(ldb_blockgen_t *bb);

/* Reset the contents as if the block builder was just constructed. */
void
ldb_blockgen_reset(ldb_blockgen_t *bb);

/* REQUIRES: finish() has not been called since the last call to reset(). */
/* REQUIRES: key is larger than any previously added key. */
void
ldb_blockgen_add(ldb_blockgen_t *bb,
                 const ldb_slice_t *key,
                 const ldb_slice_t *value);

/* Finish building the block and return a slice that refers to the
   block contents. The returned slice will remain valid for the
   lifetime of this builder or until reset() is called. */
ldb_slice_t
ldb_blockgen_finish(ldb_blockgen_t *bb);

/* Returns an estimate of the current (uncompressed) size of the block
   we are building. */
size_t
ldb_blockgen_size_estimate(const ldb_blockgen_t *bb);

/* Return true iff no entries have been added since the last reset(). */
#define ldb_blockgen_empty(bb) ((bb)->buffer.size == 0)

#endif /* LDB_BLOCK_BUILDER_H */
