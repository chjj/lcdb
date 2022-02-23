/*!
 * block.h - block for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_BLOCK_H
#define RDB_BLOCK_H

#include <stddef.h>
#include <stdint.h>

/*
 * Types
 */

struct rdb_blockcontents_s;
struct rdb_comparator_s;
struct rdb_iter_s;

typedef struct rdb_block_s {
  const uint8_t *data;
  size_t size;
  uint32_t restart_offset;  /* Offset in data of restart array. */
  int owned;                /* Block owns data[]. */
} rdb_block_t;

/*
 * Block
 */

rdb_block_t *
rdb_block_create(const struct rdb_blockcontents_s *contents);

void
rdb_block_destroy(rdb_block_t *block);

void
rdb_block_init(rdb_block_t *block, const struct rdb_blockcontents_s *contents);

void
rdb_block_clear(rdb_block_t *block);

/*
 * Block Iterator
 */

struct rdb_iter_s *
rdb_blockiter_create(const rdb_block_t *block,
                     const struct rdb_comparator_s *comparator);

#endif /* RDB_BLOCK_H */
