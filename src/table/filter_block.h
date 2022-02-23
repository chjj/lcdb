/*!
 * filter_block.h - filter block builder/reader for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_FILTER_BLOCK_H
#define RDB_FILTER_BLOCK_H

#include <stddef.h>
#include <stdint.h>

#include "../util/types.h"

/*
 * Types
 */

struct rdb_bloom_s;

/* A filter block builder is used to construct all of the filters for a
 * particular Table. It generates a single string which is stored as
 * a special block in the table.
 *
 * The sequence of calls to filter block builder must match the regexp:
 *     (start_block add_key*)* finish
 */
typedef struct rdb_filterbuilder_s {
  const rdb_bloom_t *policy;
  rdb_buffer_t keys;          /* Flattened key contents. */
  rdb_array_t start;          /* Starting index in keys of each key (size_t). */
  rdb_buffer_t result;        /* Filter data computed so far. */
  rdb_array_t filter_offsets; /* Filter offsets (uint32_t). */
} rdb_filterbuilder_t;

typedef struct rdb_filterreader_s {
  const struct rdb_bloom_s *policy;
  const uint8_t *data;    /* Pointer to filter data (at block-start). */
  const uint8_t *offset;  /* Pointer to beginning of offset array (at block-end). */
  size_t num;             /* Number of entries in offset array. */
  size_t base_lg;         /* Encoding parameter (see RDB_FILTER_BASE_LG in .c file). */
} rdb_filterreader_t;

/*
 * Filter Builder
 */

void
rdb_filterbuilder_init(rdb_filterbuilder_t *fb,
                       const struct rdb_bloom_s *policy);

void
rdb_filterbuilder_clear(rdb_filterbuilder_t *fb);

void
rdb_filterbuilder_start_block(rdb_filterbuilder_t *fb, uint64_t block_offset);

void
rdb_filterbuilder_add_key(rdb_filterbuilder_t *fb, const rdb_slice_t *key);

rdb_slice_t
rdb_filterbuilder_finish(rdb_filterbuilder_t *fb);

/*
 * Filter Reader
 */

/* REQUIRES: "contents" and *policy must stay live while *this is live. */
void
rdb_filterreader_init(rdb_filterreader_t *fr,
                      const struct rdb_bloom_s *policy,
                      const rdb_slice_t *contents);

int
rdb_filterreader_matches(const rdb_filterreader_t *fr,
                         uint64_t block_offset,
                         const rdb_slice_t *key);

#endif /* RDB_FILTER_BLOCK_H */
