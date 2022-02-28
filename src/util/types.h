/*!
 * types.h - types for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_TYPES_H
#define RDB_TYPES_H

#include <stddef.h>
#include <stdint.h>

typedef struct rdb_buffer_s {
  uint8_t *data;
  size_t size;
  size_t alloc;
} rdb_buffer_t;

typedef rdb_buffer_t rdb_slice_t;

/* A range of keys. */
typedef struct rdb_range_s {
  rdb_slice_t start; /* Included in the range. */
  rdb_slice_t limit; /* Not included in the range. */
} rdb_range_t;

typedef struct rdb_array_s {
  int64_t *items;
  size_t length;
  size_t alloc;
} rdb_array_t;

typedef struct rdb_vector_s {
  void **items;
  size_t length;
  size_t alloc;
} rdb_vector_t;

#endif /* RDB_TYPES_H */
