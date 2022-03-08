/*!
 * testutil.c - test utilities for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdint.h>
#include <stdlib.h>

#include "buffer.h"
#include "random.h"
#include "slice.h"

/*
 * Test Utils
 */

uint32_t
rdb_random_seed(void) {
  return rand() & 0x7fffffff;
}

rdb_slice_t *
rdb_random_string(rdb_buffer_t *dst, rdb_rand_t *rnd, size_t len) {
  size_t i;

  rdb_buffer_reset(dst);
  rdb_buffer_grow(dst, len + 1);

  for (i = 0; i < len; i++)
    dst->data[i] = ' ' + rdb_rand_uniform(rnd, 95);

  dst->data[len] = '\0';
  dst->size = len;

  return dst;
}

rdb_slice_t *
rdb_random_key(rdb_buffer_t *dst, rdb_rand_t *rnd, size_t len) {
  /* Make sure to generate a wide variety of characters so we
     test the boundary conditions for short-key optimizations. */
  static const char test_chars[] = {'\1', '\2', 'a',    'b',    'c',
                                    'd',  'e',  '\xfd', '\xfe', '\xff'};
  size_t i;

  rdb_buffer_reset(dst);
  rdb_buffer_grow(dst, len + 1);

  for (i = 0; i < len; i++) {
    uint32_t n = rdb_rand_uniform(rnd, sizeof(test_chars));

    dst->data[i] = test_chars[n];
  }

  dst->data[len] = '\0';
  dst->size = len;

  return dst;
}

rdb_slice_t *
rdb_compressible_string(rdb_buffer_t *dst,
                        rdb_rand_t *rnd,
                        double compressed_fraction,
                        size_t len) {
  size_t chunklen = (size_t)(len * compressed_fraction);
  rdb_buffer_t chunk;

  if (chunklen < 1)
    chunklen = 1;

  rdb_buffer_init(&chunk);
  rdb_random_string(&chunk, rnd, chunklen);

  /* Duplicate the random data until we have filled "len" bytes. */
  rdb_buffer_reset(dst);
  rdb_buffer_grow(dst, len + chunk.size + 1);

  while (dst->size < len)
    rdb_buffer_concat(dst, &chunk);

  dst->data[len] = '\0';
  dst->size = len;

  rdb_buffer_clear(&chunk);

  return dst;
}
