/*!
 * arena_test.c - arena test for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on google/leveldb:
 *   Copyright (c) 2011, The LevelDB Authors. All rights reserved.
 *   https://github.com/google/leveldb
 *
 * See LICENSE for more information.
 */

#include <stdint.h>
#include <stdlib.h>

#include "util/arena.h"
#include "util/random.h"
#include "util/slice.h"
#include "util/testutil.h"

int
main(void) {
  const size_t N = 100000;
  ldb_slice_t *allocated;
  size_t bytes = 0;
  ldb_arena_t arena;
  ldb_rand_t rnd;
  size_t i, length;

  ldb_arena_init(&arena);
  ldb_rand_init(&rnd, 301);

  allocated = ldb_malloc(N * sizeof(ldb_slice_t));
  length = 0;

  for (i = 0; i < N; i++) {
    size_t s, b;
    uint8_t *r;

    if (i % (N / 10) == 0) {
      s = i;
    } else {
      s = ldb_rand_one_in(&rnd, 4000)
        ? ldb_rand_uniform(&rnd, 6000)
        : (ldb_rand_one_in(&rnd, 10)
           ? ldb_rand_uniform(&rnd, 100)
           : ldb_rand_uniform(&rnd, 20));
    }

    if (s == 0) {
      /* Our arena disallows size 0 allocations. */
      s = 1;
    }

    if (ldb_rand_one_in(&rnd, 10))
      r = ldb_arena_alloc_aligned(&arena, s);
    else
      r = ldb_arena_alloc(&arena, s);

    for (b = 0; b < s; b++) {
      /* Fill the "i"th allocation with a known bit pattern. */
      r[b] = i % 256;
    }

    bytes += s;

    allocated[length++] = ldb_slice(r, s);

    ASSERT(ldb_arena_usage(&arena) >= bytes);

    if (i > N / 10)
      ASSERT(ldb_arena_usage(&arena) <= bytes * 1.10);
  }

  for (i = 0; i < length; i++) {
    size_t num_bytes = allocated[i].size;
    const uint8_t *p = allocated[i].data;
    size_t b;

    for (b = 0; b < num_bytes; b++) {
      /* Check the "i"th allocation for the known bit pattern. */
      ASSERT(((int)p[b] & 0xff) == (i % 256));
    }
  }

  ldb_arena_clear(&arena);
  ldb_free(allocated);

  return 0;
}
