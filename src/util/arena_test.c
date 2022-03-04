/*!
 * arena_test.c - arena test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#undef NDEBUG

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include "arena.h"
#include "extern.h"
#include "random.h"
#include "slice.h"

RDB_EXTERN int
rdb_test_arena(void);

int
rdb_test_arena(void) {
  const size_t N = 100000;
  rdb_slice_t *allocated;
  size_t bytes = 0;
  rdb_arena_t arena;
  rdb_rand_t rnd;
  size_t i, length;

  rdb_arena_init(&arena);
  rdb_rand_init(&rnd, 301);

  allocated = rdb_malloc(N * sizeof(rdb_slice_t));
  length = 0;

  for (i = 0; i < N; i++) {
    size_t s, b;
    uint8_t *r;

    if (i % (N / 10) == 0) {
      s = i;
    } else {
      s = rdb_rand_one_in(&rnd, 4000)
        ? rdb_rand_uniform(&rnd, 6000)
        : (rdb_rand_one_in(&rnd, 10)
           ? rdb_rand_uniform(&rnd, 100)
           : rdb_rand_uniform(&rnd, 20));
    }

    if (s == 0) {
      /* Our arena disallows size 0 allocations. */
      s = 1;
    }

    if (rdb_rand_one_in(&rnd, 10))
      r = rdb_arena_alloc_aligned(&arena, s);
    else
      r = rdb_arena_alloc(&arena, s);

    for (b = 0; b < s; b++) {
      /* Fill the "i"th allocation with a known bit pattern. */
      r[b] = i % 256;
    }

    bytes += s;

    allocated[length++] = rdb_slice(r, s);

    assert(rdb_arena_usage(&arena) >= bytes);

    if (i > N / 10)
      assert(rdb_arena_usage(&arena) <= bytes * 1.10);
  }

  for (i = 0; i < length; i++) {
    size_t num_bytes = allocated[i].size;
    const uint8_t *p = allocated[i].data;
    size_t b;

    for (b = 0; b < num_bytes; b++) {
      /* Check the "i"th allocation for the known bit pattern. */
      assert(((int)p[b] & 0xff) == (i % 256));
    }
  }

  rdb_arena_clear(&arena);
  rdb_free(allocated);

  return 0;
}
