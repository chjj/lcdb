/*!
 * t-util.c - util test for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 */

#include <stddef.h>
#include <stdint.h>

#include "util/array.h"
#include "util/internal.h"
#include "util/random.h"
#include "util/testutil.h"
#include "util/vector.h"

/*
 * Helpers
 */

static int
array_compare(int64_t x, int64_t y) {
  return LDB_CMP(x, y);
}

static int
vector_compare(void *x, void *y) {
  uint32_t *a = x, *b = y;
  return LDB_CMP(*a, *b);
}

static uint32_t *
ldb_rand_alloc(ldb_rand_t *rnd) {
  uint32_t *num = ldb_malloc(sizeof(uint32_t));
  *num = ldb_rand_next(rnd);
  return num;
}

/*
 * Array
 */

static void
test_sort_array(int iter, int max) {
  ldb_array_t nums;
  ldb_rand_t rnd;
  int i, j;

  ldb_rand_init(&rnd, ldb_random_seed());

  ldb_array_init(&nums);

  for (i = 0; i < iter; i++) {
    int len = ldb_rand_uniform(&rnd, max);

    for (j = 0; j < len; j++)
      ldb_array_push(&nums, ldb_rand_next(&rnd));

    ldb_array_sort(&nums, array_compare);

    for (j = 1; j < len; j++)
      ASSERT(nums.items[j - 1] <= nums.items[j]);

    ldb_array_reset(&nums);
  }

  ldb_array_clear(&nums);
}

/*
 * Vector
 */

static void
test_sort_vector(int iter, int max) {
  ldb_vector_t nums;
  ldb_rand_t rnd;
  int i, j;

  ldb_rand_init(&rnd, ldb_random_seed());

  ldb_vector_init(&nums);

  for (i = 0; i < iter; i++) {
    int len = ldb_rand_uniform(&rnd, max);

    for (j = 0; j < len; j++)
      ldb_vector_push(&nums, ldb_rand_alloc(&rnd));

    ldb_vector_sort(&nums, vector_compare);

    for (j = 1; j < len; j++) {
      uint32_t *x = nums.items[j - 1];
      uint32_t *y = nums.items[j];

      ASSERT(*x <= *y);
    }

    for (j = 0; j < len; j++)
      ldb_free(nums.items[j]);

    ldb_vector_reset(&nums);
  }

  ldb_vector_clear(&nums);
}

/*
 * Main
 */

int
main(void) {
  test_sort_array(500, 10);
  test_sort_array(500, 500);
  test_sort_array(10, 10000);
  test_sort_vector(500, 10);
  test_sort_vector(500, 500);
  test_sort_vector(10, 10000);
  return 0;
}
