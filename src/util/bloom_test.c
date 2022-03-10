/*!
 * bloom_test.c - bloom test for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bloom.h"
#include "buffer.h"
#include "coding.h"
#include "extern.h"
#include "slice.h"
#include "testutil.h"

static ldb_slice_t
bloom_key(int i, uint8_t *buffer) {
  ldb_fixed32_write(buffer, i);
  return ldb_slice(buffer, 4);
}

static void
test_empty_filter(void) {
  const ldb_bloom_t *bloom = ldb_bloom_default;
  static uint8_t filter_[8] = {0, 0, 0, 0, 0, 0, 0, 6};
  static const ldb_slice_t filter = {filter_, 8, 0};
  ldb_slice_t key;

  key = ldb_string("hello");
  ASSERT(!ldb_bloom_match(bloom, &filter, &key));

  key = ldb_string("world");
  ASSERT(!ldb_bloom_match(bloom, &filter, &key));
}

static void
test_small_filter(void) {
  const ldb_bloom_t *bloom = ldb_bloom_default;
  ldb_slice_t keys[2];
  ldb_buffer_t filter;
  ldb_slice_t key;

  keys[0] = ldb_string("hello");
  keys[1] = ldb_string("world");

  ldb_buffer_init(&filter);
  ldb_bloom_build(bloom, &filter, keys, 2);

  ASSERT(ldb_bloom_match(bloom, &filter, &keys[0]));
  ASSERT(ldb_bloom_match(bloom, &filter, &keys[1]));

  key = ldb_string("x");
  ASSERT(!ldb_bloom_match(bloom, &filter, &key));

  key = ldb_string("foo");
  ASSERT(!ldb_bloom_match(bloom, &filter, &key));

  ldb_buffer_clear(&filter);
}

static int
next_length(int length) {
  if (length < 10)
    length += 1;
  else if (length < 100)
    length += 10;
  else if (length < 1000)
    length += 100;
  else
    length += 1000;

  return length;
}

static void
test_varying_lengths(int verbose) {
  ldb_slice_t *keys = ldb_malloc(10000 * sizeof(ldb_slice_t));
  const ldb_bloom_t *bloom = ldb_bloom_default;
  uint8_t *bufs = ldb_malloc(10000 * 4);
  int mediocre_filters = 0;
  int good_filters = 0;
  ldb_buffer_t filter;
  uint8_t buffer[4];
  ldb_slice_t key;
  int length;

  ldb_buffer_init(&filter);

  /* Count number of filters that significantly exceed the FPR. */
  for (length = 1; length <= 10000; length = next_length(length)) {
    double rate;
    int i;

    for (i = 0; i < length; i++)
      keys[i] = bloom_key(i, &bufs[i * 4]);

    ldb_buffer_reset(&filter);
    ldb_bloom_build(bloom, &filter, keys, length);

    ASSERT(filter.size <= ((size_t)length * 10 / 8) + 40);

    /* All added keys must match. */
    for (i = 0; i < length; i++) {
      key = bloom_key(i, buffer);

      ASSERT(ldb_bloom_match(bloom, &filter, &key));
    }

    /* Check false positive rate. */
    {
      int result = 0;

      for (i = 0; i < 10000; i++) {
        key = bloom_key(i + 1000000000, buffer);

        if (ldb_bloom_match(bloom, &filter, &key))
          result++;
      }

      rate = result / 10000.0;
    }

    if (verbose >= 1) {
      fprintf(stderr, "False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
                      rate * 100.0, length, (int)filter.size);
    }

    ASSERT(rate <= 0.02); /* Must not be over 2%. */

    if (rate > 0.0125)
      mediocre_filters++; /* Allowed, but not too often. */
    else
      good_filters++;
  }

  if (verbose >= 1) {
    fprintf(stderr, "Filters: %d good, %d mediocre\n", good_filters,
                                                       mediocre_filters);
  }

  ASSERT(mediocre_filters <= good_filters / 5);

  ldb_buffer_clear(&filter);
  ldb_free(keys);
  ldb_free(bufs);
}

LDB_EXTERN int
ldb_test_bloom(void);

int
ldb_test_bloom(void) {
  test_empty_filter();
  test_small_filter();
  test_varying_lengths(1);
  return 0;
}
