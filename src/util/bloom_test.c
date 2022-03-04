/*!
 * bloom_test.c - bloom test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#undef NDEBUG

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "bloom.h"
#include "buffer.h"
#include "coding.h"
#include "extern.h"
#include "slice.h"

static rdb_slice_t
bloom_key(int i, uint8_t *buffer) {
  rdb_fixed32_write(buffer, i);
  return rdb_slice(buffer, 4);
}

static void
test_empty_filter(void) {
  const rdb_bloom_t *bloom = rdb_bloom_default;
  static uint8_t filter_[8] = {0, 0, 0, 0, 0, 0, 0, 6};
  static const rdb_slice_t filter = {filter_, 8, 0};
  rdb_slice_t key;

  key = rdb_string("hello");
  assert(!rdb_bloom_match(bloom, &filter, &key));

  key = rdb_string("world");
  assert(!rdb_bloom_match(bloom, &filter, &key));
}

static void
test_small_filter(void) {
  const rdb_bloom_t *bloom = rdb_bloom_default;
  rdb_slice_t keys[2];
  rdb_buffer_t filter;
  rdb_slice_t key;

  keys[0] = rdb_string("hello");
  keys[1] = rdb_string("world");

  rdb_buffer_init(&filter);
  rdb_bloom_create_filter(bloom, &filter, keys, 2);

  assert(rdb_bloom_match(bloom, &filter, &keys[0]));
  assert(rdb_bloom_match(bloom, &filter, &keys[1]));

  key = rdb_string("x");
  assert(!rdb_bloom_match(bloom, &filter, &key));

  key = rdb_string("foo");
  assert(!rdb_bloom_match(bloom, &filter, &key));

  rdb_buffer_clear(&filter);
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
  rdb_slice_t *keys = rdb_malloc(10000 * sizeof(rdb_slice_t));
  const rdb_bloom_t *bloom = rdb_bloom_default;
  uint8_t *bufs = rdb_malloc(10000 * 4);
  int mediocre_filters = 0;
  int good_filters = 0;
  rdb_buffer_t filter;
  uint8_t buffer[4];
  rdb_slice_t key;
  int length;

  rdb_buffer_init(&filter);

  /* Count number of filters that significantly exceed the FPR. */
  for (length = 1; length <= 10000; length = next_length(length)) {
    double rate;
    int i;

    for (i = 0; i < length; i++)
      keys[i] = bloom_key(i, &bufs[i * 4]);

    rdb_buffer_reset(&filter);
    rdb_bloom_create_filter(bloom, &filter, keys, length);

    assert(filter.size <= ((size_t)length * 10 / 8) + 40);

    /* All added keys must match. */
    for (i = 0; i < length; i++) {
      key = bloom_key(i, buffer);

      assert(rdb_bloom_match(bloom, &filter, &key));
    }

    /* Check false positive rate. */
    {
      int result = 0;

      for (i = 0; i < 10000; i++) {
        key = bloom_key(i + 1000000000, buffer);

        if (rdb_bloom_match(bloom, &filter, &key))
          result++;
      }

      rate = result / 10000.0;
    }

    if (verbose >= 1) {
      fprintf(stderr, "False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
                      rate * 100.0, length, (int)filter.size);
    }

    assert(rate <= 0.02); /* Must not be over 2%. */

    if (rate > 0.0125)
      mediocre_filters++; /* Allowed, but not too often. */
    else
      good_filters++;
  }

  if (verbose >= 1) {
    fprintf(stderr, "Filters: %d good, %d mediocre\n", good_filters,
                                                       mediocre_filters);
  }

  assert(mediocre_filters <= good_filters / 5);

  rdb_buffer_clear(&filter);
  rdb_free(keys);
  rdb_free(bufs);
}

RDB_EXTERN int
rdb_test_bloom(void);

int
rdb_test_bloom(void) {
  test_empty_filter();
  test_small_filter();
  test_varying_lengths(1);
  return 0;
}
