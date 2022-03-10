/*!
 * filter_block_test.c - filter_block test for lcdb
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
#include <string.h>

#include "../util/bloom.h"
#include "../util/buffer.h"
#include "../util/coding.h"
#include "../util/extern.h"
#include "../util/hash.h"
#include "../util/slice.h"
#include "../util/testutil.h"

#include "filter_block.h"

/* For testing: emit an array with one hash value per key. */
static void
bloom_build(const ldb_bloom_t *bloom,
            ldb_buffer_t *dst,
            const ldb_slice_t *keys,
            size_t length) {
  size_t zn = length * 4;
  uint8_t *zp;
  uint32_t h;
  size_t i;

  (void)bloom;

  zp = ldb_buffer_expand(dst, zn);

  for (i = 0; i < length; i++) {
    h = ldb_hash(keys[i].data, keys[i].size, 1);
    zp = ldb_fixed32_write(zp, h);
  }

  dst->size += zn;
}

static int
bloom_match(const ldb_bloom_t *bloom,
            const ldb_slice_t *filter,
            const ldb_slice_t *key) {
  uint32_t h = ldb_hash(key->data, key->size, 1);
  size_t i;

  (void)bloom;

  ASSERT((filter->size & 3) == 0);

  for (i = 0; i < filter->size; i += 4) {
    if (h == ldb_fixed32_decode(filter->data + i))
      return 1;
  }

  return 0;
}

static const ldb_bloom_t bloom_test = {
  /* .name = */ "TestHashFilter",
  /* .build = */ bloom_build,
  /* .match = */ bloom_match,
  /* .bits_per_key = */ 0,
  /* .k = */ 0,
  /* .user_policy = */ NULL,
  /* .state = */ NULL
};

static void
test_empty_builder(const ldb_bloom_t *policy) {
  static uint8_t expect[] = {0, 0, 0, 0, 11};
  ldb_filterbuilder_t fb;
  ldb_filterreader_t fr;
  ldb_slice_t block;
  ldb_slice_t key;

  ldb_filterbuilder_init(&fb, policy);

  block = ldb_filterbuilder_finish(&fb);

  ASSERT(block.size == sizeof(expect));
  ASSERT(memcmp(block.data, expect, sizeof(expect)) == 0);

  ldb_filterreader_init(&fr, policy, &block);

  key = ldb_string("foo");

  ASSERT(ldb_filterreader_matches(&fr, 0, &key));
  ASSERT(ldb_filterreader_matches(&fr, 100000, &key));

  ldb_filterbuilder_clear(&fb);
}

static void
test_single_chunk(const ldb_bloom_t *policy) {
  ldb_filterbuilder_t fb;
  ldb_filterreader_t fr;
  ldb_slice_t block;
  ldb_slice_t key;

  ldb_filterbuilder_init(&fb, policy);
  ldb_filterbuilder_start_block(&fb, 100);

  key = ldb_string("foo");
  ldb_filterbuilder_add_key(&fb, &key);
  key = ldb_string("bar");
  ldb_filterbuilder_add_key(&fb, &key);
  key = ldb_string("box");
  ldb_filterbuilder_add_key(&fb, &key);

  ldb_filterbuilder_start_block(&fb, 200);

  key = ldb_string("box");
  ldb_filterbuilder_add_key(&fb, &key);

  ldb_filterbuilder_start_block(&fb, 300);

  key = ldb_string("hello");
  ldb_filterbuilder_add_key(&fb, &key);

  block = ldb_filterbuilder_finish(&fb);

  ldb_filterreader_init(&fr, policy, &block);

  key = ldb_string("foo");
  ASSERT(ldb_filterreader_matches(&fr, 100, &key));
  key = ldb_string("bar");
  ASSERT(ldb_filterreader_matches(&fr, 100, &key));
  key = ldb_string("box");
  ASSERT(ldb_filterreader_matches(&fr, 100, &key));
  key = ldb_string("hello");
  ASSERT(ldb_filterreader_matches(&fr, 100, &key));
  key = ldb_string("foo");
  ASSERT(ldb_filterreader_matches(&fr, 100, &key));
  key = ldb_string("missing");
  ASSERT(!ldb_filterreader_matches(&fr, 100, &key));
  key = ldb_string("other");
  ASSERT(!ldb_filterreader_matches(&fr, 100, &key));

  ldb_filterbuilder_clear(&fb);
}

static void
test_multi_chunk(const ldb_bloom_t *policy) {
  ldb_filterbuilder_t fb;
  ldb_filterreader_t fr;
  ldb_slice_t block;
  ldb_slice_t key;

  ldb_filterbuilder_init(&fb, policy);

  /* First filter. */
  ldb_filterbuilder_start_block(&fb, 0);
  key = ldb_string("foo");
  ldb_filterbuilder_add_key(&fb, &key);
  ldb_filterbuilder_start_block(&fb, 2000);
  key = ldb_string("bar");
  ldb_filterbuilder_add_key(&fb, &key);

  /* Second filter. */
  ldb_filterbuilder_start_block(&fb, 3100);
  key = ldb_string("box");
  ldb_filterbuilder_add_key(&fb, &key);

  /* Third filter is empty. */

  /* Last filter. */
  ldb_filterbuilder_start_block(&fb, 9000);
  key = ldb_string("box");
  ldb_filterbuilder_add_key(&fb, &key);
  key = ldb_string("hello");
  ldb_filterbuilder_add_key(&fb, &key);

  block = ldb_filterbuilder_finish(&fb);

  ldb_filterreader_init(&fr, policy, &block);

  /* Check first filter. */
  key = ldb_string("foo");
  ASSERT(ldb_filterreader_matches(&fr, 0, &key));
  key = ldb_string("bar");
  ASSERT(ldb_filterreader_matches(&fr, 2000, &key));
  key = ldb_string("box");
  ASSERT(!ldb_filterreader_matches(&fr, 0, &key));
  key = ldb_string("hello");
  ASSERT(!ldb_filterreader_matches(&fr, 0, &key));

  /* Check second filter. */
  key = ldb_string("box");
  ASSERT(ldb_filterreader_matches(&fr, 3100, &key));
  key = ldb_string("foo");
  ASSERT(!ldb_filterreader_matches(&fr, 3100, &key));
  key = ldb_string("bar");
  ASSERT(!ldb_filterreader_matches(&fr, 3100, &key));
  key = ldb_string("hello");
  ASSERT(!ldb_filterreader_matches(&fr, 3100, &key));

  /* Check third filter (empty). */
  key = ldb_string("foo");
  ASSERT(!ldb_filterreader_matches(&fr, 4100, &key));
  key = ldb_string("bar");
  ASSERT(!ldb_filterreader_matches(&fr, 4100, &key));
  key = ldb_string("box");
  ASSERT(!ldb_filterreader_matches(&fr, 4100, &key));
  key = ldb_string("hello");
  ASSERT(!ldb_filterreader_matches(&fr, 4100, &key));

  /* Check last filter. */
  key = ldb_string("box");
  ASSERT(ldb_filterreader_matches(&fr, 9000, &key));
  key = ldb_string("hello");
  ASSERT(ldb_filterreader_matches(&fr, 9000, &key));
  key = ldb_string("foo");
  ASSERT(!ldb_filterreader_matches(&fr, 9000, &key));
  key = ldb_string("bar");
  ASSERT(!ldb_filterreader_matches(&fr, 9000, &key));

  ldb_filterbuilder_clear(&fb);
}

LDB_EXTERN int
ldb_test_filter_block(void);

int
ldb_test_filter_block(void) {
  test_empty_builder(&bloom_test);
  test_single_chunk(&bloom_test);
  test_multi_chunk(&bloom_test);
  test_empty_builder(ldb_bloom_default);
  test_single_chunk(ldb_bloom_default);
  test_multi_chunk(ldb_bloom_default);
  return 0;
}
