/*!
 * filter_block_test.c - filter_block test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#undef NDEBUG

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "../util/bloom.h"
#include "../util/buffer.h"
#include "../util/coding.h"
#include "../util/extern.h"
#include "../util/hash.h"
#include "../util/slice.h"

#include "filter_block.h"

/* For testing: emit an array with one hash value per key. */
static void
bloom_build(const rdb_bloom_t *bloom,
            rdb_buffer_t *dst,
            const rdb_slice_t *keys,
            size_t length) {
  size_t zn = length * 4;
  uint8_t *zp;
  uint32_t h;
  size_t i;

  (void)bloom;

  zp = rdb_buffer_expand(dst, zn);

  for (i = 0; i < length; i++) {
    h = rdb_hash(keys[i].data, keys[i].size, 1);
    zp = rdb_fixed32_write(zp, h);
  }

  dst->size += zn;
}

static int
bloom_match(const rdb_bloom_t *bloom,
            const rdb_slice_t *filter,
            const rdb_slice_t *key) {
  uint32_t h = rdb_hash(key->data, key->size, 1);
  size_t i;

  (void)bloom;

  for (i = 0; i + 4 <= filter->size; i += 4) {
    if (h == rdb_fixed32_decode(filter->data + i))
      return 1;
  }

  return 0;
}

static const rdb_bloom_t bloom_test = {
  /* .name = */ "TestHashFilter",
  /* .build = */ bloom_build,
  /* .match = */ bloom_match,
  /* .bits_per_key = */ 0,
  /* .k = */ 0,
  /* .user_policy = */ NULL
};

static void
test_empty_builder(const rdb_bloom_t *policy) {
  static uint8_t expect[] = {0, 0, 0, 0, 11};
  rdb_filterbuilder_t fb;
  rdb_filterreader_t fr;
  rdb_slice_t block;
  rdb_slice_t key;

  rdb_filterbuilder_init(&fb, policy);

  block = rdb_filterbuilder_finish(&fb);

  assert(block.size == sizeof(expect));
  assert(memcmp(block.data, expect, sizeof(expect)) == 0);

  rdb_filterreader_init(&fr, policy, &block);

  key = rdb_string("foo");

  assert(rdb_filterreader_matches(&fr, 0, &key));
  assert(rdb_filterreader_matches(&fr, 100000, &key));

  rdb_filterbuilder_clear(&fb);
}

static void
test_single_chunk(const rdb_bloom_t *policy) {
  rdb_filterbuilder_t fb;
  rdb_filterreader_t fr;
  rdb_slice_t block;
  rdb_slice_t key;

  rdb_filterbuilder_init(&fb, policy);
  rdb_filterbuilder_start_block(&fb, 100);

  key = rdb_string("foo");
  rdb_filterbuilder_add_key(&fb, &key);
  key = rdb_string("bar");
  rdb_filterbuilder_add_key(&fb, &key);
  key = rdb_string("box");
  rdb_filterbuilder_add_key(&fb, &key);

  rdb_filterbuilder_start_block(&fb, 200);

  key = rdb_string("box");
  rdb_filterbuilder_add_key(&fb, &key);

  rdb_filterbuilder_start_block(&fb, 300);

  key = rdb_string("hello");
  rdb_filterbuilder_add_key(&fb, &key);

  block = rdb_filterbuilder_finish(&fb);

  rdb_filterreader_init(&fr, policy, &block);

  key = rdb_string("foo");
  assert(rdb_filterreader_matches(&fr, 100, &key));
  key = rdb_string("bar");
  assert(rdb_filterreader_matches(&fr, 100, &key));
  key = rdb_string("box");
  assert(rdb_filterreader_matches(&fr, 100, &key));
  key = rdb_string("hello");
  assert(rdb_filterreader_matches(&fr, 100, &key));
  key = rdb_string("foo");
  assert(rdb_filterreader_matches(&fr, 100, &key));
  key = rdb_string("missing");
  assert(!rdb_filterreader_matches(&fr, 100, &key));
  key = rdb_string("other");
  assert(!rdb_filterreader_matches(&fr, 100, &key));

  rdb_filterbuilder_clear(&fb);
}

static void
test_multi_chunk(const rdb_bloom_t *policy) {
  rdb_filterbuilder_t fb;
  rdb_filterreader_t fr;
  rdb_slice_t block;
  rdb_slice_t key;

  rdb_filterbuilder_init(&fb, policy);

  /* First filter. */
  rdb_filterbuilder_start_block(&fb, 0);
  key = rdb_string("foo");
  rdb_filterbuilder_add_key(&fb, &key);
  rdb_filterbuilder_start_block(&fb, 2000);
  key = rdb_string("bar");
  rdb_filterbuilder_add_key(&fb, &key);

  /* Second filter. */
  rdb_filterbuilder_start_block(&fb, 3100);
  key = rdb_string("box");
  rdb_filterbuilder_add_key(&fb, &key);

  /* Third filter is empty. */

  /* Last filter. */
  rdb_filterbuilder_start_block(&fb, 9000);
  key = rdb_string("box");
  rdb_filterbuilder_add_key(&fb, &key);
  key = rdb_string("hello");
  rdb_filterbuilder_add_key(&fb, &key);

  block = rdb_filterbuilder_finish(&fb);

  rdb_filterreader_init(&fr, policy, &block);

  /* Check first filter. */
  key = rdb_string("foo");
  assert(rdb_filterreader_matches(&fr, 0, &key));
  key = rdb_string("bar");
  assert(rdb_filterreader_matches(&fr, 2000, &key));
  key = rdb_string("box");
  assert(!rdb_filterreader_matches(&fr, 0, &key));
  key = rdb_string("hello");
  assert(!rdb_filterreader_matches(&fr, 0, &key));

  /* Check second filter. */
  key = rdb_string("box");
  assert(rdb_filterreader_matches(&fr, 3100, &key));
  key = rdb_string("foo");
  assert(!rdb_filterreader_matches(&fr, 3100, &key));
  key = rdb_string("bar");
  assert(!rdb_filterreader_matches(&fr, 3100, &key));
  key = rdb_string("hello");
  assert(!rdb_filterreader_matches(&fr, 3100, &key));

  /* Check third filter (empty). */
  key = rdb_string("foo");
  assert(!rdb_filterreader_matches(&fr, 4100, &key));
  key = rdb_string("bar");
  assert(!rdb_filterreader_matches(&fr, 4100, &key));
  key = rdb_string("box");
  assert(!rdb_filterreader_matches(&fr, 4100, &key));
  key = rdb_string("hello");
  assert(!rdb_filterreader_matches(&fr, 4100, &key));

  /* Check last filter. */
  key = rdb_string("box");
  assert(rdb_filterreader_matches(&fr, 9000, &key));
  key = rdb_string("hello");
  assert(rdb_filterreader_matches(&fr, 9000, &key));
  key = rdb_string("foo");
  assert(!rdb_filterreader_matches(&fr, 9000, &key));
  key = rdb_string("bar");
  assert(!rdb_filterreader_matches(&fr, 9000, &key));

  rdb_filterbuilder_clear(&fb);
}

RDB_EXTERN int
rdb_test_filter_block(void);

int
rdb_test_filter_block(void) {
  test_empty_builder(&bloom_test);
  test_single_chunk(&bloom_test);
  test_multi_chunk(&bloom_test);
  test_empty_builder(rdb_bloom_default);
  test_single_chunk(rdb_bloom_default);
  test_multi_chunk(rdb_bloom_default);
  return 0;
}
