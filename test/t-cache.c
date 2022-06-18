/*!
 * t-cache.c - cache test for lcdb
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

#include "util/array.h"
#include "util/cache.h"
#include "util/coding.h"
#include "util/slice.h"
#include "util/testutil.h"
#include "util/vector.h"

/*
 * Constants
 */

#define CACHE_SIZE 1000

/*
 * Types
 */

typedef struct test_s {
  ldb_array_t deleted_keys;
  ldb_array_t deleted_values;
  ldb_lru_t *cache;
} test_t;

static test_t *current;

/*
 * Helpers
 */

/* Conversions between numeric keys/values and the types expected by Cache. */
static ldb_slice_t
encode_key(int k, uint8_t *buf) {
  ldb_fixed32_write(buf, k);
  return ldb_slice(buf, 4);
}

static int
decode_key(const ldb_slice_t *key) {
  ASSERT(key->size == 4);
  return ldb_fixed32_decode(key->data) & 0x7fffffff;
}

static void *
encode_value(uintptr_t v) {
  return (void *)v;
}

static int
decode_value(void *v) {
  return (uintptr_t)v & 0x7fffffff;
}

/*
 * CacheTest
 */

static void
test_init(test_t *t) {
  ldb_array_init(&t->deleted_keys);
  ldb_array_init(&t->deleted_values);

  t->cache = ldb_lru_create(CACHE_SIZE);

  current = t;
}

static void
test_clear(test_t *t) {
  ldb_lru_destroy(t->cache);

  ldb_array_clear(&t->deleted_keys);
  ldb_array_clear(&t->deleted_values);
}

static void
test_deleter(const ldb_slice_t *key, void *value) {
  ldb_array_push(&current->deleted_keys, decode_key(key));
  ldb_array_push(&current->deleted_values, decode_value(value));
}

static int
test_lookup(test_t *t, int key) {
  ldb_entry_t *h;
  uint8_t buf[4];
  ldb_slice_t k;
  int r = -1;

  k = encode_key(key, buf);
  h = ldb_lru_lookup(t->cache, &k);

  if (h != NULL) {
    r = decode_value(ldb_lru_value(h));

    ldb_lru_release(t->cache, h);
  }

  return r;
}

static void
test_insert(test_t *t, int key, int value, int charge) {
  ldb_entry_t *h;
  uint8_t buf[4];
  ldb_slice_t k;

  k = encode_key(key, buf);

  h = ldb_lru_insert(t->cache, &k,
                     encode_value(value),
                     charge,
                     &test_deleter);

  ldb_lru_release(t->cache, h);
}

static ldb_entry_t *
test_insert2(test_t *t, int key, int value, int charge) {
  uint8_t buf[4];
  ldb_slice_t k;

  k = encode_key(key, buf);

  return ldb_lru_insert(t->cache, &k,
                        encode_value(value),
                        charge,
                        &test_deleter);
}

static void
test_erase(test_t *t, int key) {
  uint8_t buf[4];
  ldb_slice_t k;

  k = encode_key(key, buf);

  ldb_lru_erase(t->cache, &k);
}

/*
 * Tests
 */

static void
test_cache_hit_and_miss(void) {
  test_t t;

  test_init(&t);

  ASSERT(-1 == test_lookup(&t, 100));

  test_insert(&t, 100, 101, 1);

  ASSERT(101 == test_lookup(&t, 100));
  ASSERT(-1 == test_lookup(&t, 200));
  ASSERT(-1 == test_lookup(&t, 300));

  test_insert(&t, 200, 201, 1);

  ASSERT(101 == test_lookup(&t, 100));
  ASSERT(201 == test_lookup(&t, 200));
  ASSERT(-1 == test_lookup(&t, 300));

  test_insert(&t, 100, 102, 1);

  ASSERT(102 == test_lookup(&t, 100));
  ASSERT(201 == test_lookup(&t, 200));
  ASSERT(-1 == test_lookup(&t, 300));

  ASSERT(1 == t.deleted_keys.length);
  ASSERT(100 == t.deleted_keys.items[0]);
  ASSERT(101 == t.deleted_values.items[0]);

  test_clear(&t);
}

static void
test_cache_erase(void) {
  test_t t;

  test_init(&t);

  test_erase(&t, 200);

  ASSERT(0 == t.deleted_keys.length);

  test_insert(&t, 100, 101, 1);
  test_insert(&t, 200, 201, 1);
  test_erase(&t, 100);

  ASSERT(-1 == test_lookup(&t, 100));
  ASSERT(201 == test_lookup(&t, 200));
  ASSERT(1 == t.deleted_keys.length);
  ASSERT(100 == t.deleted_keys.items[0]);
  ASSERT(101 == t.deleted_values.items[0]);

  test_erase(&t, 100);

  ASSERT(-1 == test_lookup(&t, 100));
  ASSERT(201 == test_lookup(&t, 200));
  ASSERT(1 == t.deleted_keys.length);

  test_clear(&t);
}

static void
test_cache_entries_are_pinned(void) {
  ldb_entry_t *h1, *h2;
  ldb_slice_t key;
  uint8_t buf[4];
  test_t t;

  test_init(&t);

  key = encode_key(100, buf);

  test_insert(&t, 100, 101, 1);

  h1 = ldb_lru_lookup(t.cache, &key);

  ASSERT(101 == decode_value(ldb_lru_value(h1)));

  test_insert(&t, 100, 102, 1);

  h2 = ldb_lru_lookup(t.cache, &key);

  ASSERT(102 == decode_value(ldb_lru_value(h2)));
  ASSERT(0 == t.deleted_keys.length);

  ldb_lru_release(t.cache, h1);

  ASSERT(1 == t.deleted_keys.length);
  ASSERT(100 == t.deleted_keys.items[0]);
  ASSERT(101 == t.deleted_values.items[0]);

  test_erase(&t, 100);

  ASSERT(-1 == test_lookup(&t, 100));
  ASSERT(1 == t.deleted_keys.length);

  ldb_lru_release(t.cache, h2);

  ASSERT(2 == t.deleted_keys.length);
  ASSERT(100 == t.deleted_keys.items[1]);
  ASSERT(102 == t.deleted_values.items[1]);

  test_clear(&t);
}

static void
test_cache_eviction_policy(void) {
  ldb_entry_t *h;
  ldb_slice_t key;
  uint8_t buf[4];
  test_t t;
  int i;

  test_init(&t);

  test_insert(&t, 100, 101, 1);
  test_insert(&t, 200, 201, 1);
  test_insert(&t, 300, 301, 1);

  key = encode_key(300, buf);
  h = ldb_lru_lookup(t.cache, &key);

  /* Frequently used entry must be kept around,
     as must things that are still in use. */
  for (i = 0; i < CACHE_SIZE + 100; i++) {
    test_insert(&t, 1000 + i, 2000 + i, 1);

    ASSERT(2000 + i == test_lookup(&t, 1000 + i));
    ASSERT(101 == test_lookup(&t, 100));
  }

  ASSERT(101 == test_lookup(&t, 100));
  ASSERT(-1 == test_lookup(&t, 200));
  ASSERT(301 == test_lookup(&t, 300));

  ldb_lru_release(t.cache, h);

  test_clear(&t);
}

static void
test_cache_use_exceeds_cache_size(void) {
  ldb_vector_t h;
  test_t t;
  size_t i;

  test_init(&t);
  ldb_vector_init(&h);

  /* Overfill the cache, keeping handles on all inserted entries. */
  for (i = 0; i < CACHE_SIZE + 100; i++)
    ldb_vector_push(&h, test_insert2(&t, 1000 + i, 2000 + i, 1));

  /* Check that all the entries can be found in the cache. */
  for (i = 0; i < h.length; i++)
    ASSERT(2000 + i == (size_t)test_lookup(&t, 1000 + i));

  for (i = 0; i < h.length; i++)
    ldb_lru_release(t.cache, h.items[i]);

  ldb_vector_clear(&h);
  test_clear(&t);
}

static void
test_cache_heavy_entries(void) {
  const int light = 1;
  const int heavy = 10;
  int cached_weight = 0;
  int added = 0;
  int index = 0;
  test_t t;
  int i;

  test_init(&t);

  /* Add a bunch of light and heavy entries and then count the combined
     size of items still in the cache, which must be approximately the
     same as the total capacity. */
  while (added < 2 * CACHE_SIZE) {
    int weight = (index & 1) ? light : heavy;

    test_insert(&t, index, 1000 + index, weight);

    added += weight;
    index++;
  }

  for (i = 0; i < index; i++) {
    int weight = (i & 1 ? light : heavy);
    int r = test_lookup(&t, i);

    if (r >= 0) {
      cached_weight += weight;
      ASSERT(1000 + i == r);
    }
  }

  ASSERT(cached_weight <= CACHE_SIZE + CACHE_SIZE / 10);

  test_clear(&t);
}

static void
test_cache_newid(void) {
  uint64_t a, b;
  test_t t;

  test_init(&t);

  a = ldb_lru_newid(t.cache);
  b = ldb_lru_newid(t.cache);

  ASSERT(a != b);

  test_clear(&t);
}

static void
test_cache_prune(void) {
  ldb_entry_t *h;
  ldb_slice_t key;
  uint8_t buf[4];
  test_t t;

  test_init(&t);

  test_insert(&t, 1, 100, 1);
  test_insert(&t, 2, 200, 1);

  key = encode_key(1, buf);
  h = ldb_lru_lookup(t.cache, &key);

  ASSERT(h != NULL);

  ldb_lru_prune(t.cache);
  ldb_lru_release(t.cache, h);

  ASSERT(100 == test_lookup(&t, 1));
  ASSERT(-1 == test_lookup(&t, 2));

  test_clear(&t);
}

static void
test_cache_zero_size_cache(void) {
  test_t t;

  test_init(&t);

  ldb_lru_destroy(t.cache);

  t.cache = ldb_lru_create(0);

  test_insert(&t, 1, 100, 1);

  ASSERT(-1 == test_lookup(&t, 1));

  test_clear(&t);
}

/*
 * Execute
 */

int
main(void) {
  test_cache_hit_and_miss();
  test_cache_erase();
  test_cache_entries_are_pinned();
  test_cache_eviction_policy();
  test_cache_use_exceeds_cache_size();
  test_cache_heavy_entries();
  test_cache_newid();
  test_cache_prune();
  test_cache_zero_size_cache();
  return 0;
}
