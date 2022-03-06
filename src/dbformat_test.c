/*!
 * dbformat_test.c - dbformat test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "util/buffer.h"
#include "util/comparator.h"
#include "util/extern.h"
#include "util/internal.h"
#include "util/slice.h"
#include "util/testutil.h"

#include "dbformat.h"

static void
ikey_set(rdb_ikey_t *ikey, const char *ukey, uint64_t seq, rdb_valtype_t vt) {
  rdb_slice_t key = rdb_string(ukey);
  rdb_ikey_set(ikey, &key, seq, vt);
}

static rdb_buffer_t *
shortest_separator(rdb_buffer_t *start, const rdb_slice_t *limit) {
  rdb_comparator_t icmp;
  rdb_ikc_init(&icmp, rdb_bytewise_comparator);
  rdb_shortest_separator(&icmp, start, limit);
  return start;
}

static rdb_buffer_t *
short_successor(rdb_buffer_t *key) {
  rdb_comparator_t icmp;
  rdb_ikc_init(&icmp, rdb_bytewise_comparator);
  rdb_short_successor(&icmp, key);
  return key;
}

static void
test_key(const char *key, uint64_t seq, rdb_valtype_t vt) {
  rdb_slice_t k = rdb_string(key);
  rdb_ikey_t encoded;
  rdb_pkey_t decoded;

  rdb_ikey_init(&encoded);

  rdb_ikey_set(&encoded, &k, seq, vt);

  ASSERT(rdb_pkey_import(&decoded, &encoded));
  ASSERT(rdb_slice_equal(&k, &decoded.user_key));
  ASSERT(seq == decoded.sequence);
  ASSERT(vt == decoded.type);

  k = rdb_string("bar");

  ASSERT(!rdb_pkey_import(&decoded, &k));

  rdb_ikey_clear(&encoded);
}

static void
test_ikey_encode_decode(void) {
  const char *keys[] = {"", "k", "hello", "longggggggggggggggggggggg"};
  const uint64_t seq[] = {1,
                          2,
                          3,
                          (UINT64_C(1) << 8) - 1,
                          UINT64_C(1) << 8,
                          (UINT64_C(1) << 8) + 1,
                          (UINT64_C(1) << 16) - 1,
                          UINT64_C(1) << 16,
                          (UINT64_C(1) << 16) + 1,
                          (UINT64_C(1) << 32) - 1,
                          UINT64_C(1) << 32,
                          (UINT64_C(1) << 32) + 1};
  size_t k, s;

  for (k = 0; k < lengthof(keys); k++) {
    for (s = 0; s < lengthof(seq); s++) {
      test_key(keys[k], seq[s], RDB_TYPE_VALUE);
      test_key("hello", 1, RDB_TYPE_DELETION);
    }
  }
}

static void
test_ikey_short_separator(void) {
  rdb_ikey_t k0, k1, k2;

  rdb_ikey_init(&k0);
  rdb_ikey_init(&k1);
  rdb_ikey_init(&k2);

  /* When user keys are same. */
  ikey_set(&k0, "foo", 100, RDB_TYPE_VALUE);
  ikey_set(&k1, "foo", 100, RDB_TYPE_VALUE);
  ikey_set(&k2, "foo", 99, RDB_TYPE_VALUE);
  ASSERT(rdb_buffer_equal(&k0, shortest_separator(&k1, &k2)));

  ikey_set(&k0, "foo", 100, RDB_TYPE_VALUE);
  ikey_set(&k1, "foo", 100, RDB_TYPE_VALUE);
  ikey_set(&k2, "foo", 101, RDB_TYPE_VALUE);
  ASSERT(rdb_buffer_equal(&k0, shortest_separator(&k1, &k2)));

  ikey_set(&k0, "foo", 100, RDB_TYPE_VALUE);
  ikey_set(&k1, "foo", 100, RDB_TYPE_VALUE);
  ikey_set(&k2, "foo", 100, RDB_TYPE_VALUE);
  ASSERT(rdb_buffer_equal(&k0, shortest_separator(&k1, &k2)));

  ikey_set(&k0, "foo", 100, RDB_TYPE_VALUE);
  ikey_set(&k1, "foo", 100, RDB_TYPE_VALUE);
  ikey_set(&k2, "foo", 100, RDB_TYPE_DELETION);
  ASSERT(rdb_buffer_equal(&k0, shortest_separator(&k1, &k2)));

  /* When user keys are misordered. */
  ikey_set(&k0, "foo", 100, RDB_TYPE_VALUE);
  ikey_set(&k1, "foo", 100, RDB_TYPE_VALUE);
  ikey_set(&k2, "bar", 99, RDB_TYPE_VALUE);
  ASSERT(rdb_buffer_equal(&k0, shortest_separator(&k1, &k2)));

  /* When user keys are different, but correctly ordered. */
  ikey_set(&k0, "g", RDB_MAX_SEQUENCE, RDB_VALTYPE_SEEK);
  ikey_set(&k1, "foo", 100, RDB_TYPE_VALUE);
  ikey_set(&k2, "hello", 200, RDB_TYPE_VALUE);
  ASSERT(rdb_buffer_equal(&k0, shortest_separator(&k1, &k2)));

  /* When start user key is prefix of limit user key. */
  ikey_set(&k0, "foo", 100, RDB_TYPE_VALUE);
  ikey_set(&k1, "foo", 100, RDB_TYPE_VALUE);
  ikey_set(&k2, "foobar", 200, RDB_TYPE_VALUE);
  ASSERT(rdb_buffer_equal(&k0, shortest_separator(&k1, &k2)));

  /* When limit user key is prefix of start user key. */
  ikey_set(&k0, "foobar", 100, RDB_TYPE_VALUE);
  ikey_set(&k1, "foobar", 100, RDB_TYPE_VALUE);
  ikey_set(&k2, "foo", 200, RDB_TYPE_VALUE);
  ASSERT(rdb_buffer_equal(&k0, shortest_separator(&k1, &k2)));

  rdb_ikey_clear(&k0);
  rdb_ikey_clear(&k1);
  rdb_ikey_clear(&k2);
}

static void
test_ikey_shortest_successor(void) {
  rdb_ikey_t k0, k1;

  rdb_ikey_init(&k0);
  rdb_ikey_init(&k1);

  ikey_set(&k0, "g", RDB_MAX_SEQUENCE, RDB_VALTYPE_SEEK);
  ikey_set(&k1, "foo", 100, RDB_TYPE_VALUE);
  ASSERT(rdb_buffer_equal(&k0, short_successor(&k1)));

  ikey_set(&k0, "\xff\xff", 100, RDB_TYPE_VALUE);
  ikey_set(&k1, "\xff\xff", 100, RDB_TYPE_VALUE);
  ASSERT(rdb_buffer_equal(&k0, short_successor(&k1)));

  rdb_ikey_clear(&k0);
  rdb_ikey_clear(&k1);
}

static void
test_pkey_debug_string(void) {
  rdb_slice_t key = rdb_string("The \"key\" in 'single quotes'");
  rdb_slice_t expect = rdb_string("'The \"key\" in 'single quotes'' @ 42 : 1");
  rdb_buffer_t str;
  rdb_pkey_t pkey;

  rdb_buffer_init(&str);

  rdb_pkey_init(&pkey, &key, 42, RDB_TYPE_VALUE);
  rdb_pkey_debug(&str, &pkey);

  ASSERT(rdb_slice_equal(&expect, &str));

  rdb_buffer_clear(&str);
}

static void
test_ikey_debug_string(void) {
  rdb_slice_t key = rdb_string("The \"key\" in 'single quotes'");
  rdb_slice_t expect = rdb_string("'The \"key\" in 'single quotes'' @ 42 : 1");
  rdb_slice_t bad = rdb_string("(bad)");
  rdb_buffer_t str;
  rdb_ikey_t ikey;

  rdb_ikey_init(&ikey);
  rdb_buffer_init(&str);

  rdb_ikey_set(&ikey, &key, 42, RDB_TYPE_VALUE);
  rdb_ikey_debug(&str, &ikey);

  ASSERT(rdb_slice_equal(&expect, &str));

  rdb_buffer_reset(&ikey);
  rdb_buffer_reset(&str);

  rdb_ikey_debug(&str, &ikey);

  ASSERT(rdb_slice_equal(&bad, &str));

  rdb_buffer_clear(&str);
  rdb_ikey_clear(&ikey);
}

RDB_EXTERN int
rdb_test_dbformat(void);

int
rdb_test_dbformat(void) {
  test_ikey_encode_decode();
  test_ikey_short_separator();
  test_ikey_shortest_successor();
  test_pkey_debug_string();
  test_ikey_debug_string();
  return 0;
}
