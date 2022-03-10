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
ikey_set(ldb_ikey_t *ikey, const char *ukey, uint64_t seq, ldb_valtype_t vt) {
  ldb_slice_t key = ldb_string(ukey);
  ldb_ikey_set(ikey, &key, seq, vt);
}

static ldb_buffer_t *
shortest_separator(ldb_buffer_t *start, const ldb_slice_t *limit) {
  ldb_comparator_t icmp;
  ldb_ikc_init(&icmp, ldb_bytewise_comparator);
  ldb_shortest_separator(&icmp, start, limit);
  return start;
}

static ldb_buffer_t *
short_successor(ldb_buffer_t *key) {
  ldb_comparator_t icmp;
  ldb_ikc_init(&icmp, ldb_bytewise_comparator);
  ldb_short_successor(&icmp, key);
  return key;
}

static void
test_key(const char *key, uint64_t seq, ldb_valtype_t vt) {
  ldb_slice_t k = ldb_string(key);
  ldb_ikey_t encoded;
  ldb_pkey_t decoded;

  ldb_ikey_init(&encoded);

  ldb_ikey_set(&encoded, &k, seq, vt);

  ASSERT(ldb_pkey_import(&decoded, &encoded));
  ASSERT(ldb_slice_equal(&k, &decoded.user_key));
  ASSERT(seq == decoded.sequence);
  ASSERT(vt == decoded.type);

  k = ldb_string("bar");

  ASSERT(!ldb_pkey_import(&decoded, &k));

  ldb_ikey_clear(&encoded);
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
      test_key(keys[k], seq[s], LDB_TYPE_VALUE);
      test_key("hello", 1, LDB_TYPE_DELETION);
    }
  }
}

static void
test_ikey_short_separator(void) {
  ldb_ikey_t k0, k1, k2;

  ldb_ikey_init(&k0);
  ldb_ikey_init(&k1);
  ldb_ikey_init(&k2);

  /* When user keys are same. */
  ikey_set(&k0, "foo", 100, LDB_TYPE_VALUE);
  ikey_set(&k1, "foo", 100, LDB_TYPE_VALUE);
  ikey_set(&k2, "foo", 99, LDB_TYPE_VALUE);
  ASSERT(ldb_buffer_equal(&k0, shortest_separator(&k1, &k2)));

  ikey_set(&k0, "foo", 100, LDB_TYPE_VALUE);
  ikey_set(&k1, "foo", 100, LDB_TYPE_VALUE);
  ikey_set(&k2, "foo", 101, LDB_TYPE_VALUE);
  ASSERT(ldb_buffer_equal(&k0, shortest_separator(&k1, &k2)));

  ikey_set(&k0, "foo", 100, LDB_TYPE_VALUE);
  ikey_set(&k1, "foo", 100, LDB_TYPE_VALUE);
  ikey_set(&k2, "foo", 100, LDB_TYPE_VALUE);
  ASSERT(ldb_buffer_equal(&k0, shortest_separator(&k1, &k2)));

  ikey_set(&k0, "foo", 100, LDB_TYPE_VALUE);
  ikey_set(&k1, "foo", 100, LDB_TYPE_VALUE);
  ikey_set(&k2, "foo", 100, LDB_TYPE_DELETION);
  ASSERT(ldb_buffer_equal(&k0, shortest_separator(&k1, &k2)));

  /* When user keys are misordered. */
  ikey_set(&k0, "foo", 100, LDB_TYPE_VALUE);
  ikey_set(&k1, "foo", 100, LDB_TYPE_VALUE);
  ikey_set(&k2, "bar", 99, LDB_TYPE_VALUE);
  ASSERT(ldb_buffer_equal(&k0, shortest_separator(&k1, &k2)));

  /* When user keys are different, but correctly ordered. */
  ikey_set(&k0, "g", LDB_MAX_SEQUENCE, LDB_VALTYPE_SEEK);
  ikey_set(&k1, "foo", 100, LDB_TYPE_VALUE);
  ikey_set(&k2, "hello", 200, LDB_TYPE_VALUE);
  ASSERT(ldb_buffer_equal(&k0, shortest_separator(&k1, &k2)));

  /* When start user key is prefix of limit user key. */
  ikey_set(&k0, "foo", 100, LDB_TYPE_VALUE);
  ikey_set(&k1, "foo", 100, LDB_TYPE_VALUE);
  ikey_set(&k2, "foobar", 200, LDB_TYPE_VALUE);
  ASSERT(ldb_buffer_equal(&k0, shortest_separator(&k1, &k2)));

  /* When limit user key is prefix of start user key. */
  ikey_set(&k0, "foobar", 100, LDB_TYPE_VALUE);
  ikey_set(&k1, "foobar", 100, LDB_TYPE_VALUE);
  ikey_set(&k2, "foo", 200, LDB_TYPE_VALUE);
  ASSERT(ldb_buffer_equal(&k0, shortest_separator(&k1, &k2)));

  ldb_ikey_clear(&k0);
  ldb_ikey_clear(&k1);
  ldb_ikey_clear(&k2);
}

static void
test_ikey_shortest_successor(void) {
  ldb_ikey_t k0, k1;

  ldb_ikey_init(&k0);
  ldb_ikey_init(&k1);

  ikey_set(&k0, "g", LDB_MAX_SEQUENCE, LDB_VALTYPE_SEEK);
  ikey_set(&k1, "foo", 100, LDB_TYPE_VALUE);
  ASSERT(ldb_buffer_equal(&k0, short_successor(&k1)));

  ikey_set(&k0, "\xff\xff", 100, LDB_TYPE_VALUE);
  ikey_set(&k1, "\xff\xff", 100, LDB_TYPE_VALUE);
  ASSERT(ldb_buffer_equal(&k0, short_successor(&k1)));

  ldb_ikey_clear(&k0);
  ldb_ikey_clear(&k1);
}

static void
test_pkey_debug_string(void) {
  ldb_slice_t key = ldb_string("The \"key\" in 'single quotes'");
  ldb_slice_t expect = ldb_string("'The \"key\" in 'single quotes'' @ 42 : 1");
  ldb_buffer_t str;
  ldb_pkey_t pkey;

  ldb_buffer_init(&str);

  ldb_pkey_init(&pkey, &key, 42, LDB_TYPE_VALUE);
  ldb_pkey_debug(&str, &pkey);

  ASSERT(ldb_slice_equal(&expect, &str));

  ldb_buffer_clear(&str);
}

static void
test_ikey_debug_string(void) {
  ldb_slice_t key = ldb_string("The \"key\" in 'single quotes'");
  ldb_slice_t expect = ldb_string("'The \"key\" in 'single quotes'' @ 42 : 1");
  ldb_slice_t bad = ldb_string("(bad)");
  ldb_buffer_t str;
  ldb_ikey_t ikey;

  ldb_ikey_init(&ikey);
  ldb_buffer_init(&str);

  ldb_ikey_set(&ikey, &key, 42, LDB_TYPE_VALUE);
  ldb_ikey_debug(&str, &ikey);

  ASSERT(ldb_slice_equal(&expect, &str));

  ldb_buffer_reset(&ikey);
  ldb_buffer_reset(&str);

  ldb_ikey_debug(&str, &ikey);

  ASSERT(ldb_slice_equal(&bad, &str));

  ldb_buffer_clear(&str);
  ldb_ikey_clear(&ikey);
}

LDB_EXTERN int
ldb_test_dbformat(void);

int
ldb_test_dbformat(void) {
  test_ikey_encode_decode();
  test_ikey_short_separator();
  test_ikey_shortest_successor();
  test_pkey_debug_string();
  test_ikey_debug_string();
  return 0;
}
