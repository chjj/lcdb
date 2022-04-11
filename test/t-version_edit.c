/*!
 * version_edit_test.c - version_edit test for lcdb
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

#include "util/buffer.h"
#include "util/internal.h"
#include "util/slice.h"
#include "util/testutil.h"

#include "version_edit.h"

static void
encode_and_decode(const ldb_vedit_t *edit) {
  ldb_buffer_t encoded, encoded2;
  ldb_vedit_t parsed;

  ldb_buffer_init(&encoded);
  ldb_buffer_init(&encoded2);
  ldb_vedit_init(&parsed);

  ldb_vedit_export(&encoded, edit);

  ASSERT(ldb_vedit_import(&parsed, &encoded));

  ldb_vedit_export(&encoded2, &parsed);

  ASSERT(ldb_buffer_equal(&encoded, &encoded2));

  ldb_vedit_clear(&parsed);
  ldb_buffer_clear(&encoded2);
  ldb_buffer_clear(&encoded);
}

static void
test_encode_decode(void) {
  static const uint64_t big = UINT64_C(1) << 50;
  ldb_slice_t s1 = ldb_string("foo");
  ldb_slice_t s2 = ldb_string("zoo");
  ldb_slice_t s3 = ldb_string("x");
  ldb_ikey_t k1, k2, k3;
  ldb_vedit_t edit;
  int i;

  ldb_vedit_init(&edit);

  ldb_ikey_init(&k1);
  ldb_ikey_init(&k2);
  ldb_ikey_init(&k3);

  for (i = 0; i < 4; i++) {
    encode_and_decode(&edit);

    ldb_ikey_set(&k1, &s1, big + 500 + i, LDB_TYPE_VALUE);
    ldb_ikey_set(&k2, &s2, big + 600 + i, LDB_TYPE_DELETION);
    ldb_ikey_set(&k3, &s3, big + 900 + i, LDB_TYPE_VALUE);

    ldb_vedit_add_file(&edit, 3, big + 300 + i, big + 400 + i, &k1, &k2);
    ldb_vedit_remove_file(&edit, 4, big + 700 + i);
    ldb_vedit_set_compact_pointer(&edit, i, &k3);
  }

  ldb_ikey_clear(&k1);
  ldb_ikey_clear(&k2);
  ldb_ikey_clear(&k3);

  ldb_vedit_set_comparator_name(&edit, "foo");
  ldb_vedit_set_log_number(&edit, big + 100);
  ldb_vedit_set_next_file(&edit, big + 200);
  ldb_vedit_set_last_sequence(&edit, big + 1000);

  encode_and_decode(&edit);

  ldb_vedit_clear(&edit);
}

int
main(void) {
  test_encode_decode();
  return 0;
}
