/*!
 * version_edit_test.c - version_edit test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "util/buffer.h"
#include "util/extern.h"
#include "util/internal.h"
#include "util/slice.h"
#include "util/testutil.h"

#include "version_edit.h"

static void
encode_and_decode(const rdb_vedit_t *edit) {
  rdb_buffer_t encoded, encoded2;
  rdb_vedit_t parsed;

  rdb_buffer_init(&encoded);
  rdb_buffer_init(&encoded2);
  rdb_vedit_init(&parsed);

  rdb_vedit_export(&encoded, edit);

  ASSERT(rdb_vedit_import(&parsed, &encoded));

  rdb_vedit_export(&encoded2, &parsed);

  ASSERT(rdb_buffer_equal(&encoded, &encoded2));

  rdb_vedit_clear(&parsed);
  rdb_buffer_clear(&encoded2);
  rdb_buffer_clear(&encoded);
}

static void
test_encode_decode(void) {
  static const uint64_t big = UINT64_C(1) << 50;
  rdb_slice_t s1 = rdb_string("foo");
  rdb_slice_t s2 = rdb_string("zoo");
  rdb_slice_t s3 = rdb_string("x");
  rdb_ikey_t k1, k2, k3;
  rdb_vedit_t edit;
  int i;

  rdb_vedit_init(&edit);

  rdb_ikey_init(&k1);
  rdb_ikey_init(&k2);
  rdb_ikey_init(&k3);

  for (i = 0; i < 4; i++) {
    encode_and_decode(&edit);

    rdb_ikey_set(&k1, &s1, big + 500 + i, RDB_TYPE_VALUE);
    rdb_ikey_set(&k2, &s2, big + 600 + i, RDB_TYPE_DELETION);
    rdb_ikey_set(&k3, &s3, big + 900 + i, RDB_TYPE_VALUE);

    rdb_vedit_add_file(&edit, 3, big + 300 + i, big + 400 + i, &k1, &k2);
    rdb_vedit_remove_file(&edit, 4, big + 700 + i);
    rdb_vedit_set_compact_pointer(&edit, i, &k3);
  }

  rdb_ikey_clear(&k1);
  rdb_ikey_clear(&k2);
  rdb_ikey_clear(&k3);

  rdb_vedit_set_comparator_name(&edit, "foo");
  rdb_vedit_set_log_number(&edit, big + 100);
  rdb_vedit_set_next_file(&edit, big + 200);
  rdb_vedit_set_last_sequence(&edit, big + 1000);

  encode_and_decode(&edit);

  rdb_vedit_clear(&edit);
}

RDB_EXTERN int
rdb_test_version_edit(void);

int
rdb_test_version_edit(void) {
  test_encode_decode();
  return 0;
}
