/*!
 * t-write_batch.c - write_batch test for lcdb
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

#include "table/iterator.h"

#include "util/buffer.h"
#include "util/comparator.h"
#include "util/internal.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/testutil.h"

#include "dbformat.h"
#include "memtable.h"
#include "write_batch.h"

static char *
print_contents(ldb_batch_t *b) {
  static uint8_t buf[1024];
  ldb_comparator_t cmp;
  ldb_memtable_t *mem;
  ldb_buffer_t state;
  ldb_iter_t *iter;
  int count = 0;
  int rc;

  ldb_ikc_init(&cmp, ldb_bytewise_comparator);

  mem = ldb_memtable_create(&cmp);

  ldb_memtable_ref(mem);

  rc = ldb_batch_insert_into(b, mem);

  iter = ldb_memiter_create(mem);

  ldb_buffer_rwset(&state, buf, sizeof(buf));

  for (ldb_iter_first(iter); ldb_iter_valid(iter); ldb_iter_next(iter)) {
    ldb_slice_t key = ldb_iter_key(iter);
    ldb_slice_t val = ldb_iter_value(iter);
    ldb_pkey_t ikey;

    ASSERT(ldb_pkey_import(&ikey, &key));

    switch (ikey.type) {
      case LDB_TYPE_VALUE:
        ldb_buffer_string(&state, "Put(");
        ldb_buffer_escape(&state, &ikey.user_key);
        ldb_buffer_string(&state, ", ");
        ldb_buffer_escape(&state, &val);
        ldb_buffer_string(&state, ")");
        count++;
        break;
      case LDB_TYPE_DELETION:
        ldb_buffer_string(&state, "Delete(");
        ldb_buffer_escape(&state, &ikey.user_key);
        ldb_buffer_string(&state, ")");
        count++;
        break;
    }

    ldb_buffer_string(&state, "@");
    ldb_buffer_number(&state, ikey.sequence);
  }

  ldb_iter_destroy(iter);

  if (rc != LDB_OK)
    ldb_buffer_string(&state, "ParseError()");
  else if (count != ldb_batch_count(b))
    ldb_buffer_string(&state, "CountMismatch()");

  ldb_buffer_push(&state, '\0');

  ldb_memtable_unref(mem);

  return (char *)state.data;
}

static void
test_batch_empty(void) {
  ldb_batch_t batch;

  ldb_batch_init(&batch);

  ASSERT_EQ("", print_contents(&batch));
  ASSERT(0 == ldb_batch_count(&batch));

  ldb_batch_clear(&batch);
}

static void
test_batch_multiple(void) {
  ldb_slice_t key, val;
  ldb_batch_t batch;

  ldb_batch_init(&batch);

  key = ldb_string("foo");
  val = ldb_string("bar");
  ldb_batch_put(&batch, &key, &val);

  key = ldb_string("box");
  ldb_batch_del(&batch, &key);

  key = ldb_string("baz");
  val = ldb_string("boo");
  ldb_batch_put(&batch, &key, &val);

  ldb_batch_set_sequence(&batch, 100);

  ASSERT(100 == ldb_batch_sequence(&batch));
  ASSERT(3 == ldb_batch_count(&batch));

  ASSERT_EQ("Put(baz, boo)@102"
            "Delete(box)@101"
            "Put(foo, bar)@100",
            print_contents(&batch));

  ldb_batch_clear(&batch);
}

static void
test_batch_corruption(void) {
  ldb_slice_t key, val, contents;
  ldb_batch_t batch;

  ldb_batch_init(&batch);

  key = ldb_string("foo");
  val = ldb_string("bar");
  ldb_batch_put(&batch, &key, &val);

  key = ldb_string("box");
  ldb_batch_del(&batch, &key);

  ldb_batch_set_sequence(&batch, 200);

  contents = ldb_batch_contents(&batch);
  contents.size -= 1;

  ldb_batch_set_contents(&batch, &contents);

  ASSERT_EQ("Put(foo, bar)@200"
            "ParseError()",
            print_contents(&batch));

  ldb_batch_clear(&batch);
}

static void
test_batch_append(void) {
  ldb_slice_t key, val;
  ldb_batch_t b1, b2;

  ldb_batch_init(&b1);
  ldb_batch_init(&b2);

  ldb_batch_set_sequence(&b1, 200);
  ldb_batch_set_sequence(&b2, 300);

  ldb_batch_append(&b1, &b2);

  ASSERT_EQ("", print_contents(&b1));

  key = ldb_string("a");
  val = ldb_string("va");
  ldb_batch_put(&b2, &key, &val);

  ldb_batch_append(&b1, &b2);

  ASSERT_EQ("Put(a, va)@200", print_contents(&b1));

  ldb_batch_reset(&b2);

  key = ldb_string("b");
  val = ldb_string("vb");
  ldb_batch_put(&b2, &key, &val);

  ldb_batch_append(&b1, &b2);

  ASSERT_EQ("Put(a, va)@200"
            "Put(b, vb)@201",
            print_contents(&b1));

  key = ldb_string("foo");
  ldb_batch_del(&b2, &key);

  ldb_batch_append(&b1, &b2);

  ASSERT_EQ("Put(a, va)@200"
            "Put(b, vb)@202"
            "Put(b, vb)@201"
            "Delete(foo)@203",
            print_contents(&b1));

  ldb_batch_clear(&b1);
  ldb_batch_clear(&b2);
}

static void
test_batch_approximate_size(void) {
  ldb_slice_t key, val;
  ldb_batch_t batch;
  size_t empty_size;
  size_t one_key_size;
  size_t two_keys_size;
  size_t post_delete_size;

  ldb_batch_init(&batch);

  empty_size = ldb_batch_approximate_size(&batch);

  key = ldb_string("foo");
  val = ldb_string("bar");
  ldb_batch_put(&batch, &key, &val);

  one_key_size = ldb_batch_approximate_size(&batch);

  ASSERT(empty_size < one_key_size);

  key = ldb_string("baz");
  val = ldb_string("boo");
  ldb_batch_put(&batch, &key, &val);

  two_keys_size = ldb_batch_approximate_size(&batch);

  ASSERT(one_key_size < two_keys_size);

  key = ldb_string("box");
  ldb_batch_del(&batch, &key);

  post_delete_size = ldb_batch_approximate_size(&batch);

  ASSERT(two_keys_size < post_delete_size);

  ldb_batch_clear(&batch);
}

int
main(void) {
  test_batch_empty();
  test_batch_multiple();
  test_batch_corruption();
  test_batch_append();
  test_batch_approximate_size();
  return 0;
}
