/*!
 * write_batch_test.c - write_batch test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "table/iterator.h"

#include "util/buffer.h"
#include "util/comparator.h"
#include "util/extern.h"
#include "util/internal.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/testutil.h"

#include "dbformat.h"
#include "memtable.h"
#include "write_batch.h"

#define ASSERT_EQ(x, y) ASSERT(strcmp(x, y) == 0)

static char *
print_contents(rdb_batch_t *b) {
  static uint8_t buf[1024];
  rdb_comparator_t cmp;
  rdb_memtable_t *mem;
  rdb_buffer_t state;
  rdb_iter_t *iter;
  int count = 0;
  int rc;

  rdb_ikc_init(&cmp, rdb_bytewise_comparator);

  mem = rdb_memtable_create(&cmp);

  rdb_memtable_ref(mem);

  rc = rdb_batch_insert_into(b, mem);

  iter = rdb_memiter_create(mem);

  rdb_buffer_rwset(&state, buf, sizeof(buf));

  for (rdb_iter_seek_first(iter); rdb_iter_valid(iter); rdb_iter_next(iter)) {
    rdb_slice_t key = rdb_iter_key(iter);
    rdb_slice_t val = rdb_iter_value(iter);
    rdb_pkey_t ikey;

    ASSERT(rdb_pkey_import(&ikey, &key));

    switch (ikey.type) {
      case RDB_TYPE_VALUE:
        rdb_buffer_string(&state, "Put(");
        rdb_buffer_escape(&state, &ikey.user_key);
        rdb_buffer_string(&state, ", ");
        rdb_buffer_escape(&state, &val);
        rdb_buffer_string(&state, ")");
        count++;
        break;
      case RDB_TYPE_DELETION:
        rdb_buffer_string(&state, "Delete(");
        rdb_buffer_escape(&state, &ikey.user_key);
        rdb_buffer_string(&state, ")");
        count++;
        break;
    }

    rdb_buffer_string(&state, "@");
    rdb_buffer_number(&state, ikey.sequence);
  }

  rdb_iter_destroy(iter);

  if (rc != RDB_OK)
    rdb_buffer_string(&state, "ParseError()");
  else if (count != rdb_batch_count(b))
    rdb_buffer_string(&state, "CountMismatch()");

  rdb_buffer_push(&state, '\0');

  rdb_memtable_unref(mem);

  return (char *)state.data;
}

static void
test_batch_empty(void) {
  rdb_batch_t batch;

  rdb_batch_init(&batch);

  ASSERT_EQ("", print_contents(&batch));
  ASSERT(0 == rdb_batch_count(&batch));

  rdb_batch_clear(&batch);
}

static void
test_batch_multiple(void) {
  rdb_slice_t key, val;
  rdb_batch_t batch;

  rdb_batch_init(&batch);

  key = rdb_string("foo");
  val = rdb_string("bar");
  rdb_batch_put(&batch, &key, &val);

  key = rdb_string("box");
  rdb_batch_del(&batch, &key);

  key = rdb_string("baz");
  val = rdb_string("boo");
  rdb_batch_put(&batch, &key, &val);

  rdb_batch_set_sequence(&batch, 100);

  ASSERT(100 == rdb_batch_sequence(&batch));
  ASSERT(3 == rdb_batch_count(&batch));

  ASSERT_EQ("Put(baz, boo)@102"
            "Delete(box)@101"
            "Put(foo, bar)@100",
            print_contents(&batch));

  rdb_batch_clear(&batch);
}

static void
test_batch_corruption(void) {
  rdb_slice_t key, val, contents;
  rdb_batch_t batch;

  rdb_batch_init(&batch);

  key = rdb_string("foo");
  val = rdb_string("bar");
  rdb_batch_put(&batch, &key, &val);

  key = rdb_string("box");
  rdb_batch_del(&batch, &key);

  rdb_batch_set_sequence(&batch, 200);

  contents = rdb_batch_contents(&batch);
  contents.size -= 1;

  rdb_batch_set_contents(&batch, &contents);

  ASSERT_EQ("Put(foo, bar)@200"
            "ParseError()",
            print_contents(&batch));

  rdb_batch_clear(&batch);
}

static void
test_batch_append(void) {
  rdb_slice_t key, val;
  rdb_batch_t b1, b2;

  rdb_batch_init(&b1);
  rdb_batch_init(&b2);

  rdb_batch_set_sequence(&b1, 200);
  rdb_batch_set_sequence(&b2, 300);

  rdb_batch_append(&b1, &b2);

  ASSERT_EQ("", print_contents(&b1));

  key = rdb_string("a");
  val = rdb_string("va");
  rdb_batch_put(&b2, &key, &val);

  rdb_batch_append(&b1, &b2);

  ASSERT_EQ("Put(a, va)@200", print_contents(&b1));

  rdb_batch_reset(&b2);

  key = rdb_string("b");
  val = rdb_string("vb");
  rdb_batch_put(&b2, &key, &val);

  rdb_batch_append(&b1, &b2);

  ASSERT_EQ("Put(a, va)@200"
            "Put(b, vb)@201",
            print_contents(&b1));

  key = rdb_string("foo");
  rdb_batch_del(&b2, &key);

  rdb_batch_append(&b1, &b2);

  ASSERT_EQ("Put(a, va)@200"
            "Put(b, vb)@202"
            "Put(b, vb)@201"
            "Delete(foo)@203",
            print_contents(&b1));

  rdb_batch_clear(&b1);
  rdb_batch_clear(&b2);
}

static void
test_batch_approximate_size(void) {
  rdb_slice_t key, val;
  rdb_batch_t batch;
  size_t empty_size;
  size_t one_key_size;
  size_t two_keys_size;
  size_t post_delete_size;

  rdb_batch_init(&batch);

  empty_size = rdb_batch_approximate_size(&batch);

  key = rdb_string("foo");
  val = rdb_string("bar");
  rdb_batch_put(&batch, &key, &val);

  one_key_size = rdb_batch_approximate_size(&batch);

  ASSERT(empty_size < one_key_size);

  key = rdb_string("baz");
  val = rdb_string("boo");
  rdb_batch_put(&batch, &key, &val);

  two_keys_size = rdb_batch_approximate_size(&batch);

  ASSERT(one_key_size < two_keys_size);

  key = rdb_string("box");
  rdb_batch_del(&batch, &key);

  post_delete_size = rdb_batch_approximate_size(&batch);

  ASSERT(two_keys_size < post_delete_size);

  rdb_batch_clear(&batch);
}

RDB_EXTERN int
rdb_test_write_batch(void);

int
rdb_test_write_batch(void) {
  test_batch_empty();
  test_batch_multiple();
  test_batch_corruption();
  test_batch_append();
  test_batch_approximate_size();
  return 0;
}
