/*!
 * fuzzer.c - fuzzer for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on chromium/chromium:
 *   Copyright 2018 The Chromium Authors. All rights reserved.
 *   https://github.com/chromium/chromium
 *
 * See LICENSE for more information.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "util/buffer.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/vector.h"

#include "db_impl.h"

/*
 * Constants
 */

/* We need to use keys and values both shorter and longer
   than 128 bytes in order to cover both fast and slow
   paths in decode_entry(). */
#define MAX_KEYLEN 256
#define MAX_VALLEN 256

/*
 * Macros
 */

#define FUZZING_ASSERT(condition) do {                                 \
  if (!(condition)) {                                                  \
    fprintf(stderr, "%s\n", "Fuzzing Assertion Failure: " #condition); \
    abort();                                                           \
  }                                                                    \
} while (0)

/*
 * FuzzedDataProvider
 */

static int
consume_bool(ldb_slice_t *x) {
  int z;

  if (x->size == 0)
    return 0;

  z = x->data[0] & 1;

  x->data += 1;
  x->size -= 1;

  return z;
}

static ldb_buffer_t *
consume_string(ldb_slice_t *x, size_t max) {
  ldb_buffer_t *z = ldb_malloc(sizeof(ldb_buffer_t));
  size_t alloc = LDB_MIN(max, x->size);
  uint8_t *data = ldb_malloc(alloc + 1);
  size_t size = 0;

  while (size < max && x->size > 0) {
    int ch = x->data[0];

    x->data += 1;
    x->size -= 1;

    if (ch == '\\' && x->size > 0) {
      ch = x->data[0];

      x->data += 1;
      x->size -= 1;

      if (ch != '\\')
        break;
    }

    data[size++] = ch;
  }

  ldb_buffer_init(z);
  ldb_buffer_set(z, data, size);
  ldb_free(data);

  return z;
}

/*
 * Fuzzer
 */

int
LLVMFuzzerTestOneInput(const uint8_t *data, size_t size);

int
LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
  ldb_writeopt_t write_opts;
  ldb_readopt_t read_opts;
  ldb_dbopt_t open_opts;
  ldb_slice_t provider;
  ldb_vector_t used;
  ldb_t *db;
  size_t i;
  int rc;

  /* Reject too long inputs as they may cause non
     actionable timeouts issues. */
  if (size > 128 * 1024)
    return 0;

  provider = ldb_slice(data, size);

  open_opts = *ldb_dbopt_default;
  open_opts.create_if_missing = 1;
  open_opts.paranoid_checks = consume_bool(&provider);
  open_opts.reuse_logs = 0;

  read_opts = *ldb_readopt_default;
  read_opts.verify_checksums = consume_bool(&provider);
  read_opts.fill_cache = consume_bool(&provider);

  write_opts = *ldb_writeopt_default;
  write_opts.sync = consume_bool(&provider);

  db = NULL;
  rc = ldb_open("leveldbfuzztest", &open_opts, &db);

  FUZZING_ASSERT(rc == LDB_OK);

  /* Put a couple constant values which must be successfully written. */
  {
    ldb_slice_t key, val;

    key = ldb_string("key1");
    val = ldb_string("val1");

    FUZZING_ASSERT(ldb_put(db, &key, &val, &write_opts) == LDB_OK);

    key = ldb_string("key2");
    val = ldb_string("val2");

    FUZZING_ASSERT(ldb_put(db, &key, &val, &write_opts) == LDB_OK);
  }

  /* Split the data into a sequence of (key, val) strings
     and put those in. Also collect both keys and values
     to be used as keys for retrieval below. */
  ldb_vector_init(&used);

  while (provider.size > 0) {
    ldb_buffer_t *key = consume_string(&provider, MAX_KEYLEN);
    ldb_buffer_t *val = consume_string(&provider, MAX_VALLEN);

    (void)ldb_put(db, key, val, &write_opts);

    ldb_vector_push(&used, key);
    ldb_vector_push(&used, val);
  }

  /* Use all the strings we have extracted from the data
     previously as the keys. */
  for (i = 0; i < used.length; i++) {
    const ldb_slice_t *key = used.items[i];
    ldb_slice_t val;

    rc = ldb_get(db, key, &val, &read_opts);

    if (rc == LDB_OK)
      ldb_free(val.data);
  }

  /* Delete all keys previously written to the database. */
  for (i = 0; i < used.length; i++) {
    const ldb_slice_t *key = used.items[i];

    (void)ldb_del(db, key, &write_opts);
  }

  /* Free up memory. */
  for (i = 0; i < used.length; i++) {
    ldb_buffer_t *key = used.items[i];

    ldb_buffer_clear(key);
    ldb_free(key);
  }

  ldb_vector_clear(&used);

  /* Close database. */
  ldb_close(db);
  db = NULL;

  return 0;
}
