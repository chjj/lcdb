/*!
 * snappy_test.c - snappy test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "extern.h"
#include "internal.h"
#include "snappy.h"
#include "testutil.h"

#include "snappy_data.h"

static void
test_snappy_1(void) {
  size_t size = 1 << 20;
  uint8_t *data = rdb_malloc(size);
  size_t encsize, decsize;
  uint8_t *enc, *dec;
  size_t i;

  for (i = 0; i < size; i++)
    data[i] = i & 0xff;

  ASSERT(snappy_encode_size(&encsize, size));

  enc = rdb_malloc(encsize);
  encsize = snappy_encode(enc, data, size);

  ASSERT(encsize > 0 && encsize < size);
  ASSERT(encsize == 53203);

  ASSERT(snappy_decode_size(&decsize, enc, encsize));
  ASSERT(decsize == size);

  dec = rdb_malloc(decsize);

  ASSERT(snappy_decode(dec, enc, encsize));
  ASSERT(memcmp(dec, data, size) == 0);

  rdb_free(data);
  rdb_free(enc);
  rdb_free(dec);
}

static void
test_snappy_2(void) {
  const uint8_t *data = snappy_test_input;
  size_t size = sizeof(snappy_test_input) - 1;
  size_t encsize, decsize;
  uint8_t *enc, *dec;

  ASSERT(snappy_encode_size(&encsize, size));

  enc = rdb_malloc(encsize);
  encsize = snappy_encode(enc, data, size);

  ASSERT(encsize > 0 && encsize < size);

  ASSERT(snappy_decode_size(&decsize, enc, encsize));
  ASSERT(decsize == size);

  dec = rdb_malloc(decsize);

  ASSERT(snappy_decode(dec, enc, encsize));
  ASSERT(memcmp(dec, data, size) == 0);

  rdb_free(enc);
  rdb_free(dec);
}

static void
test_snappy_3(void) {
  const uint8_t *data = snappy_test_input;
  size_t size = sizeof(snappy_test_input) - 1;
  const uint8_t *enc = snappy_test_output;
  size_t encsize = sizeof(snappy_test_output);
  size_t decsize;
  uint8_t *dec;

  ASSERT(snappy_decode_size(&decsize, enc, encsize));
  ASSERT(decsize == size);

  dec = rdb_malloc(decsize);

  ASSERT(snappy_decode(dec, enc, encsize));
  ASSERT(memcmp(dec, data, size) == 0);

  rdb_free(dec);
}

RDB_EXTERN int
rdb_test_snappy(void);

int
rdb_test_snappy(void) {
  test_snappy_1();
  test_snappy_2();
  test_snappy_3();
  return 0;
}
