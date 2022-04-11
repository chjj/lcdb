/*!
 * snappy_test.c - snappy test for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on golang/snappy:
 *   Copyright (c) 2011 The Snappy-Go Authors. All rights reserved.
 *   https://github.com/golang/snappy
 *
 * See LICENSE for more information.
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "util/internal.h"
#include "util/snappy.h"
#include "util/testutil.h"

#include "data/snappy_data.h"

static void
test_snappy_1(void) {
  size_t size = 1 << 20;
  uint8_t *data = ldb_malloc(size);
  size_t encsize, decsize;
  uint8_t *enc, *dec;
  size_t i;

  for (i = 0; i < size; i++)
    data[i] = i & 0xff;

  ASSERT(snappy_encode_size(&encsize, size));

  enc = ldb_malloc(encsize);
  encsize = snappy_encode(enc, data, size);

  ASSERT(encsize > 0 && encsize < size);
  ASSERT(encsize == 53203);

  ASSERT(snappy_decode_size(&decsize, enc, encsize));
  ASSERT(decsize == size);

  dec = ldb_malloc(decsize);

  ASSERT(snappy_decode(dec, enc, encsize));
  ASSERT(memcmp(dec, data, size) == 0);

  ldb_free(data);
  ldb_free(enc);
  ldb_free(dec);
}

static void
test_snappy_2(void) {
  const uint8_t *data = snappy_test_input;
  size_t size = sizeof(snappy_test_input) - 1;
  size_t encsize, decsize;
  uint8_t *enc, *dec;

  ASSERT(snappy_encode_size(&encsize, size));

  enc = ldb_malloc(encsize);
  encsize = snappy_encode(enc, data, size);

  ASSERT(encsize > 0 && encsize < size);

  ASSERT(snappy_decode_size(&decsize, enc, encsize));
  ASSERT(decsize == size);

  dec = ldb_malloc(decsize);

  ASSERT(snappy_decode(dec, enc, encsize));
  ASSERT(memcmp(dec, data, size) == 0);

  ldb_free(enc);
  ldb_free(dec);
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

  dec = ldb_malloc(decsize);

  ASSERT(snappy_decode(dec, enc, encsize));
  ASSERT(memcmp(dec, data, size) == 0);

  ldb_free(dec);
}

int
main(void) {
  test_snappy_1();
  test_snappy_2();
  test_snappy_3();
  return 0;
}
