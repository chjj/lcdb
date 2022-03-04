/*!
 * snappy_test.c - snappy test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#undef NDEBUG

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "extern.h"
#include "snappy.h"

RDB_EXTERN int
rdb_test_snappy(void);

int
rdb_test_snappy(void) {
  size_t size = 1 << 20;
  uint8_t *data = malloc(size);
  int rc, encsize, decsize;
  uint8_t *enc, *dec;
  size_t i;

  assert(data != NULL);

  for (i = 0; i < size; i++)
    data[i] = i & 0xff;

  encsize = snappy_encode_size(size);

  assert(encsize >= 0);

  enc = malloc(encsize);

  assert(enc != NULL);

  encsize = snappy_encode(enc, data, size);

  assert(encsize > 0 && (size_t)encsize != size);
  assert(encsize == 53203);

  decsize = snappy_decode_size(enc, encsize);

  assert(decsize >= 0);
  assert((size_t)decsize == size);

  dec = malloc(decsize);

  assert(dec != NULL);

  rc = snappy_decode(dec, enc, encsize);

  assert(rc == 1);
  assert(memcmp(dec, data, size) == 0);

  free(data);
  free(enc);
  free(dec);

  return 0;
}
