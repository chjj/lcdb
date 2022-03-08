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

RDB_EXTERN int
rdb_test_snappy(void);

int
rdb_test_snappy(void) {
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

  return 0;
}
