/*!
 * coding_test.c - coding test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#undef NDEBUG

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "buffer.h"
#include "coding.h"
#include "extern.h"
#include "slice.h"

static void
test_fixed32(void) {
  uint8_t *scratch = rdb_malloc(100000 * 4);
  const uint8_t *xp = scratch;
  uint8_t *zp = scratch;
  uint32_t v, a;

  for (v = 0; v < 100000; v++)
    zp = rdb_fixed32_write(zp, v);

  for (v = 0; v < 100000; v++) {
    a = rdb_fixed32_decode(xp);

    assert(v == a);

    xp += 4;
  }

  rdb_free(scratch);
}

static void
test_fixed64(void) {
  uint8_t scratch[64 * 3 * 8];
  const uint8_t *xp = scratch;
  uint8_t *zp = scratch;
  int power;

  for (power = 0; power <= 63; power++) {
    uint64_t v = UINT64_C(1) << power;

    zp = rdb_fixed64_write(zp, v - 1);
    zp = rdb_fixed64_write(zp, v + 0);
    zp = rdb_fixed64_write(zp, v + 1);
  }

  for (power = 0; power <= 63; power++) {
    uint64_t v = UINT64_C(1) << power;
    uint64_t a;

    a = rdb_fixed64_decode(xp);

    assert(v - 1 == a);

    xp += 8;

    a = rdb_fixed64_decode(xp);

    assert(v + 0 == a);

    xp += 8;

    a = rdb_fixed64_decode(xp);

    assert(v + 1 == a);

    xp += 8;
  }
}

/* Test that encoding routines generate little-endian encodings. */
static void
test_encoding_output(void) {
  uint8_t dst[8];

  rdb_fixed32_write(dst, 0x04030201);

  assert(0x01 == (int)(dst[0]));
  assert(0x02 == (int)(dst[1]));
  assert(0x03 == (int)(dst[2]));
  assert(0x04 == (int)(dst[3]));

  rdb_fixed64_write(dst, UINT64_C(0x0807060504030201));

  assert(0x01 == (int)(dst[0]));
  assert(0x02 == (int)(dst[1]));
  assert(0x03 == (int)(dst[2]));
  assert(0x04 == (int)(dst[3]));
  assert(0x05 == (int)(dst[4]));
  assert(0x06 == (int)(dst[5]));
  assert(0x07 == (int)(dst[6]));
  assert(0x08 == (int)(dst[7]));
}

static void
test_varint32(void) {
  uint8_t scratch[32 * 32 * 5];
  const uint8_t *xp = scratch;
  uint8_t *zp = scratch;
  uint32_t i;
  size_t xn;

  for (i = 0; i < (32 * 32); i++) {
    uint32_t v = (i / 32) << (i % 32);

    zp = rdb_varint32_write(zp, v);
  }

  xn = zp - xp;

  for (i = 0; i < (32 * 32); i++) {
    const uint8_t *sp = xp;
    uint32_t expected = (i / 32) << (i % 32);
    uint32_t actual;
    int rc;

    rc = rdb_varint32_read(&actual, &xp, &xn);

    assert(rc == 1);
    assert(expected == actual);
    assert(rdb_varint32_size(actual) == (size_t)(xp - sp));
  }

  assert(xn == 0);
}

static void
test_varint64(void) {
  /* Construct the list of values to check. */
  uint8_t scratch[(4 + 64 * 3) * 10];
  uint64_t values[4 + 64 * 3];
  const uint8_t *xp = scratch;
  uint8_t *zp = scratch;
  size_t len = 0;
  size_t i, xn;
  uint64_t k;

  /* Some special values. */
  values[len++] = 0;
  values[len++] = 100;
  values[len++] = ~UINT64_C(0);
  values[len++] = ~UINT64_C(0) - 1;

  for (k = 0; k < 64; k++) {
    /* Test values near powers of two. */
    uint64_t power = UINT64_C(1) << k;

    values[len++] = power;
    values[len++] = power - 1;
    values[len++] = power + 1;
  }

  for (i = 0; i < len; i++)
    zp = rdb_varint64_write(zp, values[i]);

  xn = zp - xp;

  for (i = 0; i < len; i++) {
    const uint8_t *sp = xp;
    uint64_t z;
    int rc;

    rc = rdb_varint64_read(&z, &xp, &xn);

    assert(rc == 1);
    assert(values[i] == z);
    assert(rdb_varint64_size(z) == (size_t)(xp - sp));
  }

  assert(xn == 0);
}

static void
test_varint32_overflow(void) {
  static const uint8_t input[] = {0x81, 0x82, 0x83, 0x84, 0x85, 0x11};
  const uint8_t *xp = input;
  size_t xn = sizeof(input);
  uint32_t z;
  int rc;

  rc = rdb_varint32_read(&z, &xp, &xn);

  assert(rc == 0);
}

static void
test_varint32_truncation(void) {
  uint32_t large_value = (1u << 31) + 100;
  uint8_t scratch[5];
  const uint8_t *xp;
  size_t i, xn, zn;
  uint8_t *zp;
  uint32_t z;
  int rc;

  zp = scratch;
  zn = rdb_varint32_write(zp, large_value) - zp;

  for (i = 0; i < zn - 1; i++) {
    xp = scratch;
    xn = i;
    rc = rdb_varint32_read(&z, &xp, &xn);

    assert(rc == 0);
  }

  xp = scratch;
  xn = zn;
  rc = rdb_varint32_read(&z, &xp, &xn);

  assert(rc == 1);
  assert(large_value == z);
}

static void
test_varint64_overflow(void) {
  static const uint8_t input[] = {0x81, 0x82, 0x83, 0x84, 0x85, 0x81,
                                  0x82, 0x83, 0x84, 0x85, 0x11};
  const uint8_t *xp = input;
  size_t xn = sizeof(input);
  uint64_t z;
  int rc;

  rc = rdb_varint64_read(&z, &xp, &xn);

  assert(rc == 0);
}

static void
test_varint64_truncation(void) {
  uint64_t large_value = (UINT64_C(1) << 63) + 100;
  uint8_t scratch[10];
  const uint8_t *xp;
  size_t i, xn, zn;
  uint8_t *zp;
  uint64_t z;
  int rc;

  zp = scratch;
  zn = rdb_varint64_write(zp, large_value) - zp;

  for (i = 0; i < zn - 1; i++) {
    xp = scratch;
    xn = i;
    rc = rdb_varint64_read(&z, &xp, &xn);

    assert(rc == 0);
  }

  xp = scratch;
  xn = zn;
  rc = rdb_varint64_read(&z, &xp, &xn);

  assert(rc == 1);
  assert(large_value == z);
}

static void
test_strings(void) {
  static const char *x = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                         "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                         "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                         "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
                         "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";

  rdb_slice_t k, input;
  rdb_buffer_t s;

  rdb_buffer_init(&s);

  k = rdb_string(""); rdb_slice_export(&s, &k);
  k = rdb_string("foo"); rdb_slice_export(&s, &k);
  k = rdb_string("bar"); rdb_slice_export(&s, &k);
  k = rdb_string(x); rdb_slice_export(&s, &k);

  input = s;

  assert(rdb_slice_slurp(&k, &input));
  assert(k.size == 0);

  assert(rdb_slice_slurp(&k, &input));
  assert(k.size == 3);
  assert(memcmp(k.data, "foo", 3) == 0);

  assert(rdb_slice_slurp(&k, &input));
  assert(k.size == 3);
  assert(memcmp(k.data, "bar", 3) == 0);

  assert(rdb_slice_slurp(&k, &input));
  assert(k.size == 200);
  assert(memcmp(k.data, x, 200) == 0);

  assert(input.size == 0);
  assert(!rdb_slice_slurp(&k, &input));

  rdb_buffer_clear(&s);
}

RDB_EXTERN int
rdb_test_coding(void);

int
rdb_test_coding(void) {
  test_fixed32();
  test_fixed64();
  test_encoding_output();
  test_varint32();
  test_varint64();
  test_varint32_overflow();
  test_varint32_truncation();
  test_varint64_overflow();
  test_varint64_truncation();
  test_strings();
  return 0;
}
