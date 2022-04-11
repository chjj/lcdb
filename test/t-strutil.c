/*!
 * strutil_test.c - strutil test for lcdb
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
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "util/strutil.h"
#include "util/testutil.h"

static int
check_number(uint64_t x, const char *y) {
  char z[21];
  ldb_encode_int(z, x, 0);
  return strcmp(z, y) == 0;
}

static void
test_encode_int(void) {
  ASSERT(check_number(0, "0"));
  ASSERT(check_number(1, "1"));
  ASSERT(check_number(9, "9"));

  ASSERT(check_number(10, "10"));
  ASSERT(check_number(11, "11"));
  ASSERT(check_number(19, "19"));
  ASSERT(check_number(99, "99"));

  ASSERT(check_number(100, "100"));
  ASSERT(check_number(109, "109"));
  ASSERT(check_number(190, "190"));
  ASSERT(check_number(123, "123"));
  ASSERT(check_number(12345678, "12345678"));

  ASSERT(UINT64_MAX == UINT64_C(18446744073709551615));

  ASSERT(check_number(UINT64_C(18446744073709551000), "18446744073709551000"));
  ASSERT(check_number(UINT64_C(18446744073709551600), "18446744073709551600"));
  ASSERT(check_number(UINT64_C(18446744073709551610), "18446744073709551610"));
  ASSERT(check_number(UINT64_C(18446744073709551614), "18446744073709551614"));
  ASSERT(check_number(UINT64_C(18446744073709551615), "18446744073709551615"));
}

static void
decode_int_test(uint64_t number, const char *padding) {
  char decimal_number[21];
  char input_string[128];
  const char *output;
  uint64_t result;
  char *input;

  ldb_encode_int(decimal_number, number, 0);

  sprintf(input_string, "%s%s", decimal_number, padding);

  input = input_string;
  output = input;

  ASSERT(ldb_decode_int(&result, &output));
  ASSERT(number == result);
  ASSERT(strlen(decimal_number) == strlen(input) - strlen(output));
  ASSERT(strlen(padding) == strlen(output));
}

static void
test_decode_int(void) {
  uint64_t i;

  decode_int_test(0, "");
  decode_int_test(1, "");
  decode_int_test(9, "");

  decode_int_test(10, "");
  decode_int_test(11, "");
  decode_int_test(19, "");
  decode_int_test(99, "");

  decode_int_test(100, "");
  decode_int_test(109, "");
  decode_int_test(190, "");
  decode_int_test(123, "");

  for (i = 0; i < 100; i++) {
    uint64_t large_number = UINT64_MAX - i;

    decode_int_test(large_number, "");
  }
}

static void
test_decode_int_with_padding(void) {
  uint64_t i;

  decode_int_test(0, " ");
  decode_int_test(1, "abc");
  decode_int_test(9, "x");

  decode_int_test(10, "_");
  decode_int_test(11, "\1\1\1");
  decode_int_test(19, "abc");
  decode_int_test(99, "padding");

  decode_int_test(100, " ");

  for (i = 0; i < 100; ++i) {
    uint64_t large_number = UINT64_MAX - i;

    decode_int_test(large_number, "pad");
  }
}

static void
decode_int_overflow_test(const char *input_string) {
  uint64_t result;
  ASSERT(0 == ldb_decode_int(&result, &input_string));
}

static void
test_decode_int_overflow(void) {
  decode_int_overflow_test("18446744073709551616");
  decode_int_overflow_test("18446744073709551617");
  decode_int_overflow_test("18446744073709551618");
  decode_int_overflow_test("18446744073709551619");
  decode_int_overflow_test("18446744073709551620");
  decode_int_overflow_test("18446744073709551621");
  decode_int_overflow_test("18446744073709551622");
  decode_int_overflow_test("18446744073709551623");
  decode_int_overflow_test("18446744073709551624");
  decode_int_overflow_test("18446744073709551625");
  decode_int_overflow_test("18446744073709551626");

  decode_int_overflow_test("18446744073709551700");

  decode_int_overflow_test("99999999999999999999");
}

static void
decode_int_no_digits_test(const char *input_string) {
  const char *output = input_string;
  uint64_t result;

  ASSERT(0 == ldb_decode_int(&result, &output));
  ASSERT(output == input_string);
}

static void
test_decode_int_no_digits(void) {
  decode_int_no_digits_test("");
  decode_int_no_digits_test(" ");
  decode_int_no_digits_test("a");
  decode_int_no_digits_test(" 123");
  decode_int_no_digits_test("a123");
  decode_int_no_digits_test("\001123");
  decode_int_no_digits_test("\177123");
  decode_int_no_digits_test("\377123");
}

static void
test_starts_with(void) {
  ASSERT(ldb_starts_with("foobar", "foo"));
  ASSERT(ldb_starts_with("foo", "foo"));
  ASSERT(!ldb_starts_with("zoobar", "foo"));
  ASSERT(!ldb_starts_with("fo", "foo"));
  ASSERT(!ldb_starts_with("", "foo"));
}

static void
test_basename(void) {
  ASSERT(strcmp(ldb_basename("/foo/bar"), "bar") == 0);
  ASSERT(strcmp(ldb_basename("/foo///bar"), "bar") == 0);
  ASSERT(strcmp(ldb_basename("./bar"), "bar") == 0);
  ASSERT(strcmp(ldb_basename("bar"), "bar") == 0);

#ifdef _WIN32
  ASSERT(strcmp(ldb_basename("\\foo/\\bar"), "bar") == 0);
  ASSERT(strcmp(ldb_basename("\\foo\\/bar"), "bar") == 0);
  ASSERT(strcmp(ldb_basename(".\\bar"), "bar") == 0);
  ASSERT(strcmp(ldb_basename("bar"), "bar") == 0);
#endif
}

static void
test_dirname(void) {
  char dir[128];

  ASSERT(!ldb_dirname(dir, 4, "/foo/bar"));
  ASSERT(ldb_dirname(dir, 5, "/foo/bar"));

  ASSERT(ldb_dirname(dir, 128, "/foo/bar"));
  ASSERT(strcmp(dir, "/foo") == 0);

  ASSERT(ldb_dirname(dir, 128, "/foo///bar"));
  ASSERT(strcmp(dir, "/foo") == 0);

  ASSERT(ldb_dirname(dir, 128, "./bar"));
  ASSERT(strcmp(dir, ".") == 0);

  ASSERT(ldb_dirname(dir, 128, "bar"));
  ASSERT(strcmp(dir, ".") == 0);

  ASSERT(ldb_dirname(dir, 128, "/bar"));
  ASSERT(strcmp(dir, "/") == 0);

  ASSERT(ldb_dirname(dir, 128, "/"));
  ASSERT(strcmp(dir, "/") == 0);

  ASSERT(ldb_dirname(dir, 128, ""));
  ASSERT(strcmp(dir, ".") == 0);

#ifdef _WIN32
  ASSERT(ldb_dirname(dir, 128, "/foo\\bar"));
  ASSERT(strcmp(dir, "/foo") == 0);

  ASSERT(ldb_dirname(dir, 128, "/foo/\\bar"));
  ASSERT(strcmp(dir, "/foo") == 0);

  ASSERT(ldb_dirname(dir, 128, "/foo\\/bar"));
  ASSERT(strcmp(dir, "/foo") == 0);
#endif
}

static void
test_join(void) {
  char path[128];

  ASSERT(!ldb_join(path, 7, "foo", "bar"));

#if defined(_WIN32)
  ASSERT(ldb_join(path, 8, "foo", "bar"));
  ASSERT(strcmp(path, "foo\\bar") == 0);

  ASSERT(ldb_join(path, 128, "/foo", "bar"));
  ASSERT(strcmp(path, "/foo\\bar") == 0);
#else
  ASSERT(ldb_join(path, 8, "foo", "bar"));
  ASSERT(strcmp(path, "foo/bar") == 0);

  ASSERT(ldb_join(path, 128, "/foo", "bar"));
  ASSERT(strcmp(path, "/foo/bar") == 0);
#endif
}

int
main(void) {
  test_encode_int();
  test_decode_int();
  test_decode_int_with_padding();
  test_decode_int_overflow();
  test_decode_int_no_digits();
  test_starts_with();
  test_basename();
  test_dirname();
  test_join();
  return 0;
}
