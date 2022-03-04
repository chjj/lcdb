/*!
 * strutil_test.c - strutil test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#undef NDEBUG

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "extern.h"
#include "strutil.h"

static int
check_number(uint64_t x, const char *y) {
  char z[21];
  rdb_encode_int(z, x, 0);
  return strcmp(z, y) == 0;
}

static void
test_encode_int(void) {
  assert(check_number(0, "0"));
  assert(check_number(1, "1"));
  assert(check_number(9, "9"));

  assert(check_number(10, "10"));
  assert(check_number(11, "11"));
  assert(check_number(19, "19"));
  assert(check_number(99, "99"));

  assert(check_number(100, "100"));
  assert(check_number(109, "109"));
  assert(check_number(190, "190"));
  assert(check_number(123, "123"));
  assert(check_number(12345678, "12345678"));

  assert(UINT64_MAX == UINT64_C(18446744073709551615));

  assert(check_number(UINT64_C(18446744073709551000), "18446744073709551000"));
  assert(check_number(UINT64_C(18446744073709551600), "18446744073709551600"));
  assert(check_number(UINT64_C(18446744073709551610), "18446744073709551610"));
  assert(check_number(UINT64_C(18446744073709551614), "18446744073709551614"));
  assert(check_number(UINT64_C(18446744073709551615), "18446744073709551615"));
}

static void
decode_int_test(uint64_t number, const char *padding) {
  char decimal_number[21];
  char input_string[128];
  const char *output;
  uint64_t result;
  char *input;

  rdb_encode_int(decimal_number, number, 0);

  sprintf(input_string, "%s%s", decimal_number, padding);

  input = input_string;
  output = input;

  assert(rdb_decode_int(&result, &output));
  assert(number == result);
  assert(strlen(decimal_number) == strlen(input) - strlen(output));
  assert(strlen(padding) == strlen(output));
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
  assert(0 == rdb_decode_int(&result, &input_string));
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

  assert(0 == rdb_decode_int(&result, &output));
  assert(output == input_string);
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
  assert(rdb_starts_with("foobar", "foo"));
  assert(rdb_starts_with("foo", "foo"));
  assert(!rdb_starts_with("zoobar", "foo"));
  assert(!rdb_starts_with("fo", "foo"));
  assert(!rdb_starts_with("", "foo"));
}

static void
test_basename(void) {
  assert(strcmp(rdb_basename("/foo/bar"), "bar") == 0);
  assert(strcmp(rdb_basename("/foo///bar"), "bar") == 0);
  assert(strcmp(rdb_basename("./bar"), "bar") == 0);
  assert(strcmp(rdb_basename("bar"), "bar") == 0);

#ifdef _WIN32
  assert(strcmp(rdb_basename("\\foo/\\bar"), "bar") == 0);
  assert(strcmp(rdb_basename("\\foo\\/bar"), "bar") == 0);
  assert(strcmp(rdb_basename(".\\bar"), "bar") == 0);
  assert(strcmp(rdb_basename("bar"), "bar") == 0);
#endif
}

static void
test_dirname(void) {
  char dir[128];

  assert(!rdb_dirname(dir, 4, "/foo/bar"));
  assert(rdb_dirname(dir, 5, "/foo/bar"));

  assert(rdb_dirname(dir, 128, "/foo/bar"));
  assert(strcmp(dir, "/foo") == 0);

  assert(rdb_dirname(dir, 128, "/foo///bar"));
  assert(strcmp(dir, "/foo") == 0);

  assert(rdb_dirname(dir, 128, "./bar"));
  assert(strcmp(dir, ".") == 0);

  assert(rdb_dirname(dir, 128, "bar"));
  assert(strcmp(dir, ".") == 0);

  assert(rdb_dirname(dir, 128, "/bar"));
  assert(strcmp(dir, "/") == 0);

  assert(rdb_dirname(dir, 128, "/"));
  assert(strcmp(dir, "/") == 0);

  assert(rdb_dirname(dir, 128, ""));
  assert(strcmp(dir, ".") == 0);

#ifdef _WIN32
  assert(rdb_dirname(dir, 128, "/foo\\bar"));
  assert(strcmp(dir, "/foo") == 0);

  assert(rdb_dirname(dir, 128, "/foo/\\bar"));
  assert(strcmp(dir, "/foo") == 0);

  assert(rdb_dirname(dir, 128, "/foo\\/bar"));
  assert(strcmp(dir, "/foo") == 0);
#endif
}

static void
test_join(void) {
  char path[128];

  assert(!rdb_join(path, 7, "foo", "bar"));

#if defined(_WIN32)
  assert(rdb_join(path, 8, "foo", "bar"));
  assert(strcmp(path, "foo\\bar") == 0);

  assert(rdb_join(path, 128, "/foo", "bar"));
  assert(strcmp(path, "/foo\\bar") == 0);
#else
  assert(rdb_join(path, 8, "foo", "bar"));
  assert(strcmp(path, "foo/bar") == 0);

  assert(rdb_join(path, 128, "/foo", "bar"));
  assert(strcmp(path, "/foo/bar") == 0);
#endif
}

RDB_EXTERN int
rdb_test_strutil(void);

int
rdb_test_strutil(void) {
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
