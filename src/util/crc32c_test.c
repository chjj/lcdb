/*!
 * crc32c_test.c - crc32c test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "crc32c.h"
#include "extern.h"
#include "testutil.h"

#define ldb_crc32c_str(x, y) ldb_crc32c_value((const uint8_t *)x, y)
#define ldb_crc32c_extstr(x, y, z) ldb_crc32c_extend(x, (const uint8_t *)y, z)

static void
test_standard_results(void) {
  /* From rfc3720 section B.4. */
  static const uint8_t data[48] = {
    0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
    0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
  };

  uint8_t buf[32];
  int i;

  memset(buf, 0, sizeof(buf));

  ASSERT(0x8a9136aa == ldb_crc32c_value(buf, sizeof(buf)));

  memset(buf, 0xff, sizeof(buf));

  ASSERT(0x62a8ab43 == ldb_crc32c_value(buf, sizeof(buf)));

  for (i = 0; i < 32; i++)
    buf[i] = i;

  ASSERT(0x46dd794e == ldb_crc32c_value(buf, sizeof(buf)));

  for (i = 0; i < 32; i++)
    buf[i] = 31 - i;

  ASSERT(0x113fdb5c == ldb_crc32c_value(buf, sizeof(buf)));
  ASSERT(0xd9963a56 == ldb_crc32c_value(data, sizeof(data)));
}

static void
test_values(void) {
  ASSERT(ldb_crc32c_str("a", 1) != ldb_crc32c_str("foo", 3));
}

static void
test_extend(void) {
  uint32_t v = ldb_crc32c_str("hello ", 6);

  ASSERT(ldb_crc32c_str("hello world", 11) == ldb_crc32c_extstr(v, "world", 5));
}

static void
test_mask(void) {
  uint32_t crc = ldb_crc32c_str("foo", 3);

  ASSERT(crc != ldb_crc32c_mask(crc));
  ASSERT(crc != ldb_crc32c_mask(ldb_crc32c_mask(crc)));
  ASSERT(crc == ldb_crc32c_unmask(ldb_crc32c_mask(crc)));
  ASSERT(crc == ldb_crc32c_unmask(ldb_crc32c_unmask(
                  ldb_crc32c_mask(ldb_crc32c_mask(crc)))));
}

LDB_EXTERN int
ldb_test_crc32c(void);

int
ldb_test_crc32c(void) {
  test_standard_results();
  test_values();
  test_extend();
  test_mask();
  return 0;
}
