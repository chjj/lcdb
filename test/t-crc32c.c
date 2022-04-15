/*!
 * t-crc32c.c - crc32c test for lcdb
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

#include "util/crc32c.h"
#include "util/testutil.h"

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
test_unaligned_results(void) {
  /* From rfc3720 section B.4. */
  static const uint8_t data[48] = {
    0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
    0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00
  };

  uint8_t tmp[64];
  uintptr_t align;
  int i;

  for (align = 0; align < 4; align++) {
    uint8_t *buf = tmp;

    while ((((uintptr_t)((void *)buf)) & 3) != align)
      buf += 1;

    memset(buf, 0, 32);

    ASSERT(0x8a9136aa == ldb_crc32c_value(buf, 32));

    memset(buf, 0xff, 32);

    ASSERT(0x62a8ab43 == ldb_crc32c_value(buf, 32));

    for (i = 0; i < 32; i++)
      buf[i] = i;

    ASSERT(0x46dd794e == ldb_crc32c_value(buf, 32));

    for (i = 0; i < 32; i++)
      buf[i] = 31 - i;

    ASSERT(0x113fdb5c == ldb_crc32c_value(buf, 32));

    memcpy(buf, data, 48);

    ASSERT(0xd9963a56 == ldb_crc32c_value(buf, 48));
  }
}

static void
test_large(void) {
  size_t len = (1 << 20) + 17;
  uint8_t *buf = ldb_malloc(len);

  memset(buf, 0xaa, len);

  ASSERT(0xb0d7025a == ldb_crc32c_value(buf, len));
  ASSERT(0x5a3a95f6 == ldb_crc32c_value(buf + 3, len));

  ldb_free(buf);
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

int
main(void) {
  test_standard_results();
  test_unaligned_results();
  test_large();
  test_values();
  test_extend();
  test_mask();

  if (ldb_crc32c_init()) {
    printf("accelerating\n");

    test_standard_results();
    test_unaligned_results();
    test_large();
    test_values();
    test_extend();
  }

  return 0;
}
