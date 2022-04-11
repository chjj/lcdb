/*!
 * hash_test.c - hash test for lcdb
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
#include <stdlib.h>
#include <string.h>

#include "util/hash.h"
#include "util/testutil.h"

static void
test_signed_unsigned_issue(void) {
  static const uint8_t data1[1] = {0x62};
  static const uint8_t data2[2] = {0xc3, 0x97};
  static const uint8_t data3[3] = {0xe2, 0x99, 0xa5};
  static const uint8_t data4[4] = {0xe1, 0x80, 0xb9, 0x32};
  static const uint8_t data5[48] = {
      0x01, 0xc0, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0x00,
      0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x18, 0x28, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
  };

  ASSERT(ldb_hash(0, 0, 0xbc9f1d34) == 0xbc9f1d34);
  ASSERT(ldb_hash(data1, sizeof(data1), 0xbc9f1d34) == 0xef1345c4);
  ASSERT(ldb_hash(data2, sizeof(data2), 0xbc9f1d34) == 0x5b663814);
  ASSERT(ldb_hash(data3, sizeof(data3), 0xbc9f1d34) == 0x323c078f);
  ASSERT(ldb_hash(data4, sizeof(data4), 0xbc9f1d34) == 0xed21633a);
  ASSERT(ldb_hash(data5, sizeof(data5), 0x12345678) == 0xf333dabb);
}

int
main(void) {
  test_signed_unsigned_issue();
  return 0;
}
