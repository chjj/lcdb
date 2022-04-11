/*!
 * t-status.c - status test for lcdb
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

#include "util/status.h"
#include "util/testutil.h"

int
main(void) {
  ASSERT(strcmp(ldb_strerror(LDB_OK), "OK") == 0);
  ASSERT(strcmp(ldb_strerror(LDB_NOTFOUND), "NotFound") == 0);
  ASSERT(strcmp(ldb_strerror(LDB_CORRUPTION), "Corruption") == 0);
  ASSERT(strcmp(ldb_strerror(LDB_NOSUPPORT), "Not implemented") == 0);
  ASSERT(strcmp(ldb_strerror(LDB_INVALID), "Invalid argument") == 0);
  ASSERT(strcmp(ldb_strerror(LDB_IOERR), "IO error") == 0);
  ASSERT(strcmp(ldb_strerror(1000), "Invalid argument") == 0);
  return 0;
}
