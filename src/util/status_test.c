/*!
 * status_test.c - status test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "extern.h"
#include "status.h"
#include "testutil.h"

LDB_EXTERN int
ldb_test_status(void);

int
ldb_test_status(void) {
  ASSERT(strcmp(ldb_strerror(LDB_OK), "OK") == 0);
  ASSERT(strcmp(ldb_strerror(LDB_NOTFOUND), "NotFound") == 0);
  ASSERT(strcmp(ldb_strerror(LDB_CORRUPTION), "Corruption") == 0);
  ASSERT(strcmp(ldb_strerror(LDB_NOSUPPORT), "Not implemented") == 0);
  ASSERT(strcmp(ldb_strerror(LDB_INVALID), "Invalid argument") == 0);
  ASSERT(strcmp(ldb_strerror(LDB_IOERR), "IO error") == 0);
  ASSERT(strcmp(ldb_strerror(1000), "Invalid argument") == 0);
  return 0;
}
