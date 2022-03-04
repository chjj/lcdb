/*!
 * status_test.c - status test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#undef NDEBUG

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "extern.h"
#include "status.h"

RDB_EXTERN int
rdb_test_status(void);

int
rdb_test_status(void) {
  assert(strcmp(rdb_strerror(RDB_OK), "OK") == 0);
  assert(strcmp(rdb_strerror(RDB_NOTFOUND), "NotFound") == 0);
  assert(strcmp(rdb_strerror(RDB_CORRUPTION), "Corruption") == 0);
  assert(strcmp(rdb_strerror(RDB_NOSUPPORT), "Not implemented") == 0);
  assert(strcmp(rdb_strerror(RDB_INVALID), "Invalid argument") == 0);
  assert(strcmp(rdb_strerror(RDB_IOERR), "IO error") == 0);
  assert(strcmp(rdb_strerror(1000), "Invalid argument") == 0);
  return 0;
}
