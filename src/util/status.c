/*!
 * status.c - error codes for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include "internal.h"
#include "status.h"

static const char *ldb_errmsg[] = {
  /* .LDB_OK = */ "OK",
  /* .LDB_NOTFOUND = */ "NotFound",
  /* .LDB_CORRUPTION = */ "Corruption",
  /* .LDB_NOSUPPORT = */ "Not implemented",
  /* .LDB_INVALID = */ "Invalid argument",
  /* .LDB_IOERR = */ "IO error"
};

const char *
ldb_strerror(int code) {
  if (code < 0)
    code = -code;

  if (code >= (int)lengthof(ldb_errmsg))
    code = -LDB_INVALID;

  return ldb_errmsg[code];
}
