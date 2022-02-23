/*!
 * status.c - error codes for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include "internal.h"
#include "status.h"

static const char *rdb_errmsg[] = {
  /* .RDB_OK = */ "OK",
  /* .RDB_NOTFOUND = */ "NotFound",
  /* .RDB_CORRUPTION = */ "Corruption",
  /* .RDB_NOSUPPORT = */ "Not implemented",
  /* .RDB_INVALID = */ "Invalid argument",
  /* .RDB_IOERR = */ "IO error"
};

const char *
rdb_strerror(int code) {
  if (code < 0)
    code = -code;

  if (code >= (int)lengthof(rdb_errmsg))
    code = -RDB_INVALID;

  return rdb_errmsg[code];
}
