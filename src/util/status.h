/*!
 * status.h - error codes for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef LDB_STATUS_H
#define LDB_STATUS_H

#include "extern.h"

/*
 * Constants
 */

#define LDB_OK (0)
#define LDB_NOTFOUND (-1)
#define LDB_CORRUPTION (-2)
#define LDB_NOSUPPORT (-3)
#define LDB_INVALID (-4)
#define LDB_IOERR (-5)

/*
 * Helpers
 */

LDB_EXTERN const char *
ldb_strerror(int code);

#endif /* LDB_STATUS_H */
