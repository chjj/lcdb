/*!
 * status.h - error codes for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_STATUS_H
#define RDB_STATUS_H

#include "extern.h"

/*
 * Constants
 */

#define RDB_OK (0)
#define RDB_NOTFOUND (-1)
#define RDB_CORRUPTION (-2)
#define RDB_NOSUPPORT (-3)
#define RDB_INVALID (-4)
#define RDB_IOERR (-5)

/*
 * Helpers
 */

RDB_EXTERN const char *
rdb_strerror(int code);

#endif /* RDB_STATUS_H */
