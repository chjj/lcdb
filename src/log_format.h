/*!
 * log_format.h - log format for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef LDB_LOG_FORMAT_H
#define LDB_LOG_FORMAT_H

typedef enum ldb_rectype {
  /* Zero is reserved for preallocated files. */
  LDB_TYPE_ZERO = 0,
  LDB_TYPE_FULL = 1,
  /* For fragments. */
  LDB_TYPE_FIRST = 2,
  LDB_TYPE_MIDDLE = 3,
  LDB_TYPE_LAST = 4
} ldb_rectype_t;

#define LDB_MAX_RECTYPE LDB_TYPE_LAST

#define LDB_BLOCK_SIZE 32768

/* Header is checksum (4 bytes), length (2 bytes), type (1 byte). */
#define LDB_HEADER_SIZE (4 + 2 + 1)

#endif /* LDB_LOG_FORMAT_H */
