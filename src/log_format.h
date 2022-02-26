/*!
 * log_format.h - log format for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_LOG_FORMAT_H
#define RDB_LOG_FORMAT_H

typedef enum rdb_rectype {
  /* Zero is reserved for preallocated files. */
  RDB_TYPE_ZERO = 0,
  RDB_TYPE_FULL = 1,
  /* For fragments. */
  RDB_TYPE_FIRST = 2,
  RDB_TYPE_MIDDLE = 3,
  RDB_TYPE_LAST = 4
} rdb_rectype_t;

#define RDB_MAX_RECTYPE RDB_TYPE_LAST

#define RDB_BLOCK_SIZE 32768

/* Header is checksum (4 bytes), length (2 bytes), type (1 byte). */
#define RDB_HEADER_SIZE (4 + 2 + 1)

#endif /* RDB_LOG_FORMAT_H */
