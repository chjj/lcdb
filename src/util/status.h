/*!
 * status.h - error codes for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on google/leveldb:
 *   Copyright (c) 2011, The LevelDB Authors. All rights reserved.
 *   https://github.com/google/leveldb
 *
 * See LICENSE for more information.
 */

#ifndef LDB_STATUS_H
#define LDB_STATUS_H

#include <errno.h>
#include "extern.h"

/*
 * Constants
 */

#define LDB_OK                      0
#define LDB_MINERR              30000
#define LDB_NOTFOUND            30001
#define LDB_CORRUPTION          30002
#define LDB_NOSUPPORT           30003
#define LDB_INVALID             30004
#define LDB_IOERR               30005
#define LDB_NOEXIST             30006
#define LDB_EXISTS              30007
#define LDB_COMPARATOR_MISMATCH 30008
#define LDB_MISSING_FILES       30009
#define LDB_BAD_BLOCK_ENTRY     30010
#define LDB_BAD_BLOCK_CONTENTS  30011
#define LDB_BAD_BLOCK_CHECKSUM  30012
#define LDB_BAD_BLOCK_COMPRESS  30013
#define LDB_BAD_BLOCK_HANDLE    30014
#define LDB_BAD_BLOCK_TYPE      30015
#define LDB_BAD_FOOTER          30016
#define LDB_SHORT_SSTABLE       30017
#define LDB_BAD_INTERNAL_KEY    30018
#define LDB_UNEXPECTED_VALUE    30019
#define LDB_NO_NEWLINE          30020
#define LDB_BAD_CURRENT         30021
#define LDB_MALFORMED_META      30022
#define LDB_NO_META_NEXTFILE    30023
#define LDB_NO_META_LOGNUM      30024
#define LDB_NO_META_LASTSEQ     30025
#define LDB_MALFORMED_BATCH     30026
#define LDB_BAD_BATCH_PUT       30027
#define LDB_BAD_BATCH_DELETE    30028
#define LDB_BAD_BATCH_TAG       30029
#define LDB_BAD_BATCH_COUNT     30030
#define LDB_SMALL_RECORD        30031
#define LDB_BAD_RECORD_LENGTH   30032
#define LDB_CHECKSUM_MISMATCH   30033
#define LDB_PARTIAL_RECORD_1    30034
#define LDB_PARTIAL_RECORD_2    30035
#define LDB_MISSING_START_1     30036
#define LDB_MISSING_START_2     30037
#define LDB_ERROR_IN_MIDDLE     30038
#define LDB_UNKNOWN_RECORD      30039
#define LDB_TRUNCATED_READ      30040
#define LDB_DELETE_COMPACTION   30041
#define LDB_NO_FILES            30042
#define LDB_MAXERR              30042

#ifdef _WIN32
#  define LDB_ENOENT  2 /* ERROR_FILE_NOT_FOUND */
#  define LDB_ENOMEM  8 /* ERROR_NOT_ENOUGH_MEMORY */
#  define LDB_EINVAL 87 /* ERROR_INVALID_PARAMETER */
#  define LDB_EEXIST 80 /* ERROR_FILE_EXISTS */
#  define LDB_ENOLCK 33 /* ERROR_LOCK_VIOLATION */
#else
#  define LDB_ENOENT ENOENT
#  define LDB_ENOMEM ENOMEM
#  define LDB_EINVAL EINVAL
#  define LDB_EEXIST EEXIST
#  define LDB_ENOLCK ENOLCK
#endif

/*
 * Macros
 */

#define LDB_IS_STATUS(x) \
  ((x) == LDB_OK || ((x) >= LDB_MINERR && (x) <= LDB_MAXERR))

/*
 * Helpers
 */

LDB_EXTERN const char *
ldb_strerror(int code);

#endif /* LDB_STATUS_H */
