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

#define LDB_OK                       (0)
#define LDB_MINERR              (-30100)
#define LDB_NOTFOUND            (-30099)
#define LDB_CORRUPTION          (-30098)
#define LDB_NOSUPPORT           (-30097)
#define LDB_INVALID             (-30096)
#define LDB_IOERR               (-30095)
#define LDB_NOEXIST             (-30094)
#define LDB_EXISTS              (-30093)
#define LDB_COMPARATOR_MISMATCH (-30092)
#define LDB_MISSING_FILES       (-30091)
#define LDB_BAD_BLOCK_ENTRY     (-30090)
#define LDB_BAD_BLOCK_CONTENTS  (-30089)
#define LDB_BAD_BLOCK_CHECKSUM  (-30088)
#define LDB_BAD_BLOCK_COMPRESS  (-30087)
#define LDB_BAD_BLOCK_HANDLE    (-30086)
#define LDB_BAD_BLOCK_TYPE      (-30085)
#define LDB_BAD_FOOTER          (-30084)
#define LDB_SHORT_SSTABLE       (-30083)
#define LDB_BAD_INTERNAL_KEY    (-30082)
#define LDB_UNEXPECTED_VALUE    (-30081)
#define LDB_NO_NEWLINE          (-30080)
#define LDB_BAD_CURRENT         (-30079)
#define LDB_MALFORMED_META      (-30078)
#define LDB_NO_META_NEXTFILE    (-30077)
#define LDB_NO_META_LOGNUM      (-30076)
#define LDB_NO_META_LASTSEQ     (-30075)
#define LDB_MALFORMED_BATCH     (-30074)
#define LDB_BAD_BATCH_PUT       (-30073)
#define LDB_BAD_BATCH_DELETE    (-30072)
#define LDB_BAD_BATCH_TAG       (-30071)
#define LDB_BAD_BATCH_COUNT     (-30070)
#define LDB_SMALL_RECORD        (-30069)
#define LDB_BAD_RECORD_LENGTH   (-30068)
#define LDB_CHECKSUM_MISMATCH   (-30067)
#define LDB_PARTIAL_RECORD_1    (-30066)
#define LDB_PARTIAL_RECORD_2    (-30065)
#define LDB_MISSING_START_1     (-30064)
#define LDB_MISSING_START_2     (-30063)
#define LDB_ERROR_IN_MIDDLE     (-30062)
#define LDB_UNKNOWN_RECORD      (-30061)
#define LDB_TRUNCATED_READ      (-30060)
#define LDB_DELETE_COMPACTION   (-30059)
#define LDB_NO_FILES            (-30058)
#define LDB_MAXERR              (-30058)

#ifdef _WIN32
#  define LDB_ENOENT  (-2) /* ERROR_FILE_NOT_FOUND */
#  define LDB_EBADF   (-6) /* ERROR_INVALID_HANDLE */
#  define LDB_ENOMEM  (-8) /* ERROR_NOT_ENOUGH_MEMORY */
#  define LDB_EINVAL (-87) /* ERROR_INVALID_PARAMETER */
#  define LDB_EEXIST (-80) /* ERROR_FILE_EXISTS */
#  define LDB_ENOLCK (-33) /* ERROR_LOCK_VIOLATION */
#else
#  define LDB_ENOENT LDB_ERR(ENOENT)
#  define LDB_EBADF  LDB_ERR(EBADF)
#  define LDB_ENOMEM LDB_ERR(ENOMEM)
#  define LDB_EINVAL LDB_ERR(EINVAL)
#  define LDB_EEXIST LDB_ERR(EEXIST)
#  define LDB_ENOLCK LDB_ERR(ENOLCK)
#endif

/*
 * Macros
 */

#define LDB_IS_STATUS(x) \
  ((x) == LDB_OK || ((x) >= LDB_MINERR && (x) <= LDB_MAXERR))

#if defined(__COSMOPOLITAN__)
#  define LDB_ERR(x) (EDOM > 0 ? -(x) : (x))
#elif defined(_WIN32)
#  define LDB_ERR(x) (-(int)(x))
#else
#  if EDOM > 0
#    define LDB_ERR(x) (-(x))
#  else
#    define LDB_ERR(x) (x)
#  endif
#endif

/*
 * Helpers
 */

LDB_EXTERN const char *
ldb_strerror(int code);

#endif /* LDB_STATUS_H */
