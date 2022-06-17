/*!
 * status.c - error codes for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on google/leveldb:
 *   Copyright (c) 2011, The LevelDB Authors. All rights reserved.
 *   https://github.com/google/leveldb
 *
 * See LICENSE for more information.
 */

#include "env.h"
#include "internal.h"
#include "status.h"

/*
 * Constants
 */

static const char *ldb_errmsg[] = {
  /* .LDB_OK = */ "OK",
  /* .LDB_NOTFOUND = */ "NotFound",
  /* .LDB_CORRUPTION = */ "Corruption",
  /* .LDB_NOSUPPORT = */ "Not implemented",
  /* .LDB_INVALID = */ "Invalid argument",
  /* .LDB_IOERR = */ "IO error",
  /* .LDB_NOEXIST = */
    "Invalid argument: does not exist (create_if_missing is false)",
  /* .LDB_EXISTS = */ "Invalid argument: exists (error_if_exists is true)",
  /* .LDB_COMPARATOR_MISMATCH = */
    "Invalid argument: database comparator does not match existing comparator",
  /* .LDB_MISSING_FILES = */ "Corruption: missing files",
  /* .LDB_BAD_BLOCK_ENTRY = */ "Corruption: bad entry in block",
  /* .LDB_BAD_BLOCK_CONTENTS = */ "Corruption: bad block contents",
  /* .LDB_BAD_BLOCK_CHECKSUM = */ "Corruption: block checksum mismatch",
  /* .LDB_BAD_BLOCK_COMPRESS = */
    "Corruption: corrupted compressed block contents",
  /* .LDB_BAD_BLOCK_HANDLE = */ "Corruption: bad block handle",
  /* .LDB_BAD_BLOCK_TYPE = */ "Corruption: bad block type",
  /* .LDB_BAD_FOOTER = */ "Corruption: malformed table footer",
  /* .LDB_SHORT_SSTABLE = */ "Corruption: file is too short to be an sstable",
  /* .LDB_BAD_INTERNAL_KEY = */ "Corruption: corrupted internal key",
  /* .LDB_UNEXPECTED_VALUE = */
    "Corruption: file reader invoked with unexpected value",
  /* .LDB_NO_NEWLINE = */ "Corruption: CURRENT file does not end with newline",
  /* .LDB_BAD_CURRENT = */ "Corruption: CURRENT points to a non-existent file",
  /* .LDB_MALFORMED_META = */ "Corruption: malformed meta file",
  /* .LDB_NO_META_NEXTFILE = */
    "Corruption: no meta-nextfile entry in descriptor",
  /* .LDB_NO_META_LOGNUM = */
    "Corruption: no meta-lognumber entry in descriptor",
  /* .LDB_NO_META_LASTSEQ = */
    "Corruption: no last-sequence-number entry in descriptor",
  /* .LDB_MALFORMED_BATCH = */ "Corruption: malformed write batch (too small)",
  /* .LDB_BAD_BATCH_PUT = */ "Corruption: bad write batch put",
  /* .LDB_BAD_BATCH_DELETE = */ "Corruption: bad write batch delete",
  /* .LDB_BAD_BATCH_TAG = */ "Corruption: unknown write batch tag",
  /* .LDB_BAD_BATCH_COUNT = */ "Corruption: write batch has wrong count",
  /* .LDB_SMALL_RECORD = */ "Corruption: log record too small",
  /* .LDB_BAD_RECORD_LENGTH = */ "Corruption: bad record length",
  /* .LDB_CHECKSUM_MISMATCH = */ "Corruption: checksum mismatch",
  /* .LDB_PARTIAL_RECORD_1 = */ "Corruption: partial record without end(1)",
  /* .LDB_PARTIAL_RECORD_2 = */ "Corruption: partial record without end(2)",
  /* .LDB_MISSING_START_1 = */
    "Corruption: missing start of fragmented record(1)",
  /* .LDB_MISSING_START_2 = */
    "Corruption: missing start of fragmented record(2)",
  /* .LDB_ERROR_IN_MIDDLE = */ "Corruption: error in middle of record",
  /* .LDB_UNKNOWN_RECORD = */ "Corruption: unknown record type",
  /* .LDB_TRUNCATED_READ = */ "IO error: truncated block read",
  /* .LDB_DELETE_COMPACTION = */
    "IO error: deleting database during compaction",
  /* .LDB_NO_FILES = */ "IO error: repair found no files"
};

/*
 * Status
 */

const char *
ldb_strerror(int code) {
  if (code == LDB_OK)
    return ldb_errmsg[LDB_OK];

  if (code >= LDB_MINERR && code <= LDB_MAXERR)
    return ldb_errmsg[code - LDB_MINERR];

  return ldb_error_string(code);
}
