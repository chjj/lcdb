/*!
 * log_reader.h - log reader for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_LOG_READER_H
#define RDB_LOG_READER_H

#include <stddef.h>
#include <stdint.h>

#include "util/types.h"

/*
 * Types
 */

struct rdb_logger_s;
struct rdb_rfile_s;

/* Interface for reporting errors. */
typedef struct rdb_reporter_s {
  /* Some corruption was detected. "bytes" is the approximate number
     of bytes dropped due to the corruption. */
  const char *fname;
  int *status;
  struct rdb_logger_s *info_log;
  uint64_t lognum;
  void *dst; /* FILE */
  void (*corruption)(struct rdb_reporter_s *reporter, size_t bytes, int status);
} rdb_reporter_t;

typedef struct rdb_logreader_s {
  struct rdb_rfile_s *file; /* SequentialFile */
  rdb_reporter_t *reporter;
  int checksum;
  uint8_t *backing_store;
  rdb_slice_t buffer;
  int eof; /* Last read() indicated EOF by returning < RDB_BLOCK_SIZE. */

  /* Offset of the last record returned by read_record. */
  uint64_t last_offset;

  /* Offset of the first location past the end of buffer. */
  uint64_t end_offset;

  /* Offset at which to start looking for the first record to return. */
  uint64_t initial_offset;

  /* True if we are resynchronizing after a seek (initial_offset > 0). In
     particular, a run of RDB_TYPE_MIDDLE and RDB_TYPE_LAST records can
     be silently skipped in this mode. */
  int resyncing;
} rdb_logreader_t;

/*
 * LogWriter
 */

/* Create a reader that will return log records from "*file".
 * "*file" must remain live while this Reader is in use.
 *
 * If "reporter" is non-null, it is notified whenever some data is
 * dropped due to a detected corruption. "*reporter" must remain
 * live while this Reader is in use.
 *
 * If "checksum" is true, verify checksums if available.
 *
 * The Reader will start reading at the first record located at physical
 * position >= initial_offset within the file.
 */
void
rdb_logreader_init(rdb_logreader_t *lr,
                   struct rdb_rfile_s *file,
                   rdb_reporter_t *reporter,
                   int checksum,
                   uint64_t initial_offset);

void
rdb_logreader_clear(rdb_logreader_t *lr);

/* Read the next record into *record. Returns true if read
 * successfully, false if we hit end of the input. May use
 * "*scratch" as temporary storage. The contents filled in *record
 * will only be valid until the next mutating operation on this
 * reader or the next mutation to *scratch.
 */
int
rdb_logreader_read_record(rdb_logreader_t *lr,
                          rdb_slice_t *record,
                          rdb_buffer_t *scratch);

/* Returns the physical offset of the last record returned by read_record.
 *
 * Undefined before the first call to ReadRecord.
 */
uint64_t
rdb_logreader_last_offset(const rdb_logreader_t *lr);

#endif /* RDB_LOG_READER_H */
