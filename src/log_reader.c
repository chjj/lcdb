/*!
 * log_reader.c - log reader for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "util/buffer.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/env.h"
#include "util/internal.h"
#include "util/slice.h"
#include "util/status.h"

#include "log_format.h"
#include "log_reader.h"

/*
 * Constants
 */

/* Extend record types with the following special values. */
enum {
  RDB_EOF = RDB_MAX_RECTYPE + 1,

  /* Returned whenever we find an invalid physical record.
   *
   * Currently there are three situations in which this happens:
   *
   * - The record has an invalid CRC (read_physical_record reports a drop)
   * - The record is a 0-length record (No drop is reported)
   * - The record is below constructor's initial_offset (No drop is reported)
   */
  RDB_BAD_RECORD = RDB_MAX_RECTYPE + 2
};

/*
 * LogReader
 */

void
rdb_logreader_init(rdb_logreader_t *lr,
                   rdb_rfile_t *file,
                   rdb_reporter_t *reporter,
                   int checksum,
                   uint64_t initial_offset) {
  lr->file = file;
  lr->reporter = reporter;
  lr->checksum = checksum;
  lr->backing_store = rdb_malloc(RDB_BLOCK_SIZE);
  rdb_slice_init(&lr->buffer);
  lr->eof = 0;
  lr->last_offset = 0;
  lr->end_offset = 0;
  lr->initial_offset = initial_offset;
  lr->resyncing = (initial_offset > 0);
}

void
rdb_logreader_clear(rdb_logreader_t *lr) {
  rdb_free(lr->backing_store);
}

/* Reports dropped bytes to the reporter. */
/* buffer must be updated to remove the dropped bytes prior to invocation. */
static void
report_drop(rdb_logreader_t *lr, int64_t bytes, int reason) {
  if (lr->reporter != NULL &&
      lr->end_offset - lr->buffer.size - bytes >= lr->initial_offset) {
    lr->reporter->corruption(lr->reporter, bytes, reason);
  }
}

static void
report_corruption(rdb_logreader_t *lr, uint64_t bytes, const char *reason) {
  report_drop(lr, bytes, RDB_CORRUPTION /* reason */);
  (void)reason;
}

/* Return type, or one of the preceding special values. */
static unsigned int
read_physical_record(rdb_logreader_t *lr, rdb_slice_t *result) {
  const uint8_t *header;
  uint32_t a, b, length;
  unsigned int type;
  int rc;

  for (;;) {
    if (lr->buffer.size < RDB_HEADER_SIZE) {
      if (!lr->eof) {
        /* Last read was a full read, so this is a trailer to skip. */
        rdb_slice_reset(&lr->buffer);

        rc = rdb_rfile_read(lr->file,
                            &lr->buffer,
                            lr->backing_store,
                            RDB_BLOCK_SIZE);

        lr->end_offset += lr->buffer.size;

        if (rc != RDB_OK) {
          rdb_slice_reset(&lr->buffer);
          report_drop(lr, RDB_BLOCK_SIZE, rc);
          lr->eof = 1;
          return RDB_EOF;
        }

        if (lr->buffer.size < RDB_BLOCK_SIZE)
          lr->eof = 1;

        continue;
      }

      /* Note that if buffer is non-empty, we have a truncated header at the
         end of the file, which can be caused by the writer crashing in the
         middle of writing the header. Instead of considering this an error,
         just report EOF. */
      rdb_slice_reset(&lr->buffer);

      return RDB_EOF;
    }

    /* Parse the header. */
    header = lr->buffer.data;
    a = (uint32_t)header[4] & 0xff;
    b = (uint32_t)header[5] & 0xff;
    type = header[6];
    length = a | (b << 8);

    if (RDB_HEADER_SIZE + length > lr->buffer.size) {
      size_t drop_size = lr->buffer.size;

      rdb_slice_reset(&lr->buffer);

      if (!lr->eof) {
        report_corruption(lr, drop_size, "bad record length");
        return RDB_BAD_RECORD;
      }

      /* If the end of the file has been reached without reading |length| bytes
         of payload, assume the writer died in the middle of writing the record.
         Don't report a corruption. */
      return RDB_EOF;
    }

    if (type == RDB_TYPE_ZERO && length == 0) {
      /* Skip zero length record without reporting any drops since
         such records are produced by the mmap based writing code in
         env_unix_impl.h that preallocates file regions. */
      rdb_slice_reset(&lr->buffer);

      return RDB_BAD_RECORD;
    }

    /* Check crc. */
    if (lr->checksum) {
      uint32_t expect = rdb_crc32c_unmask(rdb_fixed32_decode(header));
      uint32_t actual = rdb_crc32c_value(header + 6, 1 + length);

      if (actual != expect) {
        /* Drop the rest of the buffer since "length" itself may have
           been corrupted and if we trust it, we could find some
           fragment of a real log record that just happens to look
           like a valid log record. */
        size_t drop_size = lr->buffer.size;

        rdb_slice_reset(&lr->buffer);

        report_corruption(lr, drop_size, "checksum mismatch");

        return RDB_BAD_RECORD;
      }
    }

    rdb_slice_eat(&lr->buffer, RDB_HEADER_SIZE + length);

    /* Skip physical record that started before initial_offset. */
    if (lr->end_offset - lr->buffer.size - RDB_HEADER_SIZE - length <
        lr->initial_offset) {
      rdb_slice_reset(result);
      return RDB_BAD_RECORD;
    }

    rdb_slice_set(result, header + RDB_HEADER_SIZE, length);

    return type;
  }
}

/* Skips all blocks that are completely before "initial_offset_".
 *
 * Returns true on success. Handles reporting.
 */
static int
skip_to_initial_block(rdb_logreader_t *lr) {
  size_t offset_in_block = lr->initial_offset % RDB_BLOCK_SIZE;
  uint64_t block_start = lr->initial_offset - offset_in_block;

  /* Don't search a block if we'd be in the trailer. */
  if (offset_in_block > RDB_BLOCK_SIZE - 6)
    block_start += RDB_BLOCK_SIZE;

  lr->end_offset = block_start;

  /* Skip to start of first block that can contain the initial record. */
  if (block_start > 0) {
    int rc = rdb_rfile_skip(lr->file, block_start);

    if (rc != RDB_OK) {
      report_drop(lr, block_start, rc);
      return 0;
    }
  }

  return 1;
}

int
rdb_logreader_read_record(rdb_logreader_t *lr,
                          rdb_slice_t *record,
                          rdb_buffer_t *scratch) {
  /* Record offset of the logical record that we're reading
     0 is a dummy value to make compilers happy. */
  uint64_t prospective_offset = 0;
  int in_fragmented_record = 0;
  rdb_slice_t fragment;

  if (lr->last_offset < lr->initial_offset) {
    if (!skip_to_initial_block(lr))
      return 0;
  }

  rdb_slice_reset(record);
  rdb_buffer_reset(scratch);

  for (;;) {
    unsigned int record_type = read_physical_record(lr, &fragment);

    /* read_physical_record may have only had an empty trailer remaining in its
       internal buffer. Calculate the offset of the next physical record now
       that it has returned, properly accounting for its header size. */
    uint64_t physical_offset = (lr->end_offset
                              - lr->buffer.size
                              - RDB_HEADER_SIZE
                              - fragment.size);

    if (lr->resyncing) {
      if (record_type == RDB_TYPE_MIDDLE)
        continue;

      if (record_type == RDB_TYPE_LAST) {
        lr->resyncing = 0;
        continue;
      }

      lr->resyncing = 0;
    }

    switch (record_type) {
      case RDB_TYPE_FULL: {
        if (in_fragmented_record) {
          /* Handle bug in earlier versions of log::Writer where
            it could emit an empty RDB_TYPE_FIRST record at the tail end
            of a block followed by a RDB_TYPE_FULL or RDB_TYPE_FIRST record
            at the beginning of the next block. */
          if (scratch->size > 0) {
            report_corruption(lr, scratch->size,
                              "partial record without end(1)");
          }
        }

        prospective_offset = physical_offset;

        rdb_buffer_reset(scratch);

        *record = fragment;

        lr->last_offset = prospective_offset;

        return 1;
      }

      case RDB_TYPE_FIRST: {
        if (in_fragmented_record) {
          /* Handle bug in earlier versions of log::Writer where
             it could emit an empty RDB_TYPE_FIRST record at the tail end
             of a block followed by a RDB_TYPE_FULL or RDB_TYPE_FIRST record
             at the beginning of the next block. */
          if (scratch->size > 0) {
            report_corruption(lr, scratch->size,
                              "partial record without end(2)");
          }
        }

        prospective_offset = physical_offset;

        rdb_buffer_set(scratch, fragment.data, fragment.size);

        in_fragmented_record = 1;

        break;
      }

      case RDB_TYPE_MIDDLE: {
        if (!in_fragmented_record) {
          report_corruption(lr, fragment.size,
                            "missing start of fragmented record(1)");
        } else {
          rdb_buffer_append(scratch, fragment.data, fragment.size);
        }

        break;
      }

      case RDB_TYPE_LAST: {
        if (!in_fragmented_record) {
          report_corruption(lr, fragment.size,
                            "missing start of fragmented record(2)");
        } else {
          rdb_buffer_append(scratch, fragment.data, fragment.size);

          *record = *scratch;

          lr->last_offset = prospective_offset;

          return 1;
        }

        break;
      }

      case RDB_EOF: {
        if (in_fragmented_record) {
          /* This can be caused by the writer dying immediately after
             writing a physical record but before completing the next; don't
             treat it as a corruption, just ignore the entire logical record. */
          rdb_buffer_reset(scratch);
        }

        return 0;
      }

      case RDB_BAD_RECORD:
        if (in_fragmented_record) {
          report_corruption(lr, scratch->size, "error in middle of record");

          in_fragmented_record = 0;

          rdb_buffer_reset(scratch);
        }

        break;

      default: {
        char buf[40];

        sprintf(buf, "unknown record type %u", record_type);

        report_corruption(lr,
          (fragment.size + (in_fragmented_record ? scratch->size : 0)),
          buf);

        in_fragmented_record = 0;

        rdb_buffer_reset(scratch);

        break;
      }
    }
  }

  return 0;
}

uint64_t
rdb_logreader_last_offset(const rdb_logreader_t *lr) {
  return lr->last_offset;
}
