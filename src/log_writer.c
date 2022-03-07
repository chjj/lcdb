/*!
 * log_writer.h - log writer for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>

#include "util/buffer.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/env.h"
#include "util/internal.h"
#include "util/slice.h"
#include "util/status.h"

#include "log_format.h"
#include "log_writer.h"

/*
 * LogWriter
 */

static void
init_type_crc(uint32_t *type_crc) {
  uint8_t i;

  for (i = 0; (int)i <= RDB_MAX_RECTYPE; i++)
    type_crc[i] = rdb_crc32c_value(&i, 1);
}

rdb_logwriter_t *
rdb_logwriter_create(rdb_wfile_t *file, uint64_t length) {
  rdb_logwriter_t *lw = rdb_malloc(sizeof(rdb_logwriter_t));
  rdb_logwriter_init(lw, file, length);
  return lw;
}

void
rdb_logwriter_destroy(rdb_logwriter_t *lw) {
  rdb_free(lw);
}

void
rdb_logwriter_init(rdb_logwriter_t *lw, rdb_wfile_t *file, uint64_t length) {
  lw->file = file;
  lw->dst = NULL; /* For testing. */
  lw->block_offset = length % RDB_BLOCK_SIZE;
  init_type_crc(lw->type_crc);
}

static int
emit_physical_record(rdb_logwriter_t *lw,
                     rdb_rectype_t type,
                     const uint8_t *ptr,
                     size_t length) {
  uint8_t buf[RDB_HEADER_SIZE];
  rdb_slice_t data;
  int rc = RDB_OK;
  uint32_t crc;

  assert(length <= 0xffff); /* Must fit in two bytes. */
  assert(lw->block_offset + RDB_HEADER_SIZE + length <= RDB_BLOCK_SIZE);

  /* Format the header. */
  buf[4] = (uint8_t)(length & 0xff);
  buf[5] = (uint8_t)(length >> 8);
  buf[6] = (uint8_t)(type);

  /* Compute the crc of the record type and the payload. */
  crc = rdb_crc32c_extend(lw->type_crc[type], ptr, length);
  crc = rdb_crc32c_mask(crc); /* Adjust for storage. */

  rdb_fixed32_write(buf, crc);

  if (lw->dst != NULL) {
    rdb_buffer_append(lw->dst, buf, RDB_HEADER_SIZE);
    rdb_buffer_append(lw->dst, ptr, length);
  } else {
    /* Write the header and the payload. */
    rdb_slice_set(&data, buf, RDB_HEADER_SIZE);

    rc = rdb_wfile_append(lw->file, &data);

    if (rc == RDB_OK) {
      rdb_slice_set(&data, ptr, length);

      rc = rdb_wfile_append(lw->file, &data);

      if (rc == RDB_OK)
        rc = rdb_wfile_flush(lw->file);
    }
  }

  lw->block_offset += RDB_HEADER_SIZE + length;

  return rc;
}

int
rdb_logwriter_add_record(rdb_logwriter_t *lw, const rdb_slice_t *slice) {
  static const uint8_t zeroes[RDB_HEADER_SIZE] = {0};
  const uint8_t *ptr = slice->data;
  size_t left = slice->size;
  int rc = RDB_OK;
  int begin = 1;

  /* Fragment the record if necessary and emit it.  Note that if slice
     is empty, we still want to iterate once to emit a single
     zero-length record. */
  do {
    int leftover = RDB_BLOCK_SIZE - lw->block_offset;
    size_t avail, fragment_length;
    rdb_rectype_t type;
    int end;

    assert(leftover >= 0);

    if (leftover < RDB_HEADER_SIZE) {
      /* Switch to a new block. */
      if (leftover > 0) {
        /* Fill the trailer. */
        rdb_slice_t padding;

        rdb_slice_set(&padding, zeroes, leftover);

        if (lw->dst != NULL)
          rdb_buffer_concat(lw->dst, &padding);
        else
          rdb_wfile_append(lw->file, &padding);
      }

      lw->block_offset = 0;
    }

    /* Invariant: we never leave < RDB_HEADER_SIZE bytes in a block. */
    assert(RDB_BLOCK_SIZE - lw->block_offset - RDB_HEADER_SIZE >= 0);

    avail = RDB_BLOCK_SIZE - lw->block_offset - RDB_HEADER_SIZE;
    fragment_length = (left < avail) ? left : avail;
    end = (left == fragment_length);

    if (begin && end) {
      type = RDB_TYPE_FULL;
    } else if (begin) {
      type = RDB_TYPE_FIRST;
    } else if (end) {
      type = RDB_TYPE_LAST;
    } else {
      type = RDB_TYPE_MIDDLE;
    }

    rc = emit_physical_record(lw, type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = 0;
  } while (rc == RDB_OK && left > 0);

  return rc;
}
