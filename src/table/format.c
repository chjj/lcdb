/*!
 * format.c - table format for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stddef.h>
#include <stdint.h>

#include "../util/buffer.h"
#include "../util/coding.h"
#include "../util/crc32c.h"
#include "../util/env.h"
#include "../util/internal.h"
#include "../util/options.h"
#include "../util/slice.h"
#include "../util/snappy.h"
#include "../util/status.h"

#include "format.h"

/*
 * Block Handle
 */

void
rdb_blockhandle_init(rdb_blockhandle_t *x) {
  x->offset = ~UINT64_C(0);
  x->size = ~UINT64_C(0);
}

size_t
rdb_blockhandle_size(const rdb_blockhandle_t *x) {
  return rdb_varint64_size(x->offset) + rdb_varint64_size(x->size);
}

uint8_t *
rdb_blockhandle_write(uint8_t *zp, const rdb_blockhandle_t *x) {
  /* Sanity check that all fields have been set. */
  assert(x->offset != ~UINT64_C(0));
  assert(x->size != ~UINT64_C(0));

  zp = rdb_varint64_write(zp, x->offset);
  zp = rdb_varint64_write(zp, x->size);

  return zp;
}

void
rdb_blockhandle_export(rdb_buffer_t *z, const rdb_blockhandle_t *x) {
  uint8_t *zp = rdb_buffer_expand(z, RDB_BLOCKHANDLE_MAX);
  size_t xn = rdb_blockhandle_write(zp, x) - zp;

  z->size += xn;
}

int
rdb_blockhandle_read(rdb_blockhandle_t *z, const uint8_t **xp, size_t *xn) {
  if (!rdb_varint64_read(&z->offset, xp, xn))
    return 0;

  if (!rdb_varint64_read(&z->size, xp, xn))
    return 0;

  return 1;
}

int
rdb_blockhandle_import(rdb_blockhandle_t *z, const rdb_slice_t *x) {
  rdb_slice_t tmp = *x;
  return rdb_blockhandle_read(z, (const uint8_t **)&tmp.data, &tmp.size);
}

/*
 * Footer
 */

void
rdb_footer_init(rdb_footer_t *x) {
  rdb_blockhandle_init(&x->metaindex_handle);
  rdb_blockhandle_init(&x->index_handle);
}

uint8_t *
rdb_footer_write(uint8_t *zp, const rdb_footer_t *x) {
  uint8_t *tp = zp;
  size_t pad;

  zp = rdb_blockhandle_write(zp, &x->metaindex_handle);
  zp = rdb_blockhandle_write(zp, &x->index_handle);

  pad = (2 * RDB_BLOCKHANDLE_MAX) - (zp - tp);

  zp = rdb_padding_write(zp, pad);
  zp = rdb_fixed64_write(zp, RDB_TABLE_MAGIC);

  return zp;
}

void
rdb_footer_export(rdb_buffer_t *z, const rdb_footer_t *x) {
  uint8_t *zp = rdb_buffer_expand(z, RDB_FOOTER_SIZE);
  size_t xn = rdb_footer_write(zp, x) - zp;

  z->size += xn;
}

int
rdb_footer_read(rdb_footer_t *z, const uint8_t **xp, size_t *xn) {
  const uint8_t *tp = *xp;
  size_t tn = *xn;

  if (*xn < RDB_FOOTER_SIZE)
    return 0;

  if (rdb_fixed64_decode(*xp + RDB_FOOTER_SIZE - 8) != RDB_TABLE_MAGIC)
    return 0;

  if (!rdb_blockhandle_read(&z->metaindex_handle, xp, xn))
    return 0;

  if (!rdb_blockhandle_read(&z->index_handle, xp, xn))
    return 0;

  *xp = tp + RDB_FOOTER_SIZE;
  *xn = tn - RDB_FOOTER_SIZE;

  return 1;
}

int
rdb_footer_import(rdb_footer_t *z, const rdb_slice_t *x) {
  rdb_slice_t tmp = *x;
  return rdb_footer_read(z, (const uint8_t **)&tmp.data, &tmp.size);
}

/*
 * Block Contents
 */

void
rdb_blockcontents_init(rdb_blockcontents_t *x) {
  rdb_slice_init(&x->data);

  x->cachable = 0;
  x->heap_allocated = 0;
}

/*
 * Block Read
 */

static void
rdb_safe_free(void *ptr) {
  if (ptr != NULL)
    rdb_free(ptr);
}

int
rdb_read_block(rdb_blockcontents_t *result,
               rdb_rfile_t *file,
               const rdb_readopt_t *options,
               const rdb_blockhandle_t *handle) {
  rdb_slice_t contents;
  const uint8_t *data;
  uint8_t *buf = NULL;
  size_t n, len;
  int rc;

  rdb_blockcontents_init(result);

  /* Read the block contents as well as the type/crc footer. */
  /* See table_builder.c for the code that built this structure. */
  n = handle->size;
  len = n + RDB_BLOCK_TRAILER_SIZE;

  if (!rdb_rfile_mapped(file))
    buf = rdb_malloc(len);

  rc = rdb_rfile_pread(file, &contents, buf, len, handle->offset);

  if (rc != RDB_OK) {
    rdb_safe_free(buf);
    return rc;
  }

  if (contents.size != len) {
    rdb_safe_free(buf);
    return RDB_IOERR; /* "truncated block read" */
  }

  /* Check the crc of the type and the block contents. */
  data = contents.data; /* Pointer to where Read put the data. */

  if (options->verify_checksums) {
    uint32_t crc = rdb_crc32c_unmask(rdb_fixed32_decode(data + n + 1));
    uint32_t actual = rdb_crc32c_value(data, n + 1);

    if (crc != actual) {
      rdb_safe_free(buf);
      return RDB_CORRUPTION; /* "block checksum mismatch" */
    }
  }

  switch (data[n]) {
    case RDB_NO_COMPRESSION: {
      if (data != buf) {
        /* File implementation gave us pointer to some other data.
           Use it directly under the assumption that it will be live
           while the file is open. */
        rdb_safe_free(buf);
        rdb_slice_set(&result->data, data, n);
        result->heap_allocated = 0;
        result->cachable = 0; /* Do not double-cache. */
      } else {
        rdb_slice_set(&result->data, buf, n);
        result->heap_allocated = 1;
        result->cachable = 1;
      }

      /* Ok. */
      break;
    }

    case RDB_SNAPPY_COMPRESSION: {
      int ulength = rdb_snappy_decode_size(data, n);
      uint8_t *ubuf;

      if (ulength < 0) {
        rdb_safe_free(buf);
        return RDB_CORRUPTION; /* "corrupted compressed block contents" */
      }

      ubuf = rdb_malloc(ulength);

      if (!rdb_snappy_decode(ubuf, data, n)) {
        rdb_safe_free(buf);
        rdb_free(ubuf);
        return RDB_CORRUPTION; /* "corrupted compressed block contents" */
      }

      rdb_safe_free(buf);

      rdb_slice_set(&result->data, ubuf, ulength);

      result->heap_allocated = 1;
      result->cachable = 1;

      break;
    }

    default: {
      rdb_safe_free(buf);
      return RDB_CORRUPTION; /* "bad block type" */
    }
  }

  return RDB_OK;
}
