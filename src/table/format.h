/*!
 * format.h - table format for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_TABLE_FORMAT_H
#define RDB_TABLE_FORMAT_H

#include <stddef.h>
#include <stdint.h>

#include "../util/types.h"

/*
 * Constants
 */

/* Maximum encoding length of a BlockHandle. */
#define RDB_BLOCKHANDLE_MAX (10 + 10) /* kMaxEncodedLength */

/* Encoded length of a Footer. Note that the serialization of a
   Footer will always occupy exactly this many bytes. It consists
   of two block handles and a magic number. */
#define RDB_FOOTER_SIZE (2 * RDB_BLOCKHANDLE_MAX + 8) /* kEncodedLength */

/* 1-byte type + 32-bit crc. */
#define RDB_BLOCK_TRAILER_SIZE 5 /* kBlockTrailerSize */

/* kTableMagicNumber was picked by running
      echo http://code.google.com/p/leveldb/ | sha1sum
   and taking the leading 64 bits. */
#define RDB_TABLE_MAGIC UINT64_C(0xdb4775248b80fb57) /* kTableMagicNumber */

/*
 * Types
 */

struct rdb_rfile_s;
struct rdb_readopt_s;

/* BlockHandle is a pointer to the extent of a file that stores a data
   block or a meta block. */
typedef struct rdb_blockhandle_s {
  uint64_t offset;
  uint64_t size;
} rdb_blockhandle_t;

/* Footer encapsulates the fixed information stored at the tail
   end of every table file. */
typedef struct rdb_footer_s {
  rdb_blockhandle_t metaindex_handle;
  rdb_blockhandle_t index_handle;
} rdb_footer_t;

typedef struct rdb_blockcontents_s {
  rdb_slice_t data;    /* Actual contents of data. */
  int cachable;        /* True iff data can be cached. */
  int heap_allocated;  /* True iff caller should free() data.data. */
} rdb_blockcontents_t;

/*
 * Block Handle
 */

void
rdb_blockhandle_init(rdb_blockhandle_t *x);

size_t
rdb_blockhandle_size(const rdb_blockhandle_t *x);

uint8_t *
rdb_blockhandle_write(uint8_t *zp, const rdb_blockhandle_t *x);

void
rdb_blockhandle_export(rdb_buffer_t *z, const rdb_blockhandle_t *x);

int
rdb_blockhandle_read(rdb_blockhandle_t *z, const uint8_t **xp, size_t *xn);

int
rdb_blockhandle_import(rdb_blockhandle_t *z, const rdb_slice_t *x);

/*
 * Footer
 */

void
rdb_footer_init(rdb_footer_t *x);

uint8_t *
rdb_footer_write(uint8_t *zp, const rdb_footer_t *x);

void
rdb_footer_export(rdb_buffer_t *z, const rdb_footer_t *x);

int
rdb_footer_read(rdb_footer_t *z, const uint8_t **xp, size_t *xn);

int
rdb_footer_import(rdb_footer_t *z, const rdb_slice_t *x);

/*
 * Block Contents
 */

void
rdb_blockcontents_init(rdb_blockcontents_t *x);

/*
 * Block Read
 */

int
rdb_read_block(rdb_blockcontents_t *result,
               struct rdb_rfile_s *file,
               const struct rdb_readopt_s *options,
               const rdb_blockhandle_t *handle);

#endif /* RDB_TABLE_FORMAT_H */
