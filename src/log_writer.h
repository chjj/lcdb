/*!
 * log_writer.h - log writer for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_LOG_WRITER_H
#define RDB_LOG_WRITER_H

#include <stdint.h>

#include "util/types.h"
#include "log_format.h"

/*
 * Types
 */

struct rdb_wfile_s;

typedef struct rdb_logwriter_s {
  struct rdb_wfile_s *dest;
  int block_offset; /* Current offset in block. */

  /* crc32c values for all supported record types. These are
     pre-computed to reduce the overhead of computing the crc of the
     record type stored in the header. */
  uint32_t type_crc[RDB_MAX_RECTYPE + 1];
} rdb_logwriter_t;

/*
 * LogWriter
 */

/* Create a writer that will append data to "*dest".
 * "*dest" must have initial length "dest_length".
 * "*dest" must remain live while this Writer is in use.
 */
rdb_logwriter_t *
rdb_logwriter_create(struct rdb_wfile_s *dest, uint64_t length);

void
rdb_logwriter_destroy(rdb_logwriter_t *lw);

void
rdb_logwriter_init(rdb_logwriter_t *lw,
                   struct rdb_wfile_s *dest,
                   uint64_t length);

int
rdb_logwriter_add_record(rdb_logwriter_t *lw, const rdb_slice_t *slice);

#endif /* RDB_LOG_WRITER_H */
