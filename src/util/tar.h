/*!
 * tar.h - tar implementation for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 */

#ifndef LDB_TAR_H
#define LDB_TAR_H

#include "types.h"

/*
 * Types
 */

struct ldb_wfile_s;

typedef struct ldb_tar_s {
  struct ldb_wfile_s *dst;
  unsigned int mtime;
  char *scratch;
} ldb_tar_t;

/*
 * TarWriter
 */

void
ldb_tar_init(ldb_tar_t *tar, struct ldb_wfile_s *dst);

void
ldb_tar_clear(ldb_tar_t *tar);

int
ldb_tar_append(ldb_tar_t *tar, const char *name, const char *path);

int
ldb_tar_finish(ldb_tar_t *tar);

/*
 * TarReader
 */

int
ldb_tar_extract(const char *from,
                const char *to,
                int should_sync,
                const char *required);

#endif /* LDB_TAR_H */
