/*!
 * strutil.h - string utilities for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_STRUTIL_H
#define RDB_STRUTIL_H

#include <stddef.h>
#include <stdint.h>

/*
 * String
 */

int
rdb_starts_with(const char *xp, const char *yp);

int
rdb_size_int(uint64_t x);

int
rdb_encode_int(char *zp, uint64_t x, int pad);

int
rdb_decode_int(uint64_t *z, const char **xp);

char *
rdb_basename(const char *fname);

int
rdb_dirname(char *buf, size_t size, const char *fname);

int
rdb_join(char *zp, size_t zn, const char *xp, const char *yp);

#endif /* RDB_STRUTIL_H */
