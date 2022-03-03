/*!
 * dumpfile.h - file dumps for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_DUMPFILE_H
#define RDB_DUMPFILE_H

#include <stdio.h>
#include "util/extern.h"

/* Dump the contents of the file named by fname in text format to
 * *dst. Makes a sequence of fwrite calls; each call is passed
 * the newline-terminated text corresponding to a single item found
 * in the file.
 *
 * Returns a non-OK result if fname does not name a leveldb storage
 * file, or if the file cannot be read.
 */
RDB_EXTERN int
rdb_dump_file(const char *fname, FILE *dst);

#endif /* RDB_DUMPFILE_H */
