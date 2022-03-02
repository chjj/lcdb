/*!
 * filename.h - filename utilities for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_FILENAME_H
#define RDB_FILENAME_H

#include <stddef.h>
#include <stdint.h>

/*
 * Constants
 */

typedef enum rdb_filetype {
  RDB_FILE_LOG,
  RDB_FILE_LOCK,
  RDB_FILE_TABLE,
  RDB_FILE_DESC,
  RDB_FILE_CURRENT,
  RDB_FILE_TEMP,
  RDB_FILE_INFO /* Either the current one, or an old one */
} rdb_filetype_t;

/*
 * Helpers
 */

#define starts_with rdb_starts_with
#define encode_int rdb_encode_int
#define decode_int rdb_decode_int

int
starts_with(const char *xp, const char *yp);

int
encode_int(char *zp, uint64_t x, int pad);

int
decode_int(uint64_t *z, const char **xp);

/*
 * Filename
 */

/* Return the name of the log file with the specified number
   in the db named by "prefix". The result will be prefixed with
   "prefix". */
int
rdb_log_filename(char *buf, size_t size, const char *prefix, uint64_t num);

/* Return the name of the sstable with the specified number
   in the db named by "prefix". The result will be prefixed with
   "prefix". */
int
rdb_table_filename(char *buf, size_t size, const char *prefix, uint64_t num);

/* Return the legacy file name for an sstable with the specified number
   in the db named by "prefix". The result will be prefixed with
   "prefix". */
int
rdb_sstable_filename(char *buf, size_t size, const char *prefix, uint64_t num);

/* Return the name of the descriptor file for the db named by
   "prefix" and the specified incarnation number. The result will be
   prefixed with "prefix". */
int
rdb_desc_filename(char *buf, size_t size, const char *prefix, uint64_t num);

/* Return the name of the current file. This file contains the name
   of the current manifest file. The result will be prefixed with
   "prefix". */
int
rdb_current_filename(char *buf, size_t size, const char *prefix);

/* Return the name of the lock file for the db named by
   "prefix". The result will be prefixed with "prefix". */
int
rdb_lock_filename(char *buf, size_t size, const char *prefix);

/* Return the name of a temporary file owned by the db named "prefix".
   The result will be prefixed with "prefix". */
int
rdb_temp_filename(char *buf, size_t size, const char *prefix, uint64_t num);

/* Return the name of the info log file for "prefix". */
int
rdb_info_filename(char *buf, size_t size, const char *prefix);

/* Return the name of the old info log file for "prefix". */
int
rdb_oldinfo_filename(char *buf, size_t size, const char *prefix);

/* If filename is a leveldb file, store the type of the file in *type.
   The number encoded in the filename is stored in *num. If the
   filename was successfully parsed, returns true. Else return false. */
int
rdb_parse_filename(rdb_filetype_t *type, uint64_t *num, const char *name);

/* Join path components. */
int
rdb_path_join(char *buf, size_t size, ...);

/* Make the CURRENT file point to the descriptor file with the
   specified number. */
int
rdb_set_current_file(const char *prefix, uint64_t desc_number);

#endif /* RDB_FILENAME_H */
