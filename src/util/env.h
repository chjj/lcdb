/*!
 * env.h - platform-specific functions for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on google/leveldb:
 *   Copyright (c) 2011, The LevelDB Authors. All rights reserved.
 *   https://github.com/google/leveldb
 *
 * See LICENSE for more information.
 */

#ifndef LDB_ENV_H
#define LDB_ENV_H

#include <stdarg.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>

#ifndef NDEBUG
#include "atomic.h"
#endif
#include "extern.h"
#include "slice.h"

/*
 * Constants
 */

#define LDB_PATH_MAX 1024

/*
 * Types
 */

typedef struct ldb_filelock_s ldb_filelock_t;
typedef struct ldb_logger_s ldb_logger_t;
typedef struct ldb_rfile_s ldb_rfile_t;
typedef struct ldb_wfile_s ldb_wfile_t;

/*
 * Globals
 */

#ifndef NDEBUG
extern struct ldb_env_state_s {
  /* Whether to enable error injection. */
  int enable_testing;
  /* sstable/log sync() calls are blocked. */
  ldb_atomic(int) delay_data_sync;
  /* sstable/log sync() calls return an error. */
  ldb_atomic(int) data_sync_error;
  /* Simulate no-space errors. */
  ldb_atomic(int) no_space;
  /* Simulate non-writable file system. */
  ldb_atomic(int) non_writable;
  /* Force sync of manifest files to fail. */
  ldb_atomic(int) manifest_sync_error;
  /* Force write to manifest files to fail. */
  ldb_atomic(int) manifest_write_error;
  /* Count random reads. */
  int count_random_reads;
  ldb_atomic(int) random_read_counter;
  /* Writable file errors. */
  int writable_file_error;
  int num_writable_file_errors;
} ldb_env_state;
#endif

/*
 * Errors
 */

int
ldb_system_error(void);

const char *
ldb_error_string(int code);

/*
 * Filesystem
 */

int
ldb_path_absolute(char *buf, size_t size, const char *name);

int
ldb_file_exists(const char *filename);

int
ldb_get_children(const char *path, char ***out);

void
ldb_free_children(char **list, int len);

int
ldb_remove_file(const char *filename);

int
ldb_create_dir(const char *dirname);

int
ldb_remove_dir(const char *dirname);

int
ldb_sync_dir(const char *dirname);

int
ldb_file_size(const char *filename, uint64_t *size);

int
ldb_rename_file(const char *from, const char *to);

int
ldb_copy_file(const char *from, const char *to);

int
ldb_link_file(const char *from, const char *to);

int
ldb_lock_file(const char *filename, ldb_filelock_t **lock);

int
ldb_unlock_file(ldb_filelock_t *lock);

int
ldb_write_file(const char *fname, const ldb_slice_t *data, int should_sync);

int
ldb_read_file(const char *fname, ldb_buffer_t *data);

LDB_EXTERN int
ldb_test_directory(char *result, size_t size);

LDB_EXTERN int
ldb_test_filename(char *result, size_t size, const char *name);

/*
 * ReadableFile
 */

int
ldb_seqfile_create(const char *filename, ldb_rfile_t **file);

int
ldb_randfile_create(const char *filename, ldb_rfile_t **file, int use_mmap);

int
ldb_rfile_mapped(ldb_rfile_t *file);

int
ldb_rfile_read(ldb_rfile_t *file,
               ldb_slice_t *result,
               void *buf,
               size_t count);

int
ldb_rfile_skip(ldb_rfile_t *file, uint64_t offset);

int
ldb_rfile_pread(ldb_rfile_t *file,
                ldb_slice_t *result,
                void *buf,
                size_t count,
                uint64_t offset);

void
ldb_rfile_destroy(ldb_rfile_t *file);

/*
 * WritableFile
 */

int
ldb_truncfile_create(const char *filename, ldb_wfile_t **file);

int
ldb_appendfile_create(const char *filename, ldb_wfile_t **file);

int
ldb_wfile_append(ldb_wfile_t *file, const ldb_slice_t *data);

int
ldb_wfile_flush(ldb_wfile_t *file);

int
ldb_wfile_sync(ldb_wfile_t *file);

int
ldb_wfile_close(ldb_wfile_t *file);

void
ldb_wfile_destroy(ldb_wfile_t *file);

/*
 * Logging
 */

LDB_EXTERN ldb_logger_t *
ldb_logger_create(void (*logv)(void *, const char *, va_list), void *state);

LDB_EXTERN void
ldb_logger_destroy(ldb_logger_t *logger);

void
ldb_log(ldb_logger_t *logger, const char *fmt, ...);

ldb_logger_t *
ldb_logger_fopen(FILE *stream);

int
ldb_logger_open(const char *filename, ldb_logger_t **result);

/*
 * Time
 */

int64_t
ldb_now_usec(void);

void
ldb_sleep_usec(int64_t usec);

#endif /* LDB_ENV_H */
