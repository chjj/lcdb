/*!
 * env.h - platform-specific functions for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_ENV_H
#define RDB_ENV_H

#include <stddef.h>
#include <stdint.h>

#include "extern.h"
#include "slice.h"

/*
 * Constants
 */

#define RDB_PATH_MAX 1024

#if defined(_WIN32)
#  define RDB_PATH_SEP '\\'
#else
#  define RDB_PATH_SEP '/'
#endif

/*
 * Types
 */

typedef struct rdb_filelock_s rdb_filelock_t;
typedef struct rdb_logger_s rdb_logger_t;
typedef struct rdb_rfile_s rdb_rfile_t;
typedef struct rdb_wfile_s rdb_wfile_t;

/*
 * Environment
 */

void
rdb_env_init(void);

RDB_EXTERN void
rdb_env_clear(void);

/*
 * Filesystem
 */

int
rdb_path_join(char *zp, size_t zn, const char *xp, const char *yp);

int
rdb_path_absolute(char *buf, size_t size, const char *name);

int
rdb_file_exists(const char *filename);

int
rdb_get_children(const char *path, char ***out);

void
rdb_free_children(char **list, int len);

int
rdb_remove_file(const char *filename);

int
rdb_create_dir(const char *dirname);

int
rdb_remove_dir(const char *dirname);

int
rdb_get_file_size(const char *filename, uint64_t *size);

int
rdb_rename_file(const char *from, const char *to);

int
rdb_lock_file(const char *filename, rdb_filelock_t **lock);

int
rdb_unlock_file(rdb_filelock_t *lock);

int
rdb_test_directory(char *result, size_t size);

/*
 * Readable File
 */

int
rdb_seqfile_create(const char *filename, rdb_rfile_t **file);

int
rdb_randfile_create(const char *filename, rdb_rfile_t **file);

void
rdb_rfile_destroy(rdb_rfile_t *file);

int
rdb_rfile_mapped(rdb_rfile_t *file);

int
rdb_rfile_read(rdb_rfile_t *file,
               rdb_slice_t *result,
               void *buf,
               size_t count);

int
rdb_rfile_skip(rdb_rfile_t *file, uint64_t offset);

int
rdb_rfile_pread(rdb_rfile_t *file,
                rdb_slice_t *result,
                void *buf,
                size_t count,
                uint64_t offset);

/*
 * Writable File
 */

int
rdb_truncfile_create(const char *filename, rdb_wfile_t **file);

int
rdb_appendfile_create(const char *filename, rdb_wfile_t **file);

void
rdb_wfile_destroy(rdb_wfile_t *file);

int
rdb_wfile_close(rdb_wfile_t *file);

int
rdb_wfile_append(rdb_wfile_t *file, const rdb_slice_t *data);

int
rdb_wfile_flush(rdb_wfile_t *file);

int
rdb_wfile_sync(rdb_wfile_t *file);

/*
 * Logging
 */

RDB_EXTERN int
rdb_logger_open(const char *filename, rdb_logger_t **result);

RDB_EXTERN void
rdb_logger_destroy(rdb_logger_t *logger);

RDB_EXTERN void
rdb_log(rdb_logger_t *logger, const char *fmt, ...);

/*
 * Time
 */

int64_t
rdb_now_usec(void);

void
rdb_sleep_usec(int64_t usec);

/*
 * Extra
 */

int
rdb_write_file(const char *fname, const rdb_slice_t *data, int should_sync);

int
rdb_read_file(const char *fname, rdb_buffer_t *data);

#endif /* RDB_ENV_H */
