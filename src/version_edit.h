/*!
 * version_edit.h - version edit for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_VERSION_EDIT_H
#define RDB_VERSION_EDIT_H

#include <stddef.h>
#include <stdint.h>

#include "util/rbt.h"
#include "util/types.h"

#include "dbformat.h"

/*
 * Types
 */

typedef struct rdb_filemeta_s {
  int refs;
  int allowed_seeks;   /* Seeks allowed until compaction. */
  uint64_t number;
  uint64_t file_size;  /* File size in bytes. */
  rdb_ikey_t smallest; /* Smallest internal key served by table. */
  rdb_ikey_t largest;  /* Largest internal key served by table. */
} rdb_filemeta_t;

typedef struct rdb_vedit_s {
  rdb_buffer_t comparator;
  uint64_t log_number;
  uint64_t prev_log_number;
  uint64_t next_file_number;
  rdb_seqnum_t last_sequence;
  int has_comparator;
  int has_log_number;
  int has_prev_log_number;
  int has_next_file_number;
  int has_last_sequence;
  rdb_vector_t compact_pointers; /* ikey_entry_t */
  rb_tree_t deleted_files;       /* file_entry_t */
  rdb_vector_t new_files;        /* meta_entry_t */
} rdb_vedit_t;

typedef struct ikey_entry_s {
  int level;
  rdb_ikey_t key;
} ikey_entry_t;

typedef struct file_entry_s {
  int level;
  uint64_t number;
} file_entry_t;

typedef struct meta_entry_s {
  int level;
  rdb_filemeta_t meta;
} meta_entry_t;

/*
 * FileMetaData
 */

rdb_filemeta_t *
rdb_filemeta_create(void);

void
rdb_filemeta_destroy(rdb_filemeta_t *meta);

rdb_filemeta_t *
rdb_filemeta_clone(const rdb_filemeta_t *meta);

void
rdb_filemeta_init(rdb_filemeta_t *meta);

void
rdb_filemeta_clear(rdb_filemeta_t *meta);

void
rdb_filemeta_copy(rdb_filemeta_t *z, const rdb_filemeta_t *x);

/*
 * VersionEdit
 */

void
rdb_vedit_init(rdb_vedit_t *edit);

void
rdb_vedit_clear(rdb_vedit_t *edit);

void
rdb_vedit_reset(rdb_vedit_t *edit);

void
rdb_vedit_set_comparator_name(rdb_vedit_t *edit, const char *name);

void
rdb_vedit_set_log_number(rdb_vedit_t *edit, uint64_t num);

void
rdb_vedit_set_prev_log_number(rdb_vedit_t *edit, uint64_t num);

void
rdb_vedit_set_next_file(rdb_vedit_t *edit, uint64_t num);

void
rdb_vedit_set_last_sequence(rdb_vedit_t *edit, rdb_seqnum_t seq);

void
rdb_vedit_set_compact_pointer(rdb_vedit_t *edit,
                              int level,
                              const rdb_ikey_t *key);

/* Add the specified file at the specified number. */
/* REQUIRES: This version has not been saved (see vset_save_to). */
/* REQUIRES: "smallest" and "largest" are smallest and largest keys in file. */
void
rdb_vedit_add_file(rdb_vedit_t *edit,
                   int level,
                   uint64_t number,
                   uint64_t file_size,
                   const rdb_ikey_t *smallest,
                   const rdb_ikey_t *largest);

/* Delete the specified "file" from the specified "level". */
void
rdb_vedit_remove_file(rdb_vedit_t *edit, int level, uint64_t number);

void
rdb_vedit_export(rdb_buffer_t *dst, const rdb_vedit_t *edit);

int
rdb_vedit_import(rdb_vedit_t *edit, const rdb_slice_t *src);

#endif /* RDB_VERSION_EDIT_H */
