/*!
 * version_edit.c - version edit for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdint.h>
#include <stdlib.h>

#include "util/buffer.h"
#include "util/coding.h"
#include "util/internal.h"
#include "util/rbt.h"
#include "util/slice.h"
#include "util/vector.h"

#include "dbformat.h"
#include "version_edit.h"

/*
 * Constants
 */

/* Tag numbers for serialized VersionEdit. These numbers are written to
   disk and should not be changed. */
enum {
  TAG_COMPARATOR = 1,
  TAG_LOG_NUMBER = 2,
  TAG_NEXT_FILE_NUMBER = 3,
  TAG_LAST_SEQUENCE = 4,
  TAG_COMPACT_POINTER = 5,
  TAG_DELETED_FILE = 6,
  TAG_NEW_FILE = 7,
  /* 8 was used for large value refs. */
  TAG_PREV_LOG_NUMBER = 9
};

/*
 * InternalKey Pair
 */

static ikey_entry_t *
ikey_entry_create(int level, const rdb_ikey_t *key) {
  ikey_entry_t *entry = rdb_malloc(sizeof(ikey_entry_t));

  entry->level = level;

  rdb_buffer_init(&entry->key);
  rdb_ikey_copy(&entry->key, key);

  return entry;
}

static void
ikey_entry_destroy(ikey_entry_t *entry) {
  rdb_ikey_clear(&entry->key);
  rdb_free(entry);
}

/*
 * FileNumber Pair
 */

static file_entry_t *
file_entry_create(int level, uint64_t number) {
  file_entry_t *entry = rdb_malloc(sizeof(file_entry_t));

  entry->level = level;
  entry->number = number;

  return entry;
}

static void
file_entry_destroy(file_entry_t *entry) {
  rdb_free(entry);
}

static int
file_entry_compare(rb_val_t x, rb_val_t y, void *arg) {
  file_entry_t *xp = x.p;
  file_entry_t *yp = y.p;

  (void)arg;

  if (xp->level != yp->level)
    return xp->level - yp->level;

  if (xp->number == yp->number)
    return 0;

  return xp->number < yp->number ? -1 : 1;
}

static void
file_entry_destruct(rb_node_t *node) {
  file_entry_destroy(node->key.p);
}

/*
 * FileMetaData Pair
 */

static meta_entry_t *
meta_entry_create(int level,
                  uint64_t number,
                  uint64_t file_size,
                  const rdb_ikey_t *smallest,
                  const rdb_ikey_t *largest) {
  meta_entry_t *entry = rdb_malloc(sizeof(meta_entry_t));

  entry->level = level;

  rdb_filemeta_init(&entry->meta);

  entry->meta.number = number;
  entry->meta.file_size = file_size;

  rdb_ikey_copy(&entry->meta.smallest, smallest);
  rdb_ikey_copy(&entry->meta.largest, largest);

  return entry;
}

static void
meta_entry_destroy(meta_entry_t *entry) {
  rdb_filemeta_clear(&entry->meta);
  rdb_free(entry);
}

/*
 * FileMetaData
 */

rdb_filemeta_t *
rdb_filemeta_create(void) {
  rdb_filemeta_t *meta = rdb_malloc(sizeof(rdb_filemeta_t));
  rdb_filemeta_init(meta);
  return meta;
}

void
rdb_filemeta_destroy(rdb_filemeta_t *meta) {
  rdb_filemeta_clear(meta);
  rdb_free(meta);
}

rdb_filemeta_t *
rdb_filemeta_clone(const rdb_filemeta_t *meta) {
  rdb_filemeta_t *out = rdb_filemeta_create();
  rdb_filemeta_copy(out, meta);
  return out;
}

void
rdb_filemeta_init(rdb_filemeta_t *meta) {
  meta->refs = 0;
  meta->allowed_seeks = (1 << 30);
  meta->number = 0;
  meta->file_size = 0;

  rdb_buffer_init(&meta->smallest);
  rdb_buffer_init(&meta->largest);
}

void
rdb_filemeta_clear(rdb_filemeta_t *meta) {
  rdb_ikey_clear(&meta->smallest);
  rdb_ikey_clear(&meta->largest);
}

void
rdb_filemeta_copy(rdb_filemeta_t *z, const rdb_filemeta_t *x) {
  z->refs = x->refs;
  z->allowed_seeks = x->allowed_seeks;
  z->number = x->number;
  z->file_size = x->file_size;

  rdb_ikey_copy(&z->smallest, &x->smallest);
  rdb_ikey_copy(&z->largest, &x->largest);
}

/*
 * VersionEdit
 */

void
rdb_vedit_init(rdb_vedit_t *edit) {
  rdb_buffer_init(&edit->comparator);

  edit->log_number = 0;
  edit->prev_log_number = 0;
  edit->last_sequence = 0;
  edit->next_file_number = 0;
  edit->has_comparator = 0;
  edit->has_log_number = 0;
  edit->has_prev_log_number = 0;
  edit->has_next_file_number = 0;
  edit->has_last_sequence = 0;

  rdb_vector_init(&edit->compact_pointers);
  rb_set_init(&edit->deleted_files, file_entry_compare, NULL);
  rdb_vector_init(&edit->new_files);
}

void
rdb_vedit_clear(rdb_vedit_t *edit) {
  size_t i;

  for (i = 0; i < edit->compact_pointers.length; i++)
    ikey_entry_destroy(edit->compact_pointers.items[i]);

  for (i = 0; i < edit->new_files.length; i++)
    meta_entry_destroy(edit->new_files.items[i]);

  rdb_buffer_clear(&edit->comparator);
  rdb_vector_clear(&edit->compact_pointers);
  rb_set_clear(&edit->deleted_files, file_entry_destruct);
  rdb_vector_clear(&edit->new_files);
}

void
rdb_vedit_reset(rdb_vedit_t *edit) {
  rdb_vedit_clear(edit);
  rdb_vedit_init(edit);
}

void
rdb_vedit_set_comparator_name(rdb_vedit_t *edit, const char *name) {
  edit->has_comparator = 1;
  rdb_buffer_set_str(&edit->comparator, name);
}

void
rdb_vedit_set_log_number(rdb_vedit_t *edit, uint64_t num) {
  edit->has_log_number = 1;
  edit->log_number = num;
}

void
rdb_vedit_set_prev_log_number(rdb_vedit_t *edit, uint64_t num) {
  edit->has_prev_log_number = 1;
  edit->prev_log_number = num;
}

void
rdb_vedit_set_next_file(rdb_vedit_t *edit, uint64_t num) {
  edit->has_next_file_number = 1;
  edit->next_file_number = num;
}

void
rdb_vedit_set_last_sequence(rdb_vedit_t *edit, rdb_seqnum_t seq) {
  edit->has_last_sequence = 1;
  edit->last_sequence = seq;
}

void
rdb_vedit_set_compact_pointer(rdb_vedit_t *edit,
                              int level,
                              const rdb_ikey_t *key) {
  ikey_entry_t *entry = ikey_entry_create(level, key);

  rdb_vector_push(&edit->compact_pointers, entry);
}

void
rdb_vedit_add_file(rdb_vedit_t *edit,
                   int level,
                   uint64_t number,
                   uint64_t file_size,
                   const rdb_ikey_t *smallest,
                   const rdb_ikey_t *largest) {
  meta_entry_t *entry = meta_entry_create(level,
                                          number,
                                          file_size,
                                          smallest,
                                          largest);

  rdb_vector_push(&edit->new_files, entry);
}

void
rdb_vedit_remove_file(rdb_vedit_t *edit, int level, uint64_t number) {
  file_entry_t *entry = file_entry_create(level, number);

  if (!rb_set_put(&edit->deleted_files, entry))
    file_entry_destroy(entry);
}

void
rdb_vedit_export(rdb_buffer_t *dst, const rdb_vedit_t *edit) {
  void *item;
  size_t i;

  if (edit->has_comparator) {
    rdb_buffer_varint32(dst, TAG_COMPARATOR);
    rdb_buffer_export(dst, &edit->comparator);
  }

  if (edit->has_log_number) {
    rdb_buffer_varint32(dst, TAG_LOG_NUMBER);
    rdb_buffer_varint64(dst, edit->log_number);
  }

  if (edit->has_prev_log_number) {
    rdb_buffer_varint32(dst, TAG_PREV_LOG_NUMBER);
    rdb_buffer_varint64(dst, edit->prev_log_number);
  }

  if (edit->has_next_file_number) {
    rdb_buffer_varint32(dst, TAG_NEXT_FILE_NUMBER);
    rdb_buffer_varint64(dst, edit->next_file_number);
  }

  if (edit->has_last_sequence) {
    rdb_buffer_varint32(dst, TAG_LAST_SEQUENCE);
    rdb_buffer_varint64(dst, edit->last_sequence);
  }

  for (i = 0; i < edit->compact_pointers.length; i++) {
    const ikey_entry_t *entry = edit->compact_pointers.items[i];
    const rdb_ikey_t *key = &entry->key;

    rdb_buffer_varint32(dst, TAG_COMPACT_POINTER);
    rdb_buffer_varint32(dst, entry->level);
    rdb_ikey_export(dst, key);
  }

  rb_set_iterate(&edit->deleted_files, item) {
    const file_entry_t *entry = item;

    rdb_buffer_varint32(dst, TAG_DELETED_FILE);
    rdb_buffer_varint32(dst, entry->level);
    rdb_buffer_varint64(dst, entry->number);
  }

  for (i = 0; i < edit->new_files.length; i++) {
    const meta_entry_t *entry = edit->new_files.items[i];
    const rdb_filemeta_t *meta = &entry->meta;

    rdb_buffer_varint32(dst, TAG_NEW_FILE);
    rdb_buffer_varint32(dst, entry->level);
    rdb_buffer_varint64(dst, meta->number);
    rdb_buffer_varint64(dst, meta->file_size);
    rdb_ikey_export(dst, &meta->smallest);
    rdb_ikey_export(dst, &meta->largest);
  }
}

static int
rdb_level_slurp(int *level, rdb_slice_t *input) {
  uint32_t val;

  if (!rdb_varint32_slurp(&val, input))
    return 0;

  if (val >= RDB_NUM_LEVELS)
    return 0;

  *level = val;

  return 1;
}

int
rdb_vedit_import(rdb_vedit_t *edit, const rdb_slice_t *src) {
  rdb_slice_t smallest, largest;
  uint64_t number, file_size;
  rdb_slice_t input = *src;
  rdb_slice_t key;
  uint32_t tag;
  int level;

  rdb_vedit_reset(edit);

  while (input.size > 0) {
    if (!rdb_varint32_slurp(&tag, &input))
      return 0;

    switch (tag) {
      case TAG_COMPARATOR: {
        if (!rdb_buffer_slurp(&edit->comparator, &input))
          return 0;

        edit->has_comparator = 1;

        break;
      }

      case TAG_LOG_NUMBER: {
        if (!rdb_varint64_slurp(&edit->log_number, &input))
          return 0;

        edit->has_log_number = 1;

        break;
      }

      case TAG_PREV_LOG_NUMBER: {
        if (!rdb_varint64_slurp(&edit->prev_log_number, &input))
          return 0;

        edit->has_prev_log_number = 1;

        break;
      }

      case TAG_NEXT_FILE_NUMBER: {
        if (!rdb_varint64_slurp(&edit->next_file_number, &input))
          return 0;

        edit->has_next_file_number = 1;

        break;
      }

      case TAG_LAST_SEQUENCE: {
        if (!rdb_varint64_slurp(&edit->last_sequence, &input))
          return 0;

        edit->has_last_sequence = 1;

        break;
      }

      case TAG_COMPACT_POINTER: {
        if (!rdb_level_slurp(&level, &input))
          return 0;

        if (!rdb_slice_slurp(&key, &input))
          return 0;

        rdb_vedit_set_compact_pointer(edit, level, &key);

        break;
      }

      case TAG_DELETED_FILE: {
        if (!rdb_level_slurp(&level, &input))
          return 0;

        if (!rdb_varint64_slurp(&number, &input))
          return 0;

        rdb_vedit_remove_file(edit, level, number);

        break;
      }

      case TAG_NEW_FILE: {
        if (!rdb_level_slurp(&level, &input))
          return 0;

        if (!rdb_varint64_slurp(&number, &input))
          return 0;

        if (!rdb_varint64_slurp(&file_size, &input))
          return 0;

        if (!rdb_slice_slurp(&smallest, &input))
          return 0;

        if (!rdb_slice_slurp(&largest, &input))
          return 0;

        rdb_vedit_add_file(edit, level, number, file_size, &smallest, &largest);

        break;
      }

      default: {
        return 0;
      }
    }
  }

  return 1;
}
