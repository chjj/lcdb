/*!
 * repair.c - database repairing for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "table/iterator.h"
#include "table/table.h"
#include "table/table_builder.h"

#include "util/array.h"
#include "util/bloom.h"
#include "util/buffer.h"
#include "util/cache.h"
#include "util/comparator.h"
#include "util/env.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/vector.h"

#include "builder.h"
#include "db_impl.h"
#include "dbformat.h"
#include "filename.h"
#include "log_format.h"
#include "log_reader.h"
#include "log_writer.h"
#include "memtable.h"
#include "table_cache.h"
#include "version_edit.h"
#include "write_batch.h"

/* We recover the contents of the descriptor from the other files we find.
 * (1) Any log files are first converted to tables
 *
 * (2) We scan every table to compute
 *     (a) smallest/largest for the table
 *     (b) largest sequence number in the table
 *
 * (3) We generate descriptor contents:
 *      - log number is set to zero
 *      - next-file-number is set to 1 + largest file number we found
 *      - last-sequence-number is set to largest sequence# found across
 *        all tables (see 2c)
 *      - compaction pointers are cleared
 *      - every table file is added at level 0
 *
 * Possible optimization 1:
 *   (a) Compute total size and use to pick appropriate max-level M
 *   (b) Sort tables by largest sequence# in the table
 *   (c) For each table: if it overlaps earlier table, place in level-0,
 *       else place in level-M.
 *
 * Possible optimization 2:
 *   Store per-table metadata (smallest, largest, largest-seq#, ...)
 *   in the table's meta section to speed up scan_table.
 */

/*
 * TableInfo
 */

typedef struct rdb_tabinfo_s {
  rdb_filemeta_t meta;
  rdb_seqnum_t max_sequence;
} rdb_tabinfo_t;

static rdb_tabinfo_t *
tabinfo_create(void) {
  rdb_tabinfo_t *t = rdb_malloc(sizeof(rdb_tabinfo_t));

  rdb_filemeta_init(&t->meta);

  t->max_sequence = 0;

  return t;
}

static void
tabinfo_destroy(rdb_tabinfo_t *t) {
  rdb_filemeta_clear(&t->meta);
  rdb_free(t);
}

/*
 * Repairer
 */

typedef struct rdb_repair_s {
  char dbname[RDB_PATH_MAX];
  rdb_comparator_t icmp;
  rdb_bloom_t ipolicy;
  rdb_dbopt_t options;
  int owns_info_log;
  int owns_cache;
  rdb_tcache_t *table_cache;
  rdb_vedit_t edit;
  rdb_array_t manifests;
  rdb_array_t table_numbers;
  rdb_array_t logs;
  rdb_vector_t tables; /* rdb_tabinfo_t */
  uint64_t next_file_number;
} rdb_repair_t;

static int
repair_init(rdb_repair_t *rep, const char *dbname, const rdb_dbopt_t *options) {
  if (!rdb_path_absolute(rep->dbname, sizeof(rep->dbname) - 64, dbname))
    return 0;

  if (options->comparator != NULL)
    rdb_ikc_init(&rep->icmp, options->comparator);
  else
    rdb_ikc_init(&rep->icmp, rdb_bytewise_comparator);

  if (options->filter_policy != NULL)
    rdb_ifp_init(&rep->ipolicy, options->filter_policy);
  else
    rdb_ifp_init(&rep->ipolicy, rdb_bloom_default);

  rep->options = rdb_sanitize_options(dbname,
                                      &rep->icmp,
                                      &rep->ipolicy,
                                      options);

  rep->owns_info_log = rep->options.info_log != options->info_log;
  rep->owns_cache = rep->options.block_cache != options->block_cache;

  /* table_cache can be small since we expect each table to be opened once. */
  rep->table_cache = rdb_tcache_create(rep->dbname, &rep->options, 10);

  rdb_vedit_init(&rep->edit);
  rdb_array_init(&rep->manifests);
  rdb_array_init(&rep->table_numbers);
  rdb_array_init(&rep->logs);
  rdb_vector_init(&rep->tables);

  rep->next_file_number = 1;

  return 1;
}

void
repair_clear(rdb_repair_t *rep) {
  size_t i;

  rdb_tcache_destroy(rep->table_cache);

  if (rep->owns_info_log)
    rdb_logger_destroy(rep->options.info_log);

  if (rep->owns_cache)
    rdb_lru_destroy(rep->options.block_cache);

  for (i = 0; i < rep->tables.length; i++)
    tabinfo_destroy(rep->tables.items[i]);

  rdb_vedit_clear(&rep->edit);
  rdb_array_clear(&rep->manifests);
  rdb_array_clear(&rep->table_numbers);
  rdb_array_clear(&rep->logs);
  rdb_vector_clear(&rep->tables);
}

static int
find_files(rdb_repair_t *rep) {
  rdb_filetype_t type;
  uint64_t number;
  char **filenames;
  int i, len;

  len = rdb_get_children(rep->dbname, &filenames);

  if (len < 0)
    return RDB_IOERR;

  if (len == 0) {
    rdb_free_children(filenames, len);
    return RDB_IOERR; /* "repair found no files" */
  }

  for (i = 0; i < len; i++) {
    const char *filename = filenames[i];

    if (!rdb_parse_filename(&type, &number, filename))
      continue;

    if (type == RDB_FILE_DESC) {
      rdb_array_push(&rep->manifests, number);
    } else {
      if (number + 1 > rep->next_file_number)
        rep->next_file_number = number + 1;

      if (type == RDB_FILE_LOG)
        rdb_array_push(&rep->logs, number);
      else if (type == RDB_FILE_TABLE)
        rdb_array_push(&rep->table_numbers, number);
    }
  }

  rdb_free_children(filenames, len);

  return RDB_OK;
}

static void
archive_file(const char *fname) {
  /* Move into another directory. e.g. for
   *    dir/foo
   * rename to
   *    dir/lost/foo
   */
  char newfile[RDB_PATH_MAX];
  char newdir[RDB_PATH_MAX];
  char dir[RDB_PATH_MAX];
  const char *base;
  char *slash;

  assert(strlen(fname) + 1 <= sizeof(dir));

  slash = strrchr(strcpy(dir, fname), RDB_PATH_SEP);

  if (slash == NULL)
    strcpy(dir, ".");
  else
    *slash = '\0';

  if (!rdb_path_join(newdir, sizeof(newdir), dir, "lost"))
    abort(); /* LCOV_EXCL_LINE */

  base = strrchr(fname, RDB_PATH_SEP);

  if (base == NULL)
    base = (char *)fname;
  else
    base += 1;

  if (!rdb_path_join(newfile, sizeof(newfile), newdir, base))
    abort(); /* LCOV_EXCL_LINE */

  rdb_create_dir(newdir); /* Ignore error. */
  rdb_rename_file(fname, newfile);
}

static void
report_corruption(rdb_reporter_t *reporter, size_t bytes, int status) {
  (void)reporter;
  (void)bytes;
  (void)status;
}

static int
convert_log_to_table(rdb_repair_t *rep, uint64_t log) {
  /* Open the log file. */
  char logname[RDB_PATH_MAX];
  rdb_reporter_t reporter;
  rdb_logreader_t reader;
  rdb_rfile_t *lfile;
  rdb_buffer_t scratch;
  rdb_slice_t record;
  rdb_batch_t batch;
  rdb_memtable_t *mem;
  rdb_filemeta_t meta;
  rdb_iter_t *iter;
  int rc, counter;

  if (!rdb_log_filename(logname, sizeof(logname), rep->dbname, log))
    abort(); /* LCOV_EXCL_LINE */

  rc = rdb_seqfile_create(logname, &lfile);

  if (rc != RDB_OK)
    return rc;

  /* Create the log reader. */
  reporter.info_log = rep->options.info_log;
  reporter.lognum = log;
  reporter.corruption = report_corruption;

  /* We intentionally make log::Reader do checksumming so that
     corruptions cause entire commits to be skipped instead of
     propagating bad information (like overly large sequence
     numbers). */
  rdb_logreader_init(&reader, lfile, &reporter, 0, 0);
  rdb_buffer_init(&scratch);
  rdb_slice_init(&record);
  rdb_batch_init(&batch);

  /* Read all the records and add to a memtable. */
  mem = rdb_memtable_create(&rep->icmp);
  counter = 0;

  rdb_memtable_ref(mem);

  while (rdb_logreader_read_record(&reader, &record, &scratch)) {
    if (record.size < 12) {
      reporter.corruption(&reporter, record.size, RDB_CORRUPTION);
      continue;
    }

    rdb_batch_set_contents(&batch, &record);

    rc = rdb_batch_insert_into(&batch, mem);

    if (rc == RDB_OK)
      counter += rdb_batch_count(&batch);
    else
      rc = RDB_OK; /* Keep going with rest of file. */
  }

  rdb_batch_clear(&batch);
  rdb_buffer_clear(&scratch);
  rdb_logreader_clear(&reader);
  rdb_rfile_destroy(lfile);

  /* Do not record a version edit for this conversion to a Table
     since extract_meta_data() will also generate edits. */
  rdb_filemeta_init(&meta);

  meta.number = rep->next_file_number++;

  iter = rdb_memiter_create(mem);

  rc = rdb_build_table(rep->dbname,
                       &rep->options,
                       rep->table_cache,
                       iter,
                       &meta);

  rdb_iter_destroy(iter);

  rdb_memtable_unref(mem);
  mem = NULL;

  if (rc == RDB_OK) {
    if (meta.file_size > 0)
      rdb_array_push(&rep->table_numbers, meta.number);
  }

  rdb_filemeta_clear(&meta);

  return rc;
}

static void
convert_logs_to_tables(rdb_repair_t *rep) {
  char fname[RDB_PATH_MAX];
  size_t i;

  for (i = 0; i < rep->logs.length; i++) {
    uint64_t log = rep->logs.items[i];

    if (!rdb_log_filename(fname, sizeof(fname), rep->dbname, log))
      abort(); /* LCOV_EXCL_LINE */

    convert_log_to_table(rep, log);
    archive_file(fname);
  }
}

static rdb_iter_t *
tableiter_create(rdb_repair_t *rep, const rdb_filemeta_t *meta) {
  /* Same as compaction iterators: if paranoid_checks
     are on, turn on checksum verification. */
  rdb_readopt_t options = *rdb_readopt_default;

  options.verify_checksums = rep->options.paranoid_checks;

  return rdb_tcache_iterate(rep->table_cache,
                            &options,
                            meta->number,
                            meta->file_size,
                            NULL);
}

static void
repair_table(rdb_repair_t *rep, const char *src, rdb_tabinfo_t *t) {
  /* We will copy src contents to a new table and then rename the
     new table over the source. */
  rdb_tablebuilder_t *builder;
  char copy[RDB_PATH_MAX];
  char orig[RDB_PATH_MAX];
  rdb_wfile_t *file;
  rdb_iter_t *iter;
  int counter = 0;
  int rc;

  /* Create builder. */
  if (!rdb_table_filename(copy, sizeof(copy), rep->dbname,
                          rep->next_file_number++)) {
    abort(); /* LCOV_EXCL_LINE */
  }

  rc = rdb_truncfile_create(copy, &file);

  if (rc != RDB_OK) {
    tabinfo_destroy(t);
    return;
  }

  builder = rdb_tablebuilder_create(&rep->options, file);

  /* Copy data. */
  iter = tableiter_create(rep, &t->meta);
  counter = 0;

  for (rdb_iter_seek_first(iter); rdb_iter_valid(iter); rdb_iter_next(iter)) {
    rdb_slice_t key = rdb_iter_key(iter);
    rdb_slice_t val = rdb_iter_value(iter);

    rdb_tablebuilder_add(builder, &key, &val);

    counter++;
  }

  rdb_iter_destroy(iter);

  archive_file(src);

  if (counter == 0) {
    rdb_tablebuilder_abandon(builder); /* Nothing to save. */
  } else {
    rc = rdb_tablebuilder_finish(builder);

    if (rc == RDB_OK)
      t->meta.file_size = rdb_tablebuilder_file_size(builder);
  }

  rdb_tablebuilder_destroy(builder);
  builder = NULL;

  if (rc == RDB_OK)
    rc = rdb_wfile_close(file);

  rdb_wfile_destroy(file);
  file = NULL;

  if (counter > 0 && rc == RDB_OK) {
    if (!rdb_table_filename(orig, sizeof(orig), rep->dbname, t->meta.number))
      abort(); /* LCOV_EXCL_LINE */

    rc = rdb_rename_file(copy, orig);

    if (rc == RDB_OK) {
      rdb_vector_push(&rep->tables, t);
      t = NULL;
    }
  }

  if (rc != RDB_OK)
    rdb_remove_file(copy);

  if (t != NULL)
    tabinfo_destroy(t);
}

static void
scan_table(rdb_repair_t *rep, uint64_t number) {
  char fname[RDB_PATH_MAX];
  uint64_t file_size = 0;
  int counter, empty;
  rdb_pkey_t parsed;
  rdb_iter_t *iter;
  rdb_tabinfo_t *t;
  int rc, status;

  if (!rdb_table_filename(fname, sizeof(fname), rep->dbname, number))
    abort(); /* LCOV_EXCL_LINE */

  rc = rdb_get_file_size(fname, &file_size);

  if (rc != RDB_OK) {
    /* Try alternate file name. */
    if (!rdb_sstable_filename(fname, sizeof(fname), rep->dbname, number))
      abort(); /* LCOV_EXCL_LINE */

    status = rdb_get_file_size(fname, &file_size);

    if (status == RDB_OK)
      rc = RDB_OK;
  }

  if (rc != RDB_OK) {
    rdb_table_filename(fname, sizeof(fname), rep->dbname, number);
    archive_file(fname);

    rdb_sstable_filename(fname, sizeof(fname), rep->dbname, number);
    archive_file(fname);

    return;
  }

  t = tabinfo_create();
  t->meta.file_size = file_size;
  t->meta.number = number;

  /* Extract metadata by scanning through table. */
  iter = tableiter_create(rep, &t->meta);
  counter = 0;
  empty = 1;

  t->max_sequence = 0;

  for (rdb_iter_seek_first(iter); rdb_iter_valid(iter); rdb_iter_next(iter)) {
    rdb_slice_t key = rdb_iter_key(iter);

    if (!rdb_pkey_import(&parsed, &key))
      continue;

    counter++;

    if (empty) {
      rdb_ikey_copy(&t->meta.smallest, &key);
      empty = 0;
    }

    rdb_ikey_copy(&t->meta.largest, &key);

    if (parsed.sequence > t->max_sequence)
      t->max_sequence = parsed.sequence;
  }

  if (rdb_iter_status(iter) != RDB_OK)
    rc = rdb_iter_status(iter);

  rdb_iter_destroy(iter);

  if (rc == RDB_OK)
    rdb_vector_push(&rep->tables, t);
  else
    repair_table(rep, fname, t); /* repair_table archives input file. */
}

static void
extract_meta_data(rdb_repair_t *rep) {
  size_t i;

  for (i = 0; i < rep->table_numbers.length; i++)
    scan_table(rep, rep->table_numbers.items[i]);
}

static int
write_descriptor(rdb_repair_t *rep) {
  rdb_seqnum_t max_sequence = 0;
  char fname[RDB_PATH_MAX];
  char tmp[RDB_PATH_MAX];
  rdb_wfile_t *file;
  size_t i;
  int rc;

  if (!rdb_temp_filename(tmp, sizeof(tmp), rep->dbname, 1))
    abort(); /* LCOV_EXCL_LINE */

  rc = rdb_truncfile_create(tmp, &file);

  if (rc != RDB_OK)
    return rc;

  for (i = 0; i < rep->tables.length; i++) {
    const rdb_tabinfo_t *t = rep->tables.items[i];

    if (max_sequence < t->max_sequence)
      max_sequence = t->max_sequence;
  }

  rdb_vedit_set_comparator_name(&rep->edit, rep->icmp.user_comparator->name);
  rdb_vedit_set_log_number(&rep->edit, 0);
  rdb_vedit_set_next_file(&rep->edit, rep->next_file_number);
  rdb_vedit_set_last_sequence(&rep->edit, max_sequence);

  for (i = 0; i < rep->tables.length; i++) {
    const rdb_tabinfo_t *t = rep->tables.items[i];

    rdb_vedit_add_file(&rep->edit, 0, t->meta.number,
                                      t->meta.file_size,
                                      &t->meta.smallest,
                                      &t->meta.largest);
  }

  {
    rdb_logwriter_t log;
    rdb_buffer_t record;

    rdb_logwriter_init(&log, file, 0);
    rdb_buffer_init(&record);

    rdb_vedit_export(&record, &rep->edit);

    rc = rdb_logwriter_add_record(&log, &record);

    rdb_buffer_clear(&record);
  }

  if (rc == RDB_OK)
    rc = rdb_wfile_close(file);

  rdb_wfile_destroy(file);
  file = NULL;

  if (rc != RDB_OK) {
    rdb_remove_file(tmp);
  } else {
    /* Discard older manifests. */
    for (i = 0; i < rep->manifests.length; i++) {
      uint64_t number = rep->manifests.items[i];

      if (!rdb_desc_filename(fname, sizeof(fname), rep->dbname, number))
        abort(); /* LCOV_EXCL_LINE */

      archive_file(fname);
    }

    /* Install new manifest. */
    if (!rdb_desc_filename(fname, sizeof(fname), rep->dbname, 1))
      abort(); /* LCOV_EXCL_LINE */

    rc = rdb_rename_file(tmp, fname);

    if (rc == RDB_OK)
      rc = rdb_set_current_file(rep->dbname, 1);
    else
      rdb_remove_file(tmp);
  }

  return rc;
}

static int
repair_run(rdb_repair_t *rep) {
  int rc = find_files(rep);

  if (rc == RDB_OK) {
    convert_logs_to_tables(rep);
    extract_meta_data(rep);

    rc = write_descriptor(rep);
  }

  return rc;
}

int
rdb_repair_db(const char *dbname, const rdb_dbopt_t *options) {
  rdb_repair_t rep;
  int rc;

  rdb_env_init();

  if (!repair_init(&rep, dbname, options))
    return RDB_INVALID;

  rc = repair_run(&rep);

  repair_clear(&rep);

  return rc;
}
