/*!
 * dumpfile.c - file dumps for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "table/iterator.h"
#include "table/table.h"

#include "util/buffer.h"
#include "util/env.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/strutil.h"

#include "dbformat.h"
#include "dumpfile.h"
#include "filename.h"
#include "log_reader.h"
#include "version_edit.h"
#include "write_batch.h"

/*
 * Helpers
 */

static void
stream_append(FILE *stream, const rdb_slice_t *x) {
  fwrite(x->data, 1, x->size, stream);
}

static int
guess_type(const char *fname, rdb_filetype_t *type) {
  const char *base = rdb_basename(fname);
  uint64_t ignored;

  return rdb_parse_filename(type, &ignored, base);
}

/*
 * DumpFile
 */

/* Notified when log reader encounters corruption. */
static void
report_corruption(rdb_reporter_t *report, size_t bytes, int status) {
  rdb_buffer_t r;

  rdb_buffer_init(&r);
  rdb_buffer_string(&r, "corruption: ");
  rdb_buffer_number(&r, bytes);

  rdb_buffer_string(&r, " bytes; ");
  rdb_buffer_string(&r, rdb_strerror(status));
  rdb_buffer_push(&r, '\n');

  stream_append(report->dst, &r);
  rdb_buffer_clear(&r);
}

/* Print contents of a log file. (*func)() is called on every record. */
static int
print_log_contents(const char *fname,
                   void (*func)(uint64_t, const rdb_slice_t *, FILE *),
                   FILE *dst) {
  rdb_reporter_t reporter;
  rdb_logreader_t reader;
  rdb_buffer_t scratch;
  rdb_slice_t record;
  rdb_rfile_t *file;
  int rc;

  rc = rdb_seqfile_create(fname, &file);

  if (rc != RDB_OK)
    return rc;

  reporter.dst = dst;
  reporter.corruption = report_corruption;

  rdb_logreader_init(&reader, file, &reporter, 1, 0);
  rdb_buffer_init(&scratch);

  while (rdb_logreader_read_record(&reader, &record, &scratch))
    func(reader.last_offset, &record, dst);

  rdb_buffer_clear(&scratch);
  rdb_logreader_clear(&reader);
  rdb_rfile_destroy(file);

  return RDB_OK;
}

/* Called on every item found in a WriteBatch. */
static void
handle_put(rdb_handler_t *h, const rdb_slice_t *key, const rdb_slice_t *value) {
  FILE *dst = h->state;
  rdb_buffer_t r;

  rdb_buffer_init(&r);
  rdb_buffer_string(&r, "  put '");
  rdb_buffer_escape(&r, key);
  rdb_buffer_string(&r, "' '");
  rdb_buffer_escape(&r, value);
  rdb_buffer_string(&r, "'\n");

  stream_append(dst, &r);
  rdb_buffer_clear(&r);
}

static void
handle_del(rdb_handler_t *h, const rdb_slice_t *key) {
  FILE *dst = h->state;
  rdb_buffer_t r;

  rdb_buffer_init(&r);
  rdb_buffer_string(&r, "  del '");
  rdb_buffer_escape(&r, key);
  rdb_buffer_string(&r, "'\n");

  stream_append(dst, &r);
  rdb_buffer_clear(&r);
}

/* Called on every log record (each one of which is a WriteBatch)
   found in a RDB_FILE_LOG. */
static void
write_batch_printer(uint64_t pos, const rdb_slice_t *record, FILE *dst) {
  rdb_handler_t printer;
  rdb_batch_t batch;
  rdb_buffer_t r;
  int rc;

  rdb_buffer_init(&r);
  rdb_buffer_string(&r, "--- offset ");
  rdb_buffer_number(&r, pos);
  rdb_buffer_string(&r, "; ");

  if (record->size < 12) {
    rdb_buffer_string(&r, "log record length ");
    rdb_buffer_number(&r, record->size);
    rdb_buffer_string(&r, " is too small\n");
    stream_append(dst, &r);
    rdb_buffer_clear(&r);
    return;
  }

  rdb_batch_init(&batch);
  rdb_batch_set_contents(&batch, record);

  rdb_buffer_string(&r, "sequence ");
  rdb_buffer_number(&r, rdb_batch_sequence(&batch));
  rdb_buffer_push(&r, '\n');

  stream_append(dst, &r);

  printer.state = dst;
  printer.put = handle_put;
  printer.del = handle_del;

  rc = rdb_batch_iterate(&batch, &printer);

  if (rc != RDB_OK) {
    rdb_buffer_reset(&r);
    rdb_buffer_string(&r, "  error: ");
    rdb_buffer_string(&r, rdb_strerror(rc));
    rdb_buffer_push(&r, '\n');
    stream_append(dst, &r);
  }

  rdb_buffer_clear(&r);
  rdb_batch_clear(&batch);
}

static int
dump_log(const char *fname, FILE *dst) {
  return print_log_contents(fname, write_batch_printer, dst);
}

/* Called on every log record (each one of which is a WriteBatch)
   found in a RDB_FILE_DESC. */
static void
edit_printer(uint64_t pos, const rdb_slice_t *record, FILE *dst) {
  rdb_vedit_t edit;
  rdb_buffer_t r;

  rdb_vedit_init(&edit);

  rdb_buffer_init(&r);
  rdb_buffer_string(&r, "--- offset ");
  rdb_buffer_number(&r, pos);
  rdb_buffer_string(&r, "; ");

  if (!rdb_vedit_import(&edit, record)) {
    rdb_buffer_string(&r, rdb_strerror(RDB_CORRUPTION));
    rdb_buffer_push(&r, '\n');
  } else {
    rdb_vedit_debug(&r, &edit);
  }

  stream_append(dst, &r);

  rdb_buffer_clear(&r);
  rdb_vedit_clear(&edit);
}

static int
dump_descriptor(const char *fname, FILE *dst) {
  return print_log_contents(fname, edit_printer, dst);
}

static int
dump_table(const char *fname, FILE *dst) {
  rdb_readopt_t ro = *rdb_readopt_default;
  rdb_rfile_t *file = NULL;
  rdb_table_t *table = NULL;
  uint64_t file_size = 0;
  rdb_iter_t *iter;
  rdb_buffer_t r;
  int rc;

  rc = rdb_get_file_size(fname, &file_size);

  if (rc == RDB_OK)
    rc = rdb_randfile_create(fname, &file, 1);

  if (rc == RDB_OK) {
    /* We use the default comparator, which may or may not match the
       comparator used in this database. However this should not cause
       problems since we only use Table operations that do not require
       any comparisons. In particular, we do not call Seek or Prev. */
    rc = rdb_table_open(rdb_dbopt_default, file, file_size, &table);
  }

  if (rc != RDB_OK) {
    if (table != NULL)
      rdb_table_destroy(table);

    if (file != NULL)
      rdb_rfile_destroy(file);

    return rc;
  }

  ro.fill_cache = 0;

  iter = rdb_tableiter_create(table, &ro);

  rdb_buffer_init(&r);

  for (rdb_iter_seek_first(iter); rdb_iter_valid(iter); rdb_iter_next(iter)) {
    rdb_slice_t key = rdb_iter_key(iter);
    rdb_slice_t val = rdb_iter_value(iter);
    rdb_pkey_t pkey;

    rdb_buffer_reset(&r);

    if (!rdb_pkey_import(&pkey, &key)) {
      rdb_buffer_string(&r, "badkey '");
      rdb_buffer_escape(&r, &key);
      rdb_buffer_string(&r, "' => '");
      rdb_buffer_escape(&r, &val);
      rdb_buffer_string(&r, "'\n");
      stream_append(dst, &r);
    } else {
      rdb_buffer_push(&r, '\'');
      rdb_buffer_escape(&r, &pkey.user_key);
      rdb_buffer_string(&r, "' @ ");
      rdb_buffer_number(&r, pkey.sequence);
      rdb_buffer_string(&r, " : ");

      if (pkey.type == RDB_TYPE_DELETION)
        rdb_buffer_string(&r, "del");
      else if (pkey.type == RDB_TYPE_VALUE)
        rdb_buffer_string(&r, "val");
      else
        rdb_buffer_number(&r, pkey.type);

      rdb_buffer_string(&r, " => '");
      rdb_buffer_escape(&r, &val);
      rdb_buffer_string(&r, "'\n");

      stream_append(dst, &r);
    }
  }

  rc = rdb_iter_status(iter);

  if (rc != RDB_OK) {
    rdb_buffer_reset(&r);
    rdb_buffer_string(&r, "iterator error: ");
    rdb_buffer_string(&r, rdb_strerror(rc));
    rdb_buffer_push(&r, '\n');

    stream_append(dst, &r);
  }

  rdb_buffer_clear(&r);

  rdb_iter_destroy(iter);
  rdb_table_destroy(table);
  rdb_rfile_destroy(file);

  return RDB_OK;
}

int
rdb_dump_file(const char *fname, FILE *dst) {
  rdb_filetype_t type;

  if (!guess_type(fname, &type))
    return RDB_INVALID; /* "[fname]: unknown file type" */

  rdb_env_init();

  switch (type) {
    case RDB_FILE_LOG:
      return dump_log(fname, dst);
    case RDB_FILE_DESC:
      return dump_descriptor(fname, dst);
    case RDB_FILE_TABLE:
      return dump_table(fname, dst);
    default:
      break;
  }

  return RDB_INVALID; /* "[fname]: not a dump-able file type" */
}
