/*!
 * db_impl.c - database implementation for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "table/iterator.h"
#include "table/merger.h"
#include "table/table.h"
#include "table/table_builder.h"

#include "util/array.h"
#include "util/atomic.h"
#include "util/bloom.h"
#include "util/buffer.h"
#include "util/cache.h"
#include "util/coding.h"
#include "util/comparator.h"
#include "util/env.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/port.h"
#include "util/rbt.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/thread_pool.h"
#include "util/vector.h"

#include "builder.h"
#include "db_impl.h"
#include "db_iter.h"
#include "dbformat.h"
#include "filename.h"
#include "log_format.h"
#include "log_reader.h"
#include "log_writer.h"
#include "memtable.h"
#include "snapshot.h"
#include "table_cache.h"
#include "version_edit.h"
#include "version_set.h"
#include "write_batch.h"

/*
 * DBImpl::ManualCompaction
 */

/* Information for a manual compaction. */
typedef struct rdb_manual_s {
  int level;
  int done;
  const rdb_ikey_t *begin; /* null means beginning of key range. */
  const rdb_ikey_t *end;   /* null means end of key range. */
  rdb_ikey_t tmp_storage;  /* Used to keep track of compaction progress. */
} rdb_manual_t;

static void
rdb_manual_init(rdb_manual_t *m, int level) {
  m->level = level;
  m->done = 0;
  m->begin = NULL;
  m->end = NULL;

  rdb_buffer_init(&m->tmp_storage);
}

static void
rdb_manual_clear(rdb_manual_t *m) {
  rdb_ikey_clear(&m->tmp_storage);
}

/*
 * DBImpl::CompactionStats
 */

/* Per level compaction stats. stats[level] stores the stats for
   compactions that produced data for the specified "level". */
typedef struct rdb_cstats_s {
  int64_t micros;
  int64_t bytes_read;
  int64_t bytes_written;
} rdb_cstats_t;

static void
rdb_cstats_init(rdb_cstats_t *c) {
  c->micros = 0;
  c->bytes_read = 0;
  c->bytes_written = 0;
}

static void
rdb_cstats_add(rdb_cstats_t *z, const rdb_cstats_t *x) {
  z->micros += x->micros;
  z->bytes_read += x->bytes_read;
  z->bytes_written += x->bytes_written;
}

/*
 * DBImpl::Writer
 */

/* Information kept for every waiting writer. */
typedef struct rdb_writer_s {
  int status;
  rdb_batch_t *batch;
  int sync;
  int done;
  rdb_cond_t cv;
  struct rdb_writer_s *next;
} rdb_writer_t;

static void
rdb_writer_init(rdb_writer_t *w) {
  w->status = 0;
  w->batch = NULL;
  w->sync = 0;
  w->done = 0;
  w->next = NULL;

  rdb_cond_init(&w->cv);
}

static void
rdb_writer_clear(rdb_writer_t *w) {
  rdb_cond_destroy(&w->cv);
}

/*
 * Writer Queue
 */

typedef struct rdb_queue_s {
  rdb_writer_t *head;
  rdb_writer_t *tail;
  int length;
} rdb_queue_t;

static void
rdb_queue_init(rdb_queue_t *queue) {
  queue->head = NULL;
  queue->tail = NULL;
  queue->length = 0;
}

static void
rdb_queue_push(rdb_queue_t *queue, rdb_writer_t *writer) {
  if (queue->head == NULL)
    queue->head = writer;

  if (queue->tail != NULL)
    queue->tail->next = writer;

  queue->tail = writer;
  queue->length++;
}

static rdb_writer_t *
rdb_queue_shift(rdb_queue_t *queue) {
  rdb_writer_t *writer = queue->head;

  if (writer == NULL)
    abort(); /* LCOV_EXCL_LINE */

  queue->head = writer->next;

  if (queue->head == NULL)
    queue->tail = NULL;

  queue->length--;

  writer->next = NULL;

  return writer;
}

/*
 * CompactionState::Output
 */

/* Files produced by compaction. */
typedef struct rdb_output_s {
  uint64_t number;
  uint64_t file_size;
  rdb_ikey_t smallest, largest;
} rdb_output_t;

static rdb_output_t *
rdb_output_create(uint64_t number) {
  rdb_output_t *out = rdb_malloc(sizeof(rdb_output_t));

  out->number = number;
  out->file_size = 0;

  rdb_buffer_init(&out->smallest);
  rdb_buffer_init(&out->largest);

  return out;
}

static void
rdb_output_destroy(rdb_output_t *out) {
  rdb_ikey_clear(&out->smallest);
  rdb_ikey_clear(&out->largest);
  rdb_free(out);
}

/*
 * CompactionState
 */

typedef struct rdb_cstate_s {
  rdb_compaction_t *compaction;

  /* Sequence numbers < smallest_snapshot are not significant since we
     will never have to service a snapshot below smallest_snapshot.
     Therefore if we have seen a sequence number S <= smallest_snapshot,
     we can drop all entries for the same key with sequence numbers < S. */
  rdb_seqnum_t smallest_snapshot;

  rdb_vector_t outputs; /* rdb_output_t */

  /* State kept for output being generated. */
  rdb_wfile_t *outfile;
  rdb_tablebuilder_t *builder;

  uint64_t total_bytes;
} rdb_cstate_t;

static rdb_cstate_t *
rdb_cstate_create(rdb_compaction_t *c) {
  rdb_cstate_t *state = rdb_malloc(sizeof(rdb_cstate_t));

  state->compaction = c;
  state->smallest_snapshot = 0;
  state->outfile = NULL;
  state->builder = NULL;
  state->total_bytes = 0;

  rdb_vector_init(&state->outputs);

  return state;
}

static void
rdb_cstate_destroy(rdb_cstate_t *state) {
  size_t i;

  for (i = 0; i < state->outputs.length; i++)
    rdb_output_destroy(state->outputs.items[i]);

  rdb_vector_clear(&state->outputs);
  rdb_free(state);
}

static rdb_output_t *
rdb_cstate_top(rdb_cstate_t *state) {
  return rdb_vector_top(&state->outputs);
}

/*
 * IterState
 */

typedef struct iter_state_s {
  rdb_mutex_t *mu;
  /* All guarded by mu. */
  rdb_version_t *version;
  rdb_memtable_t *mem;
  rdb_memtable_t *imm;
} iter_state_t;

static iter_state_t *
iter_state_create(rdb_mutex_t *mutex,
                  rdb_memtable_t *mem,
                  rdb_memtable_t *imm,
                  rdb_version_t *version) {
  iter_state_t *state = rdb_malloc(sizeof(iter_state_t));

  state->mu = mutex;
  state->version = version;
  state->mem = mem;
  state->imm = imm;

  return state;
}

static void
iter_state_destroy(iter_state_t *state) {
  rdb_mutex_lock(state->mu);

  rdb_memtable_unref(state->mem);

  if (state->imm != NULL)
    rdb_memtable_unref(state->imm);

  rdb_version_unref(state->version);

  rdb_mutex_unlock(state->mu);

  rdb_free(state);
}

/*
 * Helpers
 */

static const int non_table_cache_files = 10;

/* Fix user-supplied options to be reasonable. */
#define clip_to_range(val, min, max) do { \
  if ((val) > (max)) (val) = (max);       \
  if ((val) < (min)) (val) = (min);       \
} while (0)

rdb_dbopt_t
rdb_sanitize_options(const char *dbname,
                     const rdb_comparator_t *icmp,
                     const rdb_bloom_t *ipolicy,
                     const rdb_dbopt_t *src) {
  rdb_dbopt_t result = *src;

  (void)dbname;

  result.comparator = icmp;
  result.filter_policy = (src->filter_policy != NULL) ? ipolicy : NULL;

  clip_to_range(result.max_open_files, 64 + non_table_cache_files, 50000);
  clip_to_range(result.write_buffer_size, 64 << 10, 1 << 30);
  clip_to_range(result.max_file_size, 1 << 20, 1 << 30);
  clip_to_range(result.block_size, 1 << 10, 4 << 20);

  if (result.block_cache == NULL)
    result.block_cache = rdb_lru_create(8 << 20);

  return result;
}

static int
table_cache_size(const rdb_dbopt_t *sanitized_options) {
  /* Reserve ten files or so for other uses and give the rest to TableCache. */
  return sanitized_options->max_open_files - non_table_cache_files;
}

/*
 * DBImpl
 */

struct rdb_s {
  /* Constant after construction. */
  rdb_comparator_t user_comparator;
  rdb_bloom_t user_filter_policy;
  rdb_comparator_t internal_comparator;
  rdb_bloom_t internal_filter_policy;
  rdb_dbopt_t options; /* options.comparator == &internal_comparator */
  int owns_info_log;
  int owns_cache;
  char dbname[RDB_PATH_MAX];

  /* table_cache provides its own synchronization. */
  rdb_tcache_t *table_cache;

  /* Lock over the persistent DB state. Non-null iff successfully acquired. */
  rdb_filelock_t *db_lock;

  /* State below is protected by mutex. */
  rdb_mutex_t mutex;
  rdb_atomic(int) shutting_down;
  rdb_cond_t background_work_finished_signal;
  rdb_memtable_t *mem;
  rdb_memtable_t *imm; /* Memtable being compacted. */
  rdb_atomic(int) has_imm; /* So bg thread can detect non-null imm. */
  rdb_wfile_t *logfile;
  uint64_t logfile_number;
  rdb_logwriter_t *log;
  uint32_t seed; /* For sampling. */

  /* Queue of writers. */
  rdb_queue_t writers;
  rdb_batch_t *tmp_batch;

  rdb_snaplist_t snapshots;

  /* Set of table files to protect from deletion because they are
     part of ongoing compactions. */
  rb_tree_t pending_outputs; /* uint64_t */

  /* Thread pool. */
  rdb_pool_t *pool;

  /* Has a background compaction been scheduled or is running? */
  int background_compaction_scheduled;

  rdb_manual_t *manual_compaction;

  rdb_vset_t *versions;

  /* Have we encountered a background error in paranoid mode? */
  int bg_error;

  rdb_cstats_t stats[RDB_NUM_LEVELS];
};

static rdb_t *
rdb_create(const rdb_dbopt_t *options, const char *dbname) {
  rdb_t *db = rdb_malloc(sizeof(rdb_t));
  int i;

  if (!rdb_path_absolute(db->dbname, sizeof(db->dbname) - 32, dbname)) {
    rdb_free(db);
    return NULL;
  }

  rdb_env_init();

  if (options->comparator != NULL) {
    db->user_comparator = *options->comparator;
    rdb_ikc_init(&db->internal_comparator, &db->user_comparator);
  } else {
    rdb_ikc_init(&db->internal_comparator, rdb_bytewise_comparator);
  }

  if (options->filter_policy != NULL) {
    db->user_filter_policy = *options->filter_policy;
    rdb_ifp_init(&db->internal_filter_policy, &db->user_filter_policy);
  } else {
    rdb_ifp_init(&db->internal_filter_policy, NULL);
  }

  db->options = rdb_sanitize_options(db->dbname,
                                       &db->internal_comparator,
                                       &db->internal_filter_policy,
                                       options);

  db->owns_info_log = 0;

#if 0
  db->owns_info_log = (db->options.info_log != options->info_log);
#endif

  db->owns_cache = (db->options.block_cache != options->block_cache);

  /* db->dbname = dbname; */

  db->table_cache = rdb_tcache_create(db->dbname,
                                        &db->options,
                                        table_cache_size(&db->options));

  db->db_lock = NULL;

  rdb_mutex_init(&db->mutex);

  db->shutting_down = 0;

  rdb_cond_init(&db->background_work_finished_signal);

  db->mem = NULL;
  db->imm = NULL;
  db->has_imm = 0;
  db->logfile = NULL;
  db->logfile_number = 0;
  db->log = NULL;
  db->seed = 0;

  rdb_queue_init(&db->writers);

  db->tmp_batch = rdb_batch_create();

  rdb_snaplist_init(&db->snapshots);
  rb_set64_init(&db->pending_outputs);

  db->pool = rdb_pool_create(1);
  db->background_compaction_scheduled = 0;
  db->manual_compaction = NULL;

  db->versions = rdb_vset_create(db->dbname,
                                   &db->options,
                                   db->table_cache,
                                   &db->internal_comparator);

  db->bg_error = 0;

  for (i = 0; i < RDB_NUM_LEVELS; i++)
    rdb_cstats_init(&db->stats[i]);

  return db;
}

static void
rdb_destroy(rdb_t *db) {
  /* Wait for background work to finish. */
  rdb_mutex_lock(&db->mutex);

  rdb_atomic_store(&db->shutting_down, 1, rdb_order_release);

  while (db->background_compaction_scheduled)
    rdb_cond_wait(&db->background_work_finished_signal, &db->mutex);

  rdb_mutex_unlock(&db->mutex);

  rdb_pool_destroy(db->pool);

  if (db->db_lock != NULL)
    rdb_unlock_file(db->db_lock);

  rdb_vset_destroy(db->versions);

  if (db->mem != NULL)
    rdb_memtable_unref(db->mem);

  if (db->imm != NULL)
    rdb_memtable_unref(db->imm);

  rdb_batch_destroy(db->tmp_batch);

  if (db->log != NULL)
    rdb_logwriter_destroy(db->log);

  if (db->logfile != NULL)
    rdb_wfile_destroy(db->logfile);

  rdb_tcache_destroy(db->table_cache);

#if 0
  if (db->owns_info_log)
    rdb_infolog_destroy(db->options.info_log);
#endif

  if (db->owns_cache)
    rdb_lru_destroy(db->options.block_cache);

  /* Extra */
  assert(db->writers.length == 0);
  assert(rdb_snaplist_empty(&db->snapshots));

  rb_set64_clear(&db->pending_outputs);

  rdb_mutex_destroy(&db->mutex);
  rdb_cond_destroy(&db->background_work_finished_signal);
}

static const rdb_comparator_t *
rdb_user_comparator(const rdb_t *db) {
  return db->internal_comparator.user_comparator;
}

static int
rdb_new_db(rdb_t *db) {
  char manifest[RDB_PATH_MAX];
  rdb_vedit_t new_db;
  rdb_wfile_t *file;
  int rc;

  if (!rdb_desc_filename(manifest, sizeof(manifest), db->dbname, 1))
    return RDB_INVALID;

  rc = rdb_truncfile_create(manifest, &file);

  if (rc != RDB_OK)
    return rc;

  rdb_vedit_init(&new_db);
  rdb_vedit_set_comparator_name(&new_db, rdb_user_comparator(db)->name);
  rdb_vedit_set_log_number(&new_db, 0);
  rdb_vedit_set_next_file(&new_db, 2);
  rdb_vedit_set_last_sequence(&new_db, 0);

  {
    rdb_logwriter_t log;
    rdb_buffer_t record;

    rdb_logwriter_init(&log, file, 0);
    rdb_buffer_init(&record);

    rdb_vedit_export(&record, &new_db);

    rc = rdb_logwriter_add_record(&log, &record);

    if (rc == RDB_OK)
      rc = rdb_wfile_sync(file);

    if (rc == RDB_OK)
      rc = rdb_wfile_close(file);

    rdb_buffer_clear(&record);
  }

  rdb_wfile_destroy(file);

  if (rc == RDB_OK) {
    /* Make "CURRENT" file that points to the new manifest file. */
    rc = rdb_set_current_file(db->dbname, 1);
  } else {
    rdb_remove_file(manifest);
  }

  rdb_vedit_clear(&new_db);

  return rc;
}

static void
rdb_maybe_ignore_error(const rdb_t *db, int *status) {
  if (*status == RDB_OK || db->options.paranoid_checks)
    ; /* No change needed. */
  else
    *status = RDB_OK;
}

static void
rdb_remove_obsolete_files(rdb_t *db) {
  rb_tree_t live; /* uint64_t */
  char path[RDB_PATH_MAX];
  char **filenames = NULL;
  rdb_vector_t to_delete;
  rdb_filetype_t type;
  uint64_t number;
  int i, len;

  /* rdb_mutex_assert_held(&db->mutex); */

  if (db->bg_error != RDB_OK) {
    /* After a background error, we don't know whether a new version may
       or may not have been committed, so we cannot safely garbage collect. */
    return;
  }

  rb_set64_init(&live);
  rdb_vector_init(&to_delete);

  /* Make a set of all of the live files. */
  /* rb_tree_copy(&live, &db->pending_outputs); */
  rb_set64_iterate(&db->pending_outputs, number)
    rb_set64_put(&live, number);

  rdb_vset_add_live_files(db->versions, &live);

  len = rdb_get_children(db->dbname, &filenames); /* Ignoring errors on purpose. */

  for (i = 0; i < len; i++) {
    const char *filename = filenames[i];

    if (rdb_parse_filename(&type, &number, filename)) {
      int keep = 1;

      switch (type) {
        case RDB_FILE_LOG:
          keep = ((number >= rdb_vset_log_number(db->versions)) ||
                  (number == rdb_vset_prev_log_number(db->versions)));
          break;
        case RDB_FILE_DESC:
          /* Keep my manifest file, and any newer incarnations'
             (in case there is a race that allows other incarnations). */
          keep = (number >= rdb_vset_manifest_file_number(db->versions));
          break;
        case RDB_FILE_TABLE:
          keep = rb_set64_has(&live, number);
          break;
        case RDB_FILE_TEMP:
          /* Any temp files that are currently being written to must
             be recorded in pending_outputs, which is inserted into "live". */
          keep = rb_set64_has(&live, number);
          break;
        case RDB_FILE_CURRENT:
        case RDB_FILE_LOCK:
        case RDB_FILE_INFO:
          keep = 1;
          break;
      }

      if (!keep) {
        rdb_vector_push(&to_delete, filename);

        if (type == RDB_FILE_TABLE)
          rdb_tcache_evict(db->table_cache, number);
      }
    }
  }

  /* While deleting all files unblock other threads. All files being deleted
     have unique names which will not collide with newly created files and
     are therefore safe to delete while allowing other threads to proceed. */
  rdb_mutex_unlock(&db->mutex);

  for (i = 0; i < (int)to_delete.length; i++) {
    const char *filename = to_delete.items[i];

    if (!rdb_path_join(path, sizeof(path), db->dbname, filename, NULL))
      continue;

    rdb_remove_file(path);
  }

  rb_set64_clear(&live);
  rdb_vector_clear(&to_delete);

  if (filenames != NULL)
    rdb_free_children(filenames, len);

  rdb_mutex_lock(&db->mutex);
}

static int
compare_ascending(int64_t x, int64_t y) {
  return x < y ? -1 : 1;
}

static int
rdb_write_level0_table(rdb_t *db, rdb_memtable_t *mem,
                                  rdb_vedit_t *edit,
                                  rdb_version_t *base) {
  uint64_t start_micros;
  rdb_filemeta_t meta;
  rdb_cstats_t stats;
  rdb_iter_t *iter;
  int rc = RDB_OK;
  int level = 0;

  /* rdb_mutex_assert_held(&db->mutex); */

  rdb_filemeta_init(&meta);
  rdb_cstats_init(&stats);

  start_micros = rdb_now_usec();

  meta.number = rdb_vset_new_file_number(db->versions);

  rb_set64_put(&db->pending_outputs, meta.number);

  iter = rdb_memiter_create(mem);

  {
    rdb_mutex_unlock(&db->mutex);

    rc = rdb_build_table(db->dbname,
                         &db->options,
                         db->table_cache,
                         iter,
                         &meta);

    rdb_mutex_lock(&db->mutex);
  }

  rdb_iter_destroy(iter);

  rb_set64_del(&db->pending_outputs, meta.number);

  /* Note that if file_size is zero, the file has been deleted and
     should not be added to the manifest. */
  if (rc == RDB_OK && meta.file_size > 0) {
    rdb_slice_t min_user_key = rdb_ikey_user_key(&meta.smallest);
    rdb_slice_t max_user_key = rdb_ikey_user_key(&meta.largest);

    if (base != NULL) {
      level = rdb_version_pick_level_for_memtable_output(base,
                                                         &min_user_key,
                                                         &max_user_key);
    }

    rdb_vedit_add_file(edit, level,
                       meta.number,
                       meta.file_size,
                       &meta.smallest,
                       &meta.largest);
  }

  stats.micros = rdb_now_usec() - start_micros;
  stats.bytes_written = meta.file_size;

  rdb_cstats_add(&db->stats[level], &stats);

  rdb_filemeta_clear(&meta);

  return rc;
}

static void
report_corruption(rdb_reporter_t *report, size_t bytes, int status) {
  (void)bytes;

  if (report->status != NULL && *report->status == RDB_OK)
    *report->status = status;
}

static int
rdb_recover_log_file(rdb_t *db, uint64_t log_number,
                                int last_log,
                                int *save_manifest,
                                rdb_vedit_t *edit,
                                rdb_seqnum_t *max_sequence) {
  char fname[RDB_PATH_MAX];
  rdb_reporter_t reporter;
  rdb_rfile_t *file;
  int rc = RDB_OK;
  rdb_buffer_t buf;
  rdb_slice_t record;
  rdb_batch_t batch;
  int compactions = 0;
  rdb_memtable_t *mem = NULL;
  rdb_logreader_t reader;

  /* rdb_mutex_assert_held(&db->mutex); */

  /* Open the log file. */
  if (!rdb_log_filename(fname, sizeof(fname), db->dbname, log_number))
    return RDB_INVALID;

  rc = rdb_seqfile_create(fname, &file);

  if (rc != RDB_OK) {
    rdb_maybe_ignore_error(db, &rc);
    return rc;
  }

  /* Create the log reader. */
  reporter.fname = fname;
  reporter.status = (db->options.paranoid_checks ? &rc : NULL);
  reporter.corruption = report_corruption;

  /* We intentionally make the log reader do checksumming even if
     paranoid_checks==0 so that corruptions cause entire commits
     to be skipped instead of propagating bad information (like overly
     large sequence numbers). */
  rdb_logreader_init(&reader, file, &reporter, 1, 0);
  rdb_batch_init(&batch);
  rdb_buffer_init(&buf);

  /* Read all the records and add to a memtable. */
  while (rdb_logreader_read_record(&reader, &record, &buf) && rc == RDB_OK) {
    rdb_seqnum_t last_seq;

    if (record.size < 12) {
      reporter.corruption(&reporter, record.size, RDB_CORRUPTION); /* "log record too small" */
      continue;
    }

    rdb_batch_set_contents(&batch, &record);

    if (mem == NULL) {
      mem = rdb_memtable_create(&db->internal_comparator);
      rdb_memtable_ref(mem);
    }

    rc = rdb_batch_insert_into(&batch, mem);

    rdb_maybe_ignore_error(db, &rc);

    if (rc != RDB_OK)
      break;

    last_seq = rdb_batch_sequence(&batch) + rdb_batch_count(&batch) - 1;

    if (last_seq > *max_sequence)
      *max_sequence = last_seq;

    if (rdb_memtable_usage(mem) > db->options.write_buffer_size) {
      compactions++;
      *save_manifest = 1;

      rc = rdb_write_level0_table(db, mem, edit, NULL);

      rdb_memtable_unref(mem);
      mem = NULL;

      if (rc != RDB_OK) {
        /* Reflect errors immediately so that conditions like full
           file-systems cause the db_open() to fail. */
        break;
      }
    }
  }

  rdb_buffer_clear(&buf);
  rdb_batch_clear(&batch);
  rdb_logreader_clear(&reader);
  rdb_rfile_destroy(file);

  /* See if we should keep reusing the last log file. */
  if (rc == RDB_OK && db->options.reuse_logs && last_log && compactions == 0) {
    uint64_t lfile_size;

    assert(db->logfile == NULL);
    assert(db->log == NULL);
    assert(db->mem == NULL);

    if (rdb_get_file_size(fname, &lfile_size) == RDB_OK &&
        rdb_appendfile_create(fname, &db->logfile) == RDB_OK) {
      db->log = rdb_logwriter_create(db->logfile, lfile_size);
      db->logfile_number = log_number;

      if (mem != NULL) {
        db->mem = mem;
        mem = NULL;
      } else {
        /* mem can be NULL if lognum exists but was empty. */
        db->mem = rdb_memtable_create(&db->internal_comparator);
        rdb_memtable_ref(db->mem);
      }
    }
  }

  if (mem != NULL) {
    /* mem did not get reused; compact it. */
    if (rc == RDB_OK) {
      *save_manifest = 1;
      rc = rdb_write_level0_table(db, mem, edit, NULL);
    }

    rdb_memtable_unref(mem);
  }

  return rc;
}

static int
rdb_recover(rdb_t *db, rdb_vedit_t *edit, int *save_manifest) {
  uint64_t min_log, prev_log, number;
  rdb_seqnum_t max_sequence = 0;
  char path[RDB_PATH_MAX];
  char **filenames = NULL;
  rb_tree_t expected; /* uint64_t */
  rdb_filetype_t type;
  rdb_array_t logs;
  int rc = RDB_OK;
  int i, len;

  /* rdb_mutex_assert_held(&db->mutex); */

  /* Ignore error from CreateDir since the creation of the DB is
     committed only when the descriptor is created, and this directory
     may already exist from a previous failed creation attempt. */
  rdb_create_dir(db->dbname);

  assert(db->db_lock == NULL);

  if (!rdb_lock_filename(path, sizeof(path), db->dbname))
    return RDB_INVALID;

  rc = rdb_lock_file(path, &db->db_lock);

  if (rc != RDB_OK)
    return rc;

  if (!rdb_current_filename(path, sizeof(path), db->dbname))
    return RDB_INVALID;

  if (!rdb_file_exists(path)) {
    if (db->options.create_if_missing) {
      rc = rdb_new_db(db);

      if (rc != RDB_OK)
        return rc;
    } else {
      return RDB_INVALID; /* "does not exist (create_if_missing is false)" */
    }
  } else {
    if (db->options.error_if_exists)
      return RDB_INVALID; /* "exists (error_if_exists is true)" */
  }

  rc = rdb_vset_recover(db->versions, save_manifest);

  if (rc != RDB_OK)
    return rc;

  /* Recover from all newer log files than the ones named in the
   * descriptor (new log files may have been added by the previous
   * incarnation without registering them in the descriptor).
   *
   * Note that prev_log_number() is no longer used, but we pay
   * attention to it in case we are recovering a database
   * produced by an older version of leveldb.
   */
  min_log = rdb_vset_log_number(db->versions);
  prev_log = rdb_vset_prev_log_number(db->versions);

  len = rdb_get_children(db->dbname, &filenames);

  if (len < 0)
    return RDB_IOERR;

  rb_set64_init(&expected);
  rdb_array_init(&logs);

  rdb_vset_add_live_files(db->versions, &expected);

  for (i = 0; i < len; i++) {
    if (rdb_parse_filename(&type, &number, filenames[i])) {
      rb_set64_del(&expected, number);

      if (type == RDB_FILE_LOG && ((number >= min_log) || (number == prev_log)))
        rdb_array_push(&logs, number);
    }
  }

  rdb_free_children(filenames, len);

  if (expected.size != 0) {
    rc = RDB_CORRUPTION; /* "[expected.size] missing files" */
    goto fail;
  }

  /* Recover in the order in which the logs were generated. */
  rdb_array_sort(&logs, compare_ascending);

  for (i = 0; i < (int)logs.length; i++) {
    rc = rdb_recover_log_file(db,
                                   logs.items[i],
                                   (i == (int)logs.length - 1),
                                   save_manifest,
                                   edit,
                                   &max_sequence);

    if (rc != RDB_OK)
      goto fail;

    /* The previous incarnation may not have written any MANIFEST
       records after allocating this log number. So we manually
       update the file number allocation counter in VersionSet. */
    rdb_vset_mark_file_number_used(db->versions, logs.items[i]);
  }

  if (rdb_vset_last_sequence(db->versions) < max_sequence)
    rdb_vset_set_last_sequence(db->versions, max_sequence);

  rc = RDB_OK;
fail:
  rb_set64_clear(&expected);
  rdb_array_clear(&logs);
  return rc;
}

static void
rdb_record_background_error(rdb_t *db, int status) {
  /* rdb_mutex_assert_held(&db->mutex); */

  if (db->bg_error == RDB_OK) {
    db->bg_error = status;

    rdb_cond_broadcast(&db->background_work_finished_signal);
  }
}

static void
rdb_compact_memtable(rdb_t *db) {
  rdb_version_t *base;
  rdb_vedit_t edit;
  int rc = RDB_OK;

  rdb_vedit_init(&edit);

  /* rdb_mutex_assert_held(&db->mutex); */

  assert(db->imm != NULL);

  /* Save the contents of the memtable as a new Table. */
  base = rdb_vset_current(db->versions);

  rdb_version_ref(base);

  rc = rdb_write_level0_table(db, db->imm, &edit, base);

  rdb_version_unref(base);

  if (rc == RDB_OK && rdb_atomic_load(&db->shutting_down, rdb_order_acquire))
    rc = RDB_IOERR; /* "Deleting DB during memtable compaction" */

  /* Replace immutable memtable with the generated Table. */
  if (rc == RDB_OK) {
    rdb_vedit_set_prev_log_number(&edit, 0);
    rdb_vedit_set_log_number(&edit, db->logfile_number); /* Earlier logs no longer needed. */
    rc = rdb_vset_log_and_apply(db->versions, &edit, &db->mutex);
  }

  if (rc == RDB_OK) {
    /* Commit to the new state. */
    rdb_memtable_unref(db->imm);
    db->imm = NULL;
    rdb_atomic_store(&db->has_imm, 0, rdb_order_release);
    rdb_remove_obsolete_files(db);
  } else {
    rdb_record_background_error(db, rc);
  }

  rdb_vedit_clear(&edit);
}

static int
rdb_open_compaction_output_file(rdb_t *db, rdb_cstate_t *compact) {
  char fname[RDB_PATH_MAX];
  uint64_t file_number;
  int rc = RDB_OK;

  assert(compact != NULL);
  assert(compact->builder == NULL);

  {
    rdb_mutex_lock(&db->mutex);

    file_number = rdb_vset_new_file_number(db->versions);

    rb_set64_put(&db->pending_outputs, file_number);

    rdb_vector_push(&compact->outputs, rdb_output_create(file_number));

    rdb_mutex_unlock(&db->mutex);
  }

  /* Make the output file. */
  if (rdb_table_filename(fname, sizeof(fname), db->dbname, file_number)) {
    rc = rdb_truncfile_create(fname, &compact->outfile);

    if (rc == RDB_OK) {
      compact->builder = rdb_tablebuilder_create(&db->options,
                                                 compact->outfile);
    }
  } else {
    rc = RDB_INVALID;
  }

  return rc;
}

static int
rdb_finish_compaction_output_file(rdb_t *db, rdb_cstate_t *compact,
                                             rdb_iter_t *input) {
  uint64_t output_number, current_entries, current_bytes;
  int rc = RDB_OK;

  assert(compact != NULL);
  assert(compact->outfile != NULL);
  assert(compact->builder != NULL);

  output_number = rdb_cstate_top(compact)->number;

  assert(output_number != 0);

  /* Check for iterator errors. */
  rc = rdb_iter_status(input);

  current_entries = rdb_tablebuilder_num_entries(compact->builder);

  if (rc == RDB_OK)
    rc = rdb_tablebuilder_finish(compact->builder);
  else
    rdb_tablebuilder_abandon(compact->builder);

  current_bytes = rdb_tablebuilder_file_size(compact->builder);

  rdb_cstate_top(compact)->file_size = current_bytes;

  compact->total_bytes += current_bytes;

  rdb_tablebuilder_destroy(compact->builder);
  compact->builder = NULL;

  /* Finish and check for file errors. */
  if (rc == RDB_OK)
    rc = rdb_wfile_sync(compact->outfile);

  if (rc == RDB_OK)
    rc = rdb_wfile_close(compact->outfile);

  rdb_wfile_destroy(compact->outfile);
  compact->outfile = NULL;

  if (rc == RDB_OK && current_entries > 0) {
    /* Verify that the table is usable. */
    rdb_iter_t *iter = rdb_tcache_iterate(db->table_cache,
                                          rdb_readopt_default,
                                          output_number,
                                          current_bytes,
                                          NULL);

    rc = rdb_iter_status(iter);

    rdb_iter_destroy(iter);
  }

  return rc;
}

static int
rdb_install_compaction_results(rdb_t *db, rdb_cstate_t *compact) {
  rdb_vedit_t *edit = rdb_compaction_edit(compact->compaction);
  int level;
  size_t i;

  /* rdb_mutex_assert_held(&db->mutex); */

  /* Add compaction outputs. */
  rdb_compaction_add_input_deletions(compact->compaction, edit);

  level = rdb_compaction_level(compact->compaction);

  for (i = 0; i < compact->outputs.length; i++) {
    const rdb_output_t *out = compact->outputs.items[i];

    rdb_vedit_add_file(edit, level + 1,
                       out->number,
                       out->file_size,
                       &out->smallest,
                       &out->largest);
  }

  return rdb_vset_log_and_apply(db->versions, edit, &db->mutex);
}

static int
rdb_do_compaction_work(rdb_t *db, rdb_cstate_t *compact) {
  const rdb_comparator_t *ucmp = rdb_user_comparator(db);
  rdb_seqnum_t last_sequence_for_key = RDB_MAX_SEQUENCE;
  uint64_t start_micros = rdb_now_usec();
  int64_t imm_micros = 0; /* Micros spent doing db->imm compactions. */
  rdb_buffer_t user_key;
  int has_user_key = 0;
  rdb_cstats_t stats;
  rdb_iter_t *input;
  int rc = RDB_OK;
  int which, level;
  rdb_pkey_t ikey;
  size_t i;

  rdb_buffer_init(&user_key);
  rdb_cstats_init(&stats);

  assert(rdb_vset_num_level_files(db->versions,
                                  rdb_compaction_level(compact->compaction))
                                  > 0);

  assert(compact->builder == NULL);
  assert(compact->outfile == NULL);

  if (rdb_snaplist_empty(&db->snapshots)) {
    compact->smallest_snapshot = rdb_vset_last_sequence(db->versions);
  } else {
    compact->smallest_snapshot =
      rdb_snaplist_oldest(&db->snapshots)->sequence;
  }

  input = rdb_inputiter_create(db->versions, compact->compaction);

  /* Release mutex while we're actually doing the compaction work. */
  rdb_mutex_unlock(&db->mutex);

  rdb_iter_seek_first(input);

  while (rdb_iter_valid(input) &&
        !rdb_atomic_load(&db->shutting_down, rdb_order_acquire)) {
    rdb_slice_t key, value;
    int drop = 0;

    /* Prioritize immutable compaction work. */
    if (rdb_atomic_load(&db->has_imm, rdb_order_relaxed)) {
      uint64_t imm_start = rdb_now_usec();

      rdb_mutex_lock(&db->mutex);

      if (db->imm != NULL) {
        rdb_compact_memtable(db);

        /* Wake up make_room_for_write() if necessary. */
        rdb_cond_broadcast(&db->background_work_finished_signal);
      }

      rdb_mutex_unlock(&db->mutex);

      imm_micros += (rdb_now_usec() - imm_start);
    }

    key = rdb_iter_key(input);

    if (rdb_compaction_should_stop_before(compact->compaction, &key) &&
        compact->builder != NULL) {
      rc = rdb_finish_compaction_output_file(db, compact, input);

      if (rc != RDB_OK)
        break;
    }

    /* Handle key/value, add to state, etc. */
    if (!rdb_pkey_import(&ikey, &key)) {
      /* Do not hide error keys. */
      rdb_buffer_reset(&user_key);
      has_user_key = 0;
      last_sequence_for_key = RDB_MAX_SEQUENCE;
    } else {
      if (!has_user_key || rdb_compare(ucmp, &ikey.user_key, &user_key) != 0) {
        /* First occurrence of this user key. */
        rdb_buffer_set(&user_key, ikey.user_key.data, ikey.user_key.size);
        has_user_key = 1;
        last_sequence_for_key = RDB_MAX_SEQUENCE;
      }

      if (last_sequence_for_key <= compact->smallest_snapshot) {
        /* Hidden by an newer entry for same user key. */
        drop = 1;  /* (A) */
      } else if (ikey.type == RDB_TYPE_DELETION &&
                 ikey.sequence <= compact->smallest_snapshot &&
                 rdb_compaction_is_base_level_for_key(compact->compaction,
                                                      &ikey.user_key)) {
        /* For this user key:
         *
         * (1) there is no data in higher levels
         * (2) data in lower levels will have larger sequence numbers
         * (3) data in layers that are being compacted here and have
         *     smaller sequence numbers will be dropped in the next
         *     few iterations of this loop (by rule (A) above).
         *
         * Therefore this deletion marker is obsolete and can be dropped.
         */
        drop = 1;
      }

      last_sequence_for_key = ikey.sequence;
    }

    if (!drop) {
      /* Open output file if necessary. */
      if (compact->builder == NULL) {
        rc = rdb_open_compaction_output_file(db, compact);

        if (rc != RDB_OK)
          break;
      }

      if (rdb_tablebuilder_num_entries(compact->builder) == 0)
        rdb_ikey_copy(&rdb_cstate_top(compact)->smallest, &key);

      rdb_ikey_copy(&rdb_cstate_top(compact)->largest, &key);

      value = rdb_iter_value(input);

      rdb_tablebuilder_add(compact->builder, &key, &value);

      /* Close output file if it is big enough. */
      if (rdb_tablebuilder_file_size(compact->builder) >=
          rdb_compaction_max_output_file_size(compact->compaction)) {
        rc = rdb_finish_compaction_output_file(db, compact, input);

        if (rc != RDB_OK)
          break;
      }
    }

    rdb_iter_next(input);
  }

  if (rc == RDB_OK && rdb_atomic_load(&db->shutting_down, rdb_order_acquire))
    rc = RDB_IOERR; /* "Deleting DB during compaction" */

  if (rc == RDB_OK && compact->builder != NULL)
    rc = rdb_finish_compaction_output_file(db, compact, input);

  if (rc == RDB_OK)
    rc = rdb_iter_status(input);

  rdb_iter_destroy(input);
  input = NULL;

  stats.micros = rdb_now_usec() - start_micros - imm_micros;

  for (which = 0; which < 2; which++) {
    size_t len = rdb_compaction_num_input_files(compact->compaction, which);

    for (i = 0; i < len; i++) {
      rdb_filemeta_t *f = rdb_compaction_input(compact->compaction, which, i);

      stats.bytes_read += f->file_size;
    }
  }

  for (i = 0; i < compact->outputs.length; i++) {
    rdb_output_t *out = compact->outputs.items[i];

    stats.bytes_written += out->file_size;
  }

  rdb_mutex_lock(&db->mutex);

  level = rdb_compaction_level(compact->compaction);

  rdb_cstats_add(&db->stats[level + 1], &stats);

  if (rc == RDB_OK)
    rc = rdb_install_compaction_results(db, compact);

  if (rc != RDB_OK)
    rdb_record_background_error(db, rc);

  rdb_buffer_clear(&user_key);

  return rc;
}

static void
rdb_cleanup_compaction(rdb_t *db, rdb_cstate_t *compact) {
  size_t i;

  /* rdb_mutex_assert_held(&db->mutex); */

  if (compact->builder != NULL) {
    /* May happen if we get a shutdown call in the middle of compaction. */
    rdb_tablebuilder_abandon(compact->builder);
    rdb_tablebuilder_destroy(compact->builder);
  } else {
    assert(compact->outfile == NULL);
  }

  if (compact->outfile != NULL)
    rdb_wfile_destroy(compact->outfile);

  for (i = 0; i < compact->outputs.length; i++) {
    const rdb_output_t *out = compact->outputs.items[i];

    rb_set64_del(&db->pending_outputs, out->number);
  }

  rdb_cstate_destroy(compact);
}

static void
rdb_background_compaction(rdb_t *db) {
  int is_manual = (db->manual_compaction != NULL);
  rdb_slice_t manual_end;
  rdb_compaction_t *c;
  int rc = RDB_OK;

  /* rdb_mutex_assert_held(&db->mutex); */

  if (db->imm != NULL) {
    rdb_compact_memtable(db);
    return;
  }

  if (is_manual) {
    rdb_manual_t *m = db->manual_compaction;

    c = rdb_vset_compact_range(db->versions, m->level, m->begin, m->end);

    m->done = (c == NULL);

    if (c != NULL) {
      int num = rdb_compaction_num_input_files(c, 0);

      /* XXX Can this be a slice? */
      /* Maybe place directly on tmp_storage. */
      manual_end = rdb_compaction_input(c, 0, num - 1)->largest;
    }
  } else {
    c = rdb_vset_pick_compaction(db->versions);
  }

  if (c == NULL) {
    /* Nothing to do. */
  } else if (!is_manual && rdb_compaction_is_trivial_move(c)) {
    /* Move file to next level. */
    rdb_vedit_t *edit = rdb_compaction_edit(c);
    rdb_filemeta_t *f;

    assert(rdb_compaction_num_input_files(c, 0) == 1);

    f = rdb_compaction_input(c, 0, 0);

    rdb_vedit_remove_file(edit, rdb_compaction_level(c), f->number);

    rdb_vedit_add_file(edit, rdb_compaction_level(c) + 1,
                             f->number,
                             f->file_size,
                             &f->smallest,
                             &f->largest);

    rc = rdb_vset_log_and_apply(db->versions, edit, &db->mutex);

    if (rc != RDB_OK)
      rdb_record_background_error(db, rc);
  } else {
    rdb_cstate_t *compact = rdb_cstate_create(c);

    rc = rdb_do_compaction_work(db, compact);

    if (rc != RDB_OK)
      rdb_record_background_error(db, rc);

    rdb_cleanup_compaction(db, compact);

    rdb_compaction_release_inputs(c);

    rdb_remove_obsolete_files(db);
  }

  rdb_compaction_destroy(c);

  if (rc == RDB_OK) {
    /* Done. */
  } else if (rdb_atomic_load(&db->shutting_down, rdb_order_acquire)) {
    /* Ignore compaction errors found during shutting down. */
  } else {
    /* Log compaction error. */
  }

  if (is_manual) {
    rdb_manual_t *m = db->manual_compaction;

    if (rc != RDB_OK)
      m->done = 1;

    if (!m->done) {
      /* We only compacted part of the requested range. Update *m
         to the range that is left to be compacted. */
      rdb_buffer_copy(&m->tmp_storage, &manual_end);
      m->begin = &m->tmp_storage;
    }

    db->manual_compaction = NULL;
  }
}

static void
rdb_background_call(void *db);

static void
rdb_maybe_schedule_compaction(rdb_t *db) {
  /* rdb_mutex_assert_held(&db->mutex); */

  if (db->background_compaction_scheduled) {
    /* Already scheduled. */
  } else if (rdb_atomic_load(&db->shutting_down, rdb_order_acquire)) {
    /* DB is being deleted; no more background compactions. */
  } else if (db->bg_error != RDB_OK) {
    /* Already got an error; no more changes. */
  } else if (db->imm == NULL && db->manual_compaction == NULL &&
             !rdb_vset_needs_compaction(db->versions)) {
    /* No work to be done. */
  } else {
    db->background_compaction_scheduled = 1;
    rdb_pool_schedule(db->pool, &rdb_background_call, db);
  }
}

static void
rdb_background_call(void *ptr) {
  rdb_t *db = ptr;

  rdb_mutex_lock(&db->mutex);

  assert(db->background_compaction_scheduled);

  if (rdb_atomic_load(&db->shutting_down, rdb_order_acquire)) {
    /* No more background work when shutting down. */
  } else if (db->bg_error != RDB_OK) {
    /* No more background work after a background error. */
  } else {
    rdb_background_compaction(db);
  }

  db->background_compaction_scheduled = 0;

  /* Previous compaction may have produced too many files in a level,
     so reschedule another compaction if needed. */
  rdb_maybe_schedule_compaction(db);

  rdb_cond_broadcast(&db->background_work_finished_signal);

  rdb_mutex_unlock(&db->mutex);
}

static void
cleanup_iter_state(void *arg1, void *arg2) {
  iter_state_destroy((iter_state_t *)arg1);
  (void)arg2;
}

static rdb_iter_t *
rdb_internal_iterator(rdb_t *db, const rdb_readopt_t *options,
                                 rdb_seqnum_t *latest_snapshot,
                                 uint32_t *seed) {
  rdb_iter_t *internal_iter;
  rdb_version_t *current;
  iter_state_t *cleanup;
  rdb_vector_t list;

  rdb_vector_init(&list);

  rdb_mutex_lock(&db->mutex);

  *latest_snapshot = rdb_vset_last_sequence(db->versions);

  /* Collect together all needed child iterators. */
  rdb_vector_push(&list, rdb_memiter_create(db->mem));

  rdb_memtable_ref(db->mem);

  if (db->imm != NULL) {
    rdb_vector_push(&list, rdb_memiter_create(db->imm));
    rdb_memtable_ref(db->imm);
  }

  current = rdb_vset_current(db->versions);

  rdb_version_add_iterators(current, options, &list);

  internal_iter = rdb_mergeiter_create(&db->internal_comparator,
                                       (rdb_iter_t **)list.items,
                                       list.length);

  rdb_version_ref(current);

  cleanup = iter_state_create(&db->mutex, db->mem, db->imm,
                              rdb_vset_current(db->versions));

  rdb_iter_register_cleanup(internal_iter, cleanup_iter_state, cleanup, NULL);

  *seed = ++db->seed;

  rdb_mutex_unlock(&db->mutex);

  rdb_vector_clear(&list);

  return internal_iter;
}

/* REQUIRES: Writer list must be non-empty. */
/* REQUIRES: First writer must have a non-null batch. */
static rdb_batch_t *
rdb_build_batch_group(rdb_t *db, rdb_writer_t **last_writer) {
  rdb_writer_t *first = db->writers.head;
  rdb_batch_t *result = first->batch;
  size_t size, max_size;
  rdb_writer_t *w;

  /* rdb_mutex_assert_held(&db->mutex); */

  assert(first != NULL);

  result = first->batch;

  assert(result != NULL);

  size = rdb_batch_size(first->batch);

  /* Allow the group to grow up to a maximum size, but if the
     original write is small, limit the growth so we do not slow
     down the small write too much. */
  max_size = 1 << 20;

  if (size <= (128 << 10))
    max_size = size + (128 << 10);

  *last_writer = first;

  /* Advance past "first". */
  for (w = first->next; w != NULL; w = w->next) {
    if (w->sync && !first->sync) {
      /* Do not include a sync write into a
         batch handled by a non-sync write. */
      break;
    }

    if (w->batch != NULL) {
      size += rdb_batch_size(w->batch);

      if (size > max_size) {
        /* Do not make batch too big. */
        break;
      }

      /* Append to *result. */
      if (result == first->batch) {
        /* Switch to temporary batch instead of disturbing caller's batch. */
        result = db->tmp_batch;

        assert(rdb_batch_count(result) == 0);

        rdb_batch_append(result, first->batch);
      }

      rdb_batch_append(result, w->batch);
    }

    *last_writer = w;
  }

  return result;
}

/* REQUIRES: db->mutex is held. */
/* REQUIRES: this thread is currently at the front of the writer queue. */
static int
rdb_make_room_for_write(rdb_t *db, int force) {
  size_t write_buffer_size = db->options.write_buffer_size;
  char fname[RDB_PATH_MAX];
  int allow_delay = !force;
  int rc = RDB_OK;

  /* rdb_mutex_assert_held(&db->mutex); */

  assert(db->writers.length > 0);

  for (;;) {
#define L0_FILES rdb_vset_num_level_files(db->versions, 0)
    if (db->bg_error != RDB_OK) {
      /* Yield previous error. */
      rc = db->bg_error;
      break;
    } else if (allow_delay && L0_FILES >= RDB_L0_SLOWDOWN_WRITES_TRIGGER) {
      /* We are getting close to hitting a hard limit on the number of
         L0 files. Rather than delaying a single write by several
         seconds when we hit the hard limit, start delaying each
         individual write by 1ms to reduce latency variance.  Also,
         this delay hands over some CPU to the compaction thread in
         case it is sharing the same core as the writer. */
      rdb_mutex_unlock(&db->mutex);
      rdb_sleep_usec(1000);
      allow_delay = 0;  /* Do not delay a single write more than once. */
      rdb_mutex_lock(&db->mutex);
    } else if (!force && rdb_memtable_usage(db->mem) <= write_buffer_size) {
      /* There is room in current memtable. */
      break;
    } else if (db->imm != NULL) {
      /* We have filled up the current memtable, but the previous
         one is still being compacted, so we wait. */
      rdb_cond_wait(&db->background_work_finished_signal, &db->mutex);
    } else if (L0_FILES >= RDB_L0_STOP_WRITES_TRIGGER) {
      /* There are too many level-0 files. */
      rdb_cond_wait(&db->background_work_finished_signal, &db->mutex);
    } else {
      rdb_wfile_t *lfile = NULL;
      uint64_t new_log_number;

      /* Attempt to switch to a new memtable and trigger compaction of old. */
      assert(rdb_vset_prev_log_number(db->versions) == 0);

      new_log_number = rdb_vset_new_file_number(db->versions);

      if (!rdb_log_filename(fname, sizeof(fname), db->dbname, new_log_number))
        abort(); /* LCOV_EXCL_LINE */

      rc = rdb_truncfile_create(fname, &lfile);

      if (rc != RDB_OK) {
        /* Avoid chewing through file number space in a tight loop. */
        rdb_vset_reuse_file_number(db->versions, new_log_number);
        break;
      }

      if (db->log != NULL)
        rdb_logwriter_destroy(db->log);

      if (db->logfile != NULL)
        rdb_wfile_destroy(db->logfile);

      db->logfile = lfile;
      db->logfile_number = new_log_number;
      db->log = rdb_logwriter_create(lfile, 0);
      db->imm = db->mem;

      rdb_atomic_store(&db->has_imm, 1, rdb_order_release);

      db->mem = rdb_memtable_create(&db->internal_comparator);

      rdb_memtable_ref(db->mem);

      force = 0; /* Do not force another compaction if have room. */
      rdb_maybe_schedule_compaction(db);
    }
#undef L0_FILES
  }

  return rc;
}

/*
 * API
 */

int
rdb_open(const char *dbname, const rdb_dbopt_t *options, rdb_t **dbptr) {
  const rdb_dbopt_t *opt = options ? options : rdb_dbopt_default;
  rdb_t *db = rdb_create(opt, dbname);
  int save_manifest;
  rdb_vedit_t edit;
  int rc = RDB_OK;

  *dbptr = NULL;

  if (db == NULL)
    return RDB_INVALID;

  rdb_vedit_init(&edit);
  rdb_mutex_lock(&db->mutex);

  /* Recover handles create_if_missing, error_if_exists. */
  save_manifest = 0;
  rc = rdb_recover(db, &edit, &save_manifest);

  if (rc == RDB_OK && db->mem == NULL) {
    /* Create new log and a corresponding memtable. */
    uint64_t new_log_number = rdb_vset_new_file_number(db->versions);
    char fname[RDB_PATH_MAX];
    rdb_wfile_t *lfile;

    if (!rdb_log_filename(fname, sizeof(fname), db->dbname, new_log_number))
      abort(); /* LCOV_EXCL_LINE */

    rc = rdb_truncfile_create(fname, &lfile);

    if (rc == RDB_OK) {
      rdb_vedit_set_log_number(&edit, new_log_number);

      db->logfile = lfile;
      db->logfile_number = new_log_number;
      db->log = rdb_logwriter_create(lfile, 0);
      db->mem = rdb_memtable_create(&db->internal_comparator);

      rdb_memtable_ref(db->mem);
    }
  }

  if (rc == RDB_OK && save_manifest) {
    rdb_vedit_set_prev_log_number(&edit, 0); /* No older logs needed
                                                after recovery. */
    rdb_vedit_set_log_number(&edit, db->logfile_number);

    rc = rdb_vset_log_and_apply(db->versions, &edit, &db->mutex);
  }

  if (rc == RDB_OK) {
    rdb_remove_obsolete_files(db);
    rdb_maybe_schedule_compaction(db);
  }

  rdb_mutex_unlock(&db->mutex);

  if (rc == RDB_OK) {
    assert(db->mem != NULL);
    *dbptr = db;
  } else {
    rdb_destroy(db);
  }

  rdb_vedit_clear(&edit);

  return rc;
}

void
rdb_close(rdb_t *db) {
  rdb_destroy(db);
}

int
rdb_get(rdb_t *db, const rdb_slice_t *key,
                   rdb_slice_t *value,
                   const rdb_readopt_t *options) {
  rdb_memtable_t *mem, *imm;
  rdb_version_t *current;
  rdb_seqnum_t snapshot;
  int have_stat_update = 0;
  rdb_getstats_t stats;
  int rc = RDB_OK;

  if (value != NULL)
    rdb_buffer_init(value);

  if (options == NULL)
    options = rdb_readopt_default;

  rdb_mutex_lock(&db->mutex);

  if (options->snapshot != NULL)
    snapshot = options->snapshot->sequence;
  else
    snapshot = rdb_vset_last_sequence(db->versions);

  mem = db->mem;
  imm = db->imm;
  current = rdb_vset_current(db->versions);

  rdb_memtable_ref(mem);

  if (imm != NULL)
    rdb_memtable_ref(imm);

  rdb_version_ref(current);

  /* Unlock while reading from files and memtables. */
  {
    rdb_lkey_t lkey;

    rdb_mutex_unlock(&db->mutex);

    /* First look in the memtable, then in the immutable memtable (if any). */
    rdb_lkey_init(&lkey, key, snapshot);

    if (rdb_memtable_get(mem, &lkey, value, &rc)) {
      /* Done. */
    } else if (imm != NULL && rdb_memtable_get(imm, &lkey, value, &rc)) {
      /* Done. */
    } else {
      rc = rdb_version_get(current, options, &lkey, value, &stats);
      have_stat_update = 1;
    }

    rdb_lkey_clear(&lkey);

    rdb_mutex_lock(&db->mutex);
  }

  if (have_stat_update && rdb_version_update_stats(current, &stats))
    rdb_maybe_schedule_compaction(db);

  rdb_memtable_unref(mem);

  if (imm != NULL)
    rdb_memtable_unref(imm);

  rdb_version_unref(current);

  rdb_mutex_unlock(&db->mutex);

  if (value != NULL) {
    if (rc == RDB_OK) {
      if (value->alloc == 0)
        rdb_buffer_grow(value, 1);
    } else {
      rdb_buffer_clear(value);
    }
  }

  return rc;
}

int
rdb_has(rdb_t *db, const rdb_slice_t *key, const rdb_readopt_t *options) {
  return rdb_get(db, key, NULL, options);
}

int
rdb_put(rdb_t *db, const rdb_slice_t *key,
                   const rdb_slice_t *value,
                   const rdb_writeopt_t *options) {
  rdb_batch_t batch;
  int rc;

  rdb_batch_init(&batch);
  rdb_batch_put(&batch, key, value);

  rc = rdb_write(db, &batch, options);

  rdb_batch_clear(&batch);

  return rc;
}

int
rdb_del(rdb_t *db, const rdb_slice_t *key, const rdb_writeopt_t *options) {
  rdb_batch_t batch;
  int rc;

  rdb_batch_init(&batch);
  rdb_batch_del(&batch, key);

  rc = rdb_write(db, &batch, options);

  rdb_batch_clear(&batch);

  return rc;
}

int
rdb_write(rdb_t *db, rdb_batch_t *updates, const rdb_writeopt_t *options) {
  rdb_writer_t *last_writer;
  uint64_t last_sequence;
  rdb_writer_t w;
  int rc;

  if (options == NULL)
    options = rdb_writeopt_default;

  rdb_writer_init(&w);

  w.batch = updates;
  w.sync = options->sync;
  w.done = 0;

  rdb_mutex_lock(&db->mutex);

  rdb_queue_push(&db->writers, &w);

  while (!w.done && &w != db->writers.head)
    rdb_cond_wait(&w.cv, &db->mutex);

  if (w.done) {
    rdb_mutex_unlock(&db->mutex);
    rdb_writer_clear(&w);
    return w.status;
  }

  /* May temporarily unlock and wait. */
  rc = rdb_make_room_for_write(db, updates == NULL);
  last_sequence = rdb_vset_last_sequence(db->versions);
  last_writer = &w;

  if (rc == RDB_OK && updates != NULL) {  /* NULL batch is for compactions. */
    rdb_batch_t *write_batch = rdb_build_batch_group(db, &last_writer);

    rdb_batch_set_sequence(write_batch, last_sequence + 1);

    last_sequence += rdb_batch_count(write_batch);

    /* Add to log and apply to memtable.  We can release the lock
       during this phase since &w is currently responsible for logging
       and protects against concurrent loggers and concurrent writes
       into db->mem. */
    {
      rdb_slice_t contents;
      int sync_error = 0;

      rdb_mutex_unlock(&db->mutex);

      contents = rdb_batch_contents(write_batch);
      rc = rdb_logwriter_add_record(db->log, &contents);

      if (rc == RDB_OK && options->sync) {
        rc = rdb_wfile_sync(db->logfile);

        if (rc != RDB_OK)
          sync_error = 1;
      }

      if (rc == RDB_OK)
        rc = rdb_batch_insert_into(write_batch, db->mem);

      rdb_mutex_lock(&db->mutex);

      if (sync_error) {
        /* The state of the log file is indeterminate: the log record we
           just added may or may not show up when the DB is re-opened.
           So we force the DB into a mode where all future writes fail. */
        rdb_record_background_error(db, rc);
      }
    }

    if (write_batch == db->tmp_batch)
      rdb_batch_reset(db->tmp_batch);

    rdb_vset_set_last_sequence(db->versions, last_sequence);
  }

  for (;;) {
    rdb_writer_t *ready = rdb_queue_shift(&db->writers);

    if (ready != &w) {
      ready->status = rc;
      ready->done = 1;
      rdb_cond_signal(&ready->cv);
    }

    if (ready == last_writer)
      break;
  }

  /* Notify new head of write queue. */
  if (db->writers.length > 0)
    rdb_cond_signal(&db->writers.head->cv);

  rdb_mutex_unlock(&db->mutex);
  rdb_writer_clear(&w);

  return rc;
}

const rdb_snapshot_t *
rdb_get_snapshot(rdb_t *db) {
  rdb_snapshot_t *snap;
  rdb_seqnum_t seq;

  rdb_mutex_lock(&db->mutex);

  seq = rdb_vset_last_sequence(db->versions);
  snap = rdb_snaplist_new(&db->snapshots, seq);

  rdb_mutex_unlock(&db->mutex);

  return snap;
}

void
rdb_release_snapshot(rdb_t *db, const rdb_snapshot_t *snapshot) {
  rdb_mutex_lock(&db->mutex);

  rdb_snaplist_delete(&db->snapshots, snapshot);

  rdb_mutex_unlock(&db->mutex);
}

rdb_iter_t *
rdb_iterator(rdb_t *db, const rdb_readopt_t *options) {
  const rdb_comparator_t *ucmp = rdb_user_comparator(db);
  rdb_seqnum_t latest_snapshot;
  rdb_iter_t *iter;
  uint32_t seed;

  if (options == NULL)
    options = rdb_iteropt_default;

  iter = rdb_internal_iterator(db, options, &latest_snapshot, &seed);

  return rdb_dbiter_create(db, ucmp, iter,
                           (options->snapshot != NULL
                              ? options->snapshot->sequence
                              : latest_snapshot),
                           seed);
}

int
rdb_get_property(rdb_t *db, const char *property, char **value) {
  (void)db;
  (void)property;
  (void)value;
  return 0;
}

void
rdb_get_approximate_sizes(rdb_t *db, const rdb_range_t *range,
                                     size_t length,
                                     uint64_t *sizes) {
  uint64_t start, limit;
  rdb_ikey_t k1, k2;
  rdb_version_t *v;
  size_t i;

  rdb_mutex_lock(&db->mutex);

  v = rdb_vset_current(db->versions);

  rdb_version_ref(v);

  for (i = 0; i < length; i++) {
    /* Convert user_key into a corresponding internal key. */
    rdb_ikey_init(&k1, &range[i].start, RDB_MAX_SEQUENCE, RDB_VALTYPE_SEEK);
    rdb_ikey_init(&k2, &range[i].limit, RDB_MAX_SEQUENCE, RDB_VALTYPE_SEEK);

    start = rdb_vset_approximate_offset_of(db->versions, v, &k1);
    limit = rdb_vset_approximate_offset_of(db->versions, v, &k2);

    sizes[i] = (limit >= start ? limit - start : 0);

    rdb_ikey_clear(&k1);
    rdb_ikey_clear(&k2);
  }

  rdb_version_unref(v);

  rdb_mutex_unlock(&db->mutex);
}

void
rdb_compact_range(rdb_t *db, const rdb_slice_t *begin, const rdb_slice_t *end) {
  int max_level_with_files = 1;
  int level;

  {
    rdb_version_t *base;

    rdb_mutex_lock(&db->mutex);

    base = rdb_vset_current(db->versions);

    for (level = 1; level < RDB_NUM_LEVELS; level++) {
      if (rdb_version_overlap_in_level(base, level, begin, end))
        max_level_with_files = level;
    }

    rdb_mutex_unlock(&db->mutex);
  }

  rdb_test_compact_memtable(db);

  for (level = 0; level < max_level_with_files; level++)
    rdb_test_compact_range(db, level, begin, end);
}

/*
 * Static
 */

int
rdb_repair_db(const char *dbname, const rdb_dbopt_t *options) {
  (void)dbname;
  (void)options;
  return 0;
}

int
rdb_destroy_db(const char *dbname, const rdb_dbopt_t *options) {
  char lockname[RDB_PATH_MAX];
  char path[RDB_PATH_MAX];
  rdb_filelock_t *lock;
  char **files = NULL;
  int rc = RDB_OK;
  int len;

  (void)options;

  if (!rdb_lock_filename(lockname, sizeof(lockname), dbname))
    return RDB_INVALID;

  rdb_env_init();

  len = rdb_get_children(dbname, &files);

  if (len < 0) {
    /* Ignore error in case directory does not exist. */
    return RDB_OK;
  }

  rc = rdb_lock_file(lockname, &lock);

  if (rc == RDB_OK) {
    rdb_filetype_t type;
    uint64_t number;
    int i, status;

    for (i = 0; i < len; i++) {
      const char *name = files[i];

      if (!rdb_parse_filename(&type, &number, name))
        continue;

      if (type == RDB_FILE_LOCK)
        continue; /* Lock file will be deleted at end. */

      if (!rdb_path_join(path, sizeof(path), dbname, name, NULL)) {
        rc = RDB_INVALID;
        continue;
      }

      status = rdb_remove_file(path);

      if (rc == RDB_OK && status != RDB_OK)
        rc = status;
    }

    rdb_unlock_file(lock); /* Ignore error since state is already gone. */
    rdb_remove_file(lockname);
    rdb_remove_dir(dbname); /* Ignore error in case dir contains other files. */
  }

  rdb_free_children(files, len);

  return rc;
}

/*
 * Testing
 */

int
rdb_test_compact_memtable(rdb_t *db) {
  /* NULL batch means just wait for earlier writes to be done. */
  int rc = rdb_write(db, NULL, rdb_writeopt_default);

  if (rc == RDB_OK) {
    /* Wait until the compaction completes. */
    rdb_mutex_lock(&db->mutex);

    while (db->imm != NULL && db->bg_error == RDB_OK)
      rdb_cond_wait(&db->background_work_finished_signal, &db->mutex);

    if (db->imm != NULL)
      rc = db->bg_error;

    rdb_mutex_unlock(&db->mutex);
  }

  return rc;
}

void
rdb_test_compact_range(rdb_t *db, int level,
                                  const rdb_slice_t *begin,
                                  const rdb_slice_t *end) {
  rdb_ikey_t begin_storage, end_storage;
  rdb_manual_t manual;

  assert(level >= 0);
  assert(level + 1 < RDB_NUM_LEVELS);

  rdb_manual_init(&manual, level);

  if (begin == NULL) {
    manual.begin = NULL;
  } else {
    rdb_ikey_init(&begin_storage, begin, RDB_MAX_SEQUENCE, RDB_VALTYPE_SEEK);
    manual.begin = &begin_storage;
  }

  if (end == NULL) {
    manual.end = NULL;
  } else {
    rdb_ikey_init(&end_storage, end, 0, (rdb_valtype_t)0);
    manual.end = &end_storage;
  }

  rdb_mutex_lock(&db->mutex);

  while (!manual.done
      && !rdb_atomic_load(&db->shutting_down, rdb_order_acquire)
      && db->bg_error == RDB_OK) {
    if (db->manual_compaction == NULL) { /* Idle. */
      db->manual_compaction = &manual;
      rdb_maybe_schedule_compaction(db);
    } else { /* Running either my compaction or another compaction. */
      rdb_cond_wait(&db->background_work_finished_signal, &db->mutex);
    }
  }

  if (db->manual_compaction == &manual) {
    /* Cancel my manual compaction since we aborted early for some reason. */
    db->manual_compaction = NULL;
  }

  rdb_mutex_unlock(&db->mutex);

  if (begin != NULL)
    rdb_ikey_clear(&begin_storage);

  if (end != NULL)
    rdb_ikey_clear(&end_storage);

  rdb_manual_clear(&manual);
}

rdb_iter_t *
rdb_test_internal_iterator(rdb_t *db) {
  rdb_seqnum_t ignored;
  uint32_t ignored_seed;

  return rdb_internal_iterator(db, rdb_readopt_default,
                                   &ignored,
                                   &ignored_seed);
}

int64_t
rdb_test_max_next_level_overlapping_bytes(rdb_t *db) {
  int64_t result;
  rdb_mutex_lock(&db->mutex);
  result = rdb_vset_max_next_level_overlapping_bytes(db->versions);
  rdb_mutex_unlock(&db->mutex);
  return result;
}

/*
 * Internal
 */

void
rdb_record_read_sample(rdb_t *db, const rdb_slice_t *key) {
  rdb_version_t *current;

  rdb_mutex_lock(&db->mutex);

  current = rdb_vset_current(db->versions);

  if (rdb_version_record_read_sample(current, key))
    rdb_maybe_schedule_compaction(db);

  rdb_mutex_unlock(&db->mutex);
}
