/*!
 * db_bench_lmdb.c - lmdb benchmarks for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on google/leveldb:
 *   Copyright (c) 2011, The LevelDB Authors. All rights reserved.
 *   https://github.com/google/leveldb
 *
 * See LICENSE for more information.
 */

#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <lmdb.h>

#include "util/buffer.h"
#include "util/env.h"
#include "util/internal.h"
#include "util/random.h"
#include "util/strutil.h"
#include "util/testutil.h"

#include "histogram.h"

/* Comma-separated list of operations to run in the specified order
 *   Actual benchmarks:
 *      fillseq       -- write N values in sequential key order in async mode
 *      fillrandom    -- write N values in random key order in async mode
 *      overwrite     -- overwrite N values in random key order in async mode
 *      fillsync      -- write N/100 values in random key order in sync mode
 *      fill100K      -- write N/1000 100K values in random order in async mode
 *      deleteseq     -- delete N keys in sequential order
 *      deleterandom  -- delete N keys in random order
 *      readseq       -- read N times sequentially
 *      readreverse   -- read N times in reverse order
 *      readrandom    -- read N times in random order
 *      readmissing   -- read N missing keys in random order
 *      readhot       -- read N times in random order from 1% section of DB
 *      seekrandom    -- N random seeks
 *      seekordered   -- N ordered seeks
 *      open          -- cost of opening a DB
 *   Meta operations:
 *      stat       -- Print env stat
 *      info       -- Print env info
 */
static const char *FLAGS_benchmarks =
    "fillseq,"
    "fillsync,"
    "fillrandom,"
    "overwrite,"
    "readrandom,"
    "readseq,"
    "readreverse,"
    "fill100K,"
    "deleterandom,"
    "overwrite,"
    "deleterandom,"
    "overwrite,"
    "readrandom,"
    "readseq,";

/* Number of key/values to place in database. */
static int FLAGS_num = 1000000;

/* Number of read operations to do. If negative, do FLAGS_num reads. */
static int FLAGS_reads = -1;

/* Size of each value */
static int FLAGS_value_size = 100;

/* Arrange to generate values that shrink to this fraction of
   their original size after compression */
static double FLAGS_compression_ratio = 0.5;

/* Print histogram of operation timings */
static int FLAGS_histogram = 0;

/* Common key prefix length. */
static int FLAGS_key_prefix = 0;

/* If true, do not destroy the existing database. If you set this
   flag and also specify a benchmark that wants a fresh database, that
   benchmark will fail. */
static int FLAGS_use_existing_db = 0;

/* If true, enable MDB_WRITEMAP. */
static int FLAGS_write_map = 0;

/* If false, enable MDB_NOMETASYNC */
static int FLAGS_meta_sync = 0;

/* If false, enable MDB_NOSYNC. */
static int FLAGS_sync = 1;

/* If true, enable MDB_MAPASYNC. */
static int FLAGS_map_async = 0;

/* If false, enable MDB_NOTLS. */
static int FLAGS_tls = 1;

/* If false, enable MDB_NOLOCK. */
static int FLAGS_locks = 1;

/* If false, enable MDB_NORDAHEAD. */
static int FLAGS_readahead = 1;

/* The size of the map. */
static size_t FLAGS_map_size = (256 << 20);

/* Use the db with the following name. */
static const char *FLAGS_db = NULL;

/*
 * Helpers
 */

static void
error_check(int rc, const char *type) {
  if (rc != MDB_SUCCESS) {
    fprintf(stderr, "%s error: %s\n", type, mdb_strerror(rc));
    exit(1);
  }
}

/*
 * RandomGenerator
 */

/* Helper for quickly generating random data. */
typedef struct rng_s {
  ldb_buffer_t data;
  int pos;
} rng_t;

static void
rng_init(rng_t *rng) {
  ldb_buffer_t piece;
  ldb_rand_t rnd;

  ldb_buffer_init(&rng->data);

  /* We use a limited amount of data over and over again and ensure
     that it is larger than the compression window (32KB), and also
     large enough to serve all typical value sizes we want to write. */
  ldb_rand_init(&rnd, 301);
  ldb_buffer_init(&piece);

  while (rng->data.size < 1048576) {
    /* Add a short fragment that is as compressible as specified
       by FLAGS_compression_ratio. */
    ldb_compressible_string(&piece, &rnd, FLAGS_compression_ratio, 100);
    ldb_buffer_concat(&rng->data, &piece);
  }

  ldb_buffer_clear(&piece);

  rng->pos = 0;
}

static void
rng_clear(rng_t *rng) {
  ldb_buffer_clear(&rng->data);
}

static MDB_val
rng_generate(rng_t *rng, size_t len) {
  MDB_val mval;

  if (rng->pos + len > rng->data.size) {
    rng->pos = 0;
    assert(len < rng->data.size);
  }

  rng->pos += len;

  mval.mv_data = rng->data.data + rng->pos - len;
  mval.mv_size = len;

  return mval;
}

/*
 * KeyBuffer
 */

static MDB_val
key_encode(int k, char *buffer) {
  MDB_val mkey;

  memset(buffer, 'a', FLAGS_key_prefix);
  sprintf(buffer + FLAGS_key_prefix, "%016d", k);

  mkey.mv_data = buffer;
  mkey.mv_size = FLAGS_key_prefix + 16;

  return mkey;
}

/*
 * String Helpers
 */

#ifdef __linux__
static char *
trim_space(char *xp) {
  size_t xn;

  while (*xp && *xp <= ' ')
    xp++;

  xn = strlen(xp);

  while (xn > 0 && xp[xn - 1] <= ' ')
    xn--;

  xp[xn] = '\0';

  return xp;
}
#endif

static void
append_with_space(ldb_buffer_t *z, const ldb_slice_t *x) {
  if (x->size == 0)
    return;

  if (z->size > 0)
    ldb_buffer_push(z, ' ');

  ldb_buffer_concat(z, x);
}

/*
 * Stats
 */

typedef struct stats_s {
  double start;
  double finish;
  double seconds;
  int done;
  int next_report;
  int64_t bytes;
  double last_op_finish;
  histogram_t hist;
  ldb_buffer_t message;
} stats_t;

static void
stats_start(stats_t *st) {
  int64_t now = ldb_now_usec();

  st->start = now;
  st->finish = now;
  st->seconds = 0;
  st->done = 0;
  st->next_report = 100;
  st->bytes = 0;
  st->last_op_finish = now;

  histogram_init(&st->hist);
  ldb_buffer_reset(&st->message);
}

static void
stats_init(stats_t *st) {
  ldb_buffer_init(&st->message);
  stats_start(st);
}

static void
stats_clear(stats_t *st) {
  ldb_buffer_clear(&st->message);
}

static void
stats_stop(stats_t *st) {
  st->finish = ldb_now_usec();
  st->seconds = (st->finish - st->start) * 1e-6;
}

static void
stats_add_message(stats_t *st, const char *str) {
  ldb_slice_t msg = ldb_string(str);

  append_with_space(&st->message, &msg);
}

static void
stats_finished_single_op(stats_t *st) {
  if (FLAGS_histogram) {
    double now = ldb_now_usec();
    double micros = now - st->last_op_finish;

    histogram_add(&st->hist, micros);

    if (micros > 20000) {
      fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
      fflush(stderr);
    }

    st->last_op_finish = now;
  }

  st->done++;

  if (st->done >= st->next_report) {
    if (st->next_report < 1000)
      st->next_report += 100;
    else if (st->next_report < 5000)
      st->next_report += 500;
    else if (st->next_report < 10000)
      st->next_report += 1000;
    else if (st->next_report < 50000)
      st->next_report += 5000;
    else if (st->next_report < 100000)
      st->next_report += 10000;
    else if (st->next_report < 500000)
      st->next_report += 50000;
    else
      st->next_report += 100000;

    fprintf(stderr, "... finished %d ops%30s\r", st->done, "");
    fflush(stderr);
  }
}

static void
stats_add_bytes(stats_t *st, int64_t n) {
  st->bytes += n;
}

static void
stats_report(stats_t *st, const char *name) {
  ldb_buffer_t extra;

  /* Pretend at least one op was done in case we are running
     a benchmark that does not call finished_single_op(). */
  if (st->done < 1)
    st->done = 1;

  ldb_buffer_init(&extra);

  if (st->bytes > 0) {
    /* Rate is computed on actual elapsed time,
       not the sum of per-thread elapsed times. */
    double elapsed = (st->finish - st->start) * 1e-6;
    char rate[100];

    sprintf(rate, "%6.1f MB/s", (st->bytes / 1048576.0) / elapsed);

    ldb_buffer_set_str(&extra, rate);
  }

  append_with_space(&extra, &st->message);

  ldb_buffer_push(&extra, '\0');

  fprintf(stdout, "%-12s : %11.3f micros/op;%s%s\n", name,
                  st->seconds * 1e6 / st->done,
                  (extra.size == 0 ? "" : " "),
                  (char *)extra.data);

  ldb_buffer_clear(&extra);

  if (FLAGS_histogram) {
    char buf[MAX_HISTOGRAM];
    fprintf(stdout, "Microseconds per op:\n%s\n",
                    histogram_string(&st->hist, buf));
  }

  fflush(stdout);
}

/*
 * BenchState
 */

typedef struct bench_state_s {
  ldb_rand_t rnd;
  stats_t stats;
} bench_state_t;

static void
bench_state_init(bench_state_t *state, int seed) {
  ldb_rand_init(&state->rnd, seed);
  stats_init(&state->stats);
}

static void
bench_state_clear(bench_state_t *state) {
  stats_clear(&state->stats);
}

/*
 * Benchmark
 */

typedef struct bench_s {
  MDB_env *env;
  MDB_dbi db;
  int num;
  int val_size;
  int entries_per_batch;
  int reads;
  int total_bench_count;
  int sync_writes;
} bench_t;

static void
bench_init(bench_t *bench) {
  bench->env = NULL;
  bench->db = 0;
  bench->num = FLAGS_num;
  bench->val_size = FLAGS_value_size;
  bench->entries_per_batch = 1;
  bench->reads = FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads;
  bench->total_bench_count = 0;
  bench->sync_writes = 0;

  if (!FLAGS_use_existing_db)
    ldb_remove_file(FLAGS_db);
}

static void
bench_clear(bench_t *bench) {
  if (bench->env != NULL) {
    mdb_dbi_close(bench->env, bench->db);
    mdb_env_close(bench->env);
  }
}

static void
bench_open(bench_t *bench) {
  unsigned int flags = MDB_NOSUBDIR;
  MDB_txn *txn = NULL;
  int rc;

  assert(bench->env == NULL);
  assert(bench->db == 0);

  if (FLAGS_write_map)
    flags |= MDB_WRITEMAP;

  if (!FLAGS_meta_sync)
    flags |= MDB_NOMETASYNC;

  if (!FLAGS_sync)
    flags |= MDB_NOSYNC;

  if (FLAGS_write_map && FLAGS_map_async)
    flags |= MDB_MAPASYNC;

  if (!FLAGS_tls)
    flags |= MDB_NOTLS;

  if (!FLAGS_locks)
    flags |= MDB_NOLOCK;

  if (!FLAGS_readahead)
    flags |= MDB_NORDAHEAD;

  rc = mdb_env_create(&bench->env);

  if (rc == MDB_SUCCESS)
    rc = mdb_env_set_mapsize(bench->env, FLAGS_map_size);

  if (rc == MDB_SUCCESS)
    rc = mdb_env_open(bench->env, FLAGS_db, flags, 0644);

  if (rc == MDB_SUCCESS)
    rc = mdb_txn_begin(bench->env, NULL, 0, &txn);

  if (rc == MDB_SUCCESS)
    rc = mdb_dbi_open(txn, NULL, 0, &bench->db);

  if (rc == MDB_SUCCESS) {
    rc = mdb_txn_commit(txn);
    txn = NULL;
  }

  if (rc != MDB_SUCCESS) {
    fprintf(stderr, "open error: %s\n", mdb_strerror(rc));

    if (txn != NULL)
      mdb_txn_abort(txn);

    if (bench->db != 0)
      mdb_dbi_close(bench->env, bench->db);

    if (bench->env != NULL)
      mdb_env_close(bench->env);

    exit(1);
  }
}

static void
bench_print_environment(void) {
#ifdef __linux__
  time_t now = time(NULL);
  FILE *cpuinfo;
#endif

  fprintf(stderr, "Database:   %s\n", MDB_VERSION_STRING);

#ifdef __linux__
  fprintf(stderr, "Date:       %s", ctime(&now)); /* ctime() adds newline. */

  cpuinfo = fopen("/proc/cpuinfo", "r");

  if (cpuinfo != NULL) {
    char line[1000];
    int num_cpus = 0;
    char cpu_type[1000];
    char cache_size[1000];

    strcpy(cpu_type, "None");
    strcpy(cache_size, "None");

    while (fgets(line, sizeof(line), cpuinfo) != NULL) {
      char *sep = strchr(line, ':');
      char *key, *val;

      if (sep == NULL)
        continue;

      *sep = '\0';

      key = trim_space(line);
      val = trim_space(sep + 1);

      if (strcmp(key, "model name") == 0) {
        ++num_cpus;
        strcpy(cpu_type, val);
      } else if (strcmp(key, "cache size") == 0) {
        strcpy(cache_size, val);
      }
    }

    fclose(cpuinfo);

    fprintf(stderr, "CPU:        %d * %s\n", num_cpus, cpu_type);
    fprintf(stderr, "CPUCache:   %s\n", cache_size);
  }
#endif
}

static void
bench_print_warnings(void) {
#if defined(__GNUC__) && !defined(__OPTIMIZE__)
  fprintf(stdout,
    "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
  fprintf(stdout,
    "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif
}

static void
bench_print_header(bench_t *bench) {
  const int key_size = 16 + FLAGS_key_prefix;

  bench_print_environment();

  fprintf(stdout, "Keys:       %d bytes each\n", key_size);
  fprintf(stdout, "Values:     %d bytes each\n", FLAGS_value_size);
  fprintf(stdout, "Entries:    %d\n", bench->num);
  fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
          ((int64_t)(key_size + FLAGS_value_size) * bench->num) / 1048576.0);

  bench_print_warnings();

  fprintf(stdout, "------------------------------------------------\n");
}

static void
run_benchmark(bench_t *bench, const char *name,
              void (*method)(bench_t *, bench_state_t *)) {
  bench_state_t state;

  ++bench->total_bench_count;

  bench_state_init(&state, 1000 + bench->total_bench_count);

  stats_start(&state.stats);

  method(bench, &state);

  stats_stop(&state.stats);
  stats_report(&state.stats, name);

  bench_state_clear(&state);
}

static void
bench_open_db(bench_t *bench, bench_state_t *state) {
  int i;

  for (i = 0; i < bench->num; i++) {
    if (bench->env != NULL) {
      mdb_dbi_close(bench->env, bench->db);
      mdb_env_close(bench->env);
    }

    bench->env = NULL;
    bench->db = 0;

    bench_open(bench);

    stats_finished_single_op(&state->stats);
  }
}

static void
bench_do_write(bench_t *bench, bench_state_t *state, int seq) {
  int rc = MDB_SUCCESS;
  int64_t bytes = 0;
  char buffer[1024];
  MDB_txn *txn;
  rng_t gen;
  int i, j;

  rng_init(&gen);

  if (bench->num != FLAGS_num) {
    char msg[100];
    sprintf(msg, "(%d ops)", bench->num);
    stats_add_message(&state->stats, msg);
  }

  for (i = 0; i < bench->num; i += bench->entries_per_batch) {
    rc = mdb_txn_begin(bench->env, NULL, 0, &txn);
    error_check(rc, "txn");

    for (j = 0; j < bench->entries_per_batch; j++) {
      int k = seq ? i + j : (int)ldb_rand_uniform(&state->rnd, FLAGS_num);
      MDB_val key = key_encode(k, buffer);
      MDB_val val = rng_generate(&gen, bench->val_size);

      rc = mdb_put(txn, bench->db, &key, &val, 0);
      error_check(rc, "put");

      bytes += key.mv_size + val.mv_size;

      stats_finished_single_op(&state->stats);
    }

    rc = mdb_txn_commit(txn);
    error_check(rc, "commit");

    if (bench->sync_writes) {
      rc = mdb_env_sync(bench->env, 1);
      error_check(rc, "sync");
    }
  }

  stats_add_bytes(&state->stats, bytes);

  rng_clear(&gen);
}

static void
bench_write_sequential(bench_t *bench, bench_state_t *state) {
  bench_do_write(bench, state, 1);
}

static void
bench_write_random(bench_t *bench, bench_state_t *state) {
  bench_do_write(bench, state, 0);
}

static void
bench_read_sequential(bench_t *bench, bench_state_t *state) {
  int rc = MDB_SUCCESS;
  int64_t bytes = 0;
  MDB_val key, val;
  MDB_cursor *cur;
  MDB_txn *txn;
  int i = 0;

  rc = mdb_txn_begin(bench->env, NULL, MDB_RDONLY, &txn);
  error_check(rc, "txn");

  rc = mdb_cursor_open(txn, bench->db, &cur);
  error_check(rc, "cursor");

  rc = mdb_cursor_get(cur, &key, &val, MDB_FIRST);

  while (rc == MDB_SUCCESS && i < bench->reads) {
    bytes += key.mv_size + val.mv_size;

    stats_finished_single_op(&state->stats);

    i += 1;
    rc = mdb_cursor_get(cur, &key, &val, MDB_NEXT);
  }

  if (rc != MDB_NOTFOUND)
    error_check(rc, "cursor_get");

  mdb_cursor_close(cur);
  mdb_txn_abort(txn);

  stats_add_bytes(&state->stats, bytes);
}

static void
bench_read_reverse(bench_t *bench, bench_state_t *state) {
  int rc = MDB_SUCCESS;
  int64_t bytes = 0;
  MDB_val key, val;
  MDB_cursor *cur;
  MDB_txn *txn;
  int i = 0;

  rc = mdb_txn_begin(bench->env, NULL, MDB_RDONLY, &txn);
  error_check(rc, "txn");

  rc = mdb_cursor_open(txn, bench->db, &cur);
  error_check(rc, "cursor");

  rc = mdb_cursor_get(cur, &key, &val, MDB_LAST);

  while (rc == MDB_SUCCESS && i < bench->reads) {
    bytes += key.mv_size + val.mv_size;

    stats_finished_single_op(&state->stats);

    i += 1;
    rc = mdb_cursor_get(cur, &key, &val, MDB_PREV);
  }

  if (rc != MDB_NOTFOUND)
    error_check(rc, "cursor_get");

  mdb_cursor_close(cur);
  mdb_txn_abort(txn);

  stats_add_bytes(&state->stats, bytes);
}

static void
bench_read_random(bench_t *bench, bench_state_t *state) {
  char buffer[1024], msg[100];
  int rc = MDB_SUCCESS;
  int found = 0;
  MDB_txn *txn;
  int i;

  rc = mdb_txn_begin(bench->env, NULL, MDB_RDONLY, &txn);
  error_check(rc, "txn");

  for (i = 0; i < bench->reads; i++) {
    const int k = ldb_rand_uniform(&state->rnd, FLAGS_num);
    MDB_val key = key_encode(k, buffer);
    MDB_val val;

    rc = mdb_get(txn, bench->db, &key, &val);

    if (rc != MDB_NOTFOUND)
      error_check(rc, "get");

    if (rc == MDB_SUCCESS)
      found++;

    stats_finished_single_op(&state->stats);
  }

  mdb_txn_abort(txn);

  sprintf(msg, "(%d of %d found)", found, bench->num);
  stats_add_message(&state->stats, msg);
}

static void
bench_read_missing(bench_t *bench, bench_state_t *state) {
  int rc = MDB_SUCCESS;
  char buffer[1024];
  MDB_txn *txn;
  int i;

  rc = mdb_txn_begin(bench->env, NULL, MDB_RDONLY, &txn);
  error_check(rc, "txn");

  for (i = 0; i < bench->reads; i++) {
    const int k = ldb_rand_uniform(&state->rnd, FLAGS_num);
    MDB_val key = key_encode(k, buffer);
    MDB_val val;

    key.mv_size -= 1;

    rc = mdb_get(txn, bench->db, &key, &val);

    if (rc != MDB_NOTFOUND)
      error_check(rc, "get");

    stats_finished_single_op(&state->stats);
  }

  mdb_txn_abort(txn);
}

static void
bench_read_hot(bench_t *bench, bench_state_t *state) {
  const int range = (FLAGS_num + 99) / 100;
  int rc = MDB_SUCCESS;
  char buffer[1024];
  MDB_txn *txn;
  int i;

  rc = mdb_txn_begin(bench->env, NULL, MDB_RDONLY, &txn);
  error_check(rc, "txn");

  for (i = 0; i < bench->reads; i++) {
    const int k = ldb_rand_uniform(&state->rnd, range);
    MDB_val key = key_encode(k, buffer);
    MDB_val val;

    rc = mdb_get(txn, bench->db, &key, &val);

    if (rc != MDB_NOTFOUND)
      error_check(rc, "get");

    stats_finished_single_op(&state->stats);
  }

  mdb_txn_abort(txn);
}

static void
bench_seek_random(bench_t *bench, bench_state_t *state) {
  char buffer[1024], msg[100];
  int rc = MDB_SUCCESS;
  MDB_txn *txn = NULL;
  int found = 0;
  int i;

  rc = mdb_txn_begin(bench->env, NULL, MDB_RDONLY, &txn);
  error_check(rc, "txn");

  for (i = 0; i < bench->reads; i++) {
    MDB_cursor *cur = NULL;
    const int k = ldb_rand_uniform(&state->rnd, FLAGS_num);
    MDB_val key = key_encode(k, buffer);
    MDB_val val;

    rc = mdb_cursor_open(txn, bench->db, &cur);
    error_check(rc, "cursor");

    rc = mdb_cursor_get(cur, &key, &val, MDB_SET);

    if (rc != MDB_NOTFOUND)
      error_check(rc, "get");

    if (rc == MDB_SUCCESS)
      found++;

    mdb_cursor_close(cur);

    stats_finished_single_op(&state->stats);
  }

  mdb_txn_abort(txn);

  sprintf(msg, "(%d of %d found)", found, bench->num);
  stats_add_message(&state->stats, msg);
}

static void
bench_seek_ordered(bench_t *bench, bench_state_t *state) {
  char buffer[1024], msg[100];
  MDB_cursor *cur = NULL;
  MDB_txn *txn = NULL;
  int rc = MDB_SUCCESS;
  int found = 0;
  int last = 0;
  int i;

  rc = mdb_txn_begin(bench->env, NULL, MDB_RDONLY, &txn);
  error_check(rc, "txn");

  rc = mdb_cursor_open(txn, bench->db, &cur);
  error_check(rc, "cursor");

  for (i = 0; i < bench->reads; i++) {
    const int k = (last + ldb_rand_uniform(&state->rnd, 100)) % FLAGS_num;
    MDB_val key = key_encode(k, buffer);
    MDB_val val;

    rc = mdb_cursor_get(cur, &key, &val, MDB_SET);

    if (rc != MDB_NOTFOUND)
      error_check(rc, "get");

    if (rc == MDB_SUCCESS)
      found++;

    stats_finished_single_op(&state->stats);
    last = k;
  }

  mdb_cursor_close(cur);
  mdb_txn_abort(txn);

  sprintf(msg, "(%d of %d found)", found, bench->num);
  stats_add_message(&state->stats, msg);
}

static void
bench_do_delete(bench_t *bench, bench_state_t *state, int seq) {
  int rc = MDB_SUCCESS;
  MDB_txn *txn = NULL;
  char buffer[1024];
  int i, j;

  for (i = 0; i < bench->num; i += bench->entries_per_batch) {
    rc = mdb_txn_begin(bench->env, NULL, 0, &txn);
    error_check(rc, "txn");

    for (j = 0; j < bench->entries_per_batch; j++) {
      int k = seq ? i + j : (int)ldb_rand_uniform(&state->rnd, FLAGS_num);
      MDB_val key = key_encode(k, buffer);

      rc = mdb_del(txn, bench->db, &key, NULL);

      if (rc != MDB_NOTFOUND)
        error_check(rc, "del");

      stats_finished_single_op(&state->stats);
    }

    rc = mdb_txn_commit(txn);
    error_check(rc, "commit");

    if (bench->sync_writes) {
      rc = mdb_env_sync(bench->env, 1);
      error_check(rc, "sync");
    }
  }
}

static void
bench_delete_sequential(bench_t *bench, bench_state_t *state) {
  bench_do_delete(bench, state, 1);
}

static void
bench_delete_random(bench_t *bench, bench_state_t *state) {
  bench_do_delete(bench, state, 0);
}

static void
bench_print_stat(bench_t *bench) {
  MDB_stat ms;

  if (mdb_env_stat(bench->env, &ms) == MDB_SUCCESS) {
    fprintf(stdout, "\n"
            "MDB_stat {\n"
            "  .psize = %u,\n"
            "  .depth = %u,\n"
            "  .branch_pages = %lu,\n"
            "  .leaf_pages = %lu,\n"
            "  .overflow_pages = %lu,\n"
            "  .entries = %lu\n"
            "}\n",
            ms.ms_psize,
            ms.ms_depth,
            (unsigned long)ms.ms_branch_pages,
            (unsigned long)ms.ms_leaf_pages,
            (unsigned long)ms.ms_overflow_pages,
            (unsigned long)ms.ms_entries);
  } else {
    fprintf(stdout, "\n(failed)\n");
  }
}

static void
bench_print_info(bench_t *bench) {
  MDB_envinfo me;

  if (mdb_env_info(bench->env, &me) == MDB_SUCCESS) {
    fprintf(stdout, "\n"
            "MDB_envinfo {\n"
            "  .mapsize = %lu,\n"
            "  .last_pgno = %lu,\n"
            "  .last_txnid = %lu,\n"
            "  .maxreaders = %u,\n"
            "  .numreaders = %u\n"
            "}\n",
            (unsigned long)me.me_mapsize,
            (unsigned long)me.me_last_pgno,
            (unsigned long)me.me_last_txnid,
            me.me_maxreaders,
            me.me_numreaders);
  } else {
    fprintf(stdout, "\n(failed)\n");
  }
}

static void
bench_run(bench_t *bench) {
  const char *benchmarks = FLAGS_benchmarks;
  char name[128];

  bench_print_header(bench);
  bench_open(bench);

  while (benchmarks != NULL) {
    const char *sep = strchr(benchmarks, ',');
    void (*method)(bench_t *, bench_state_t *);
    int fresh_db;

    if (sep == NULL) {
      if (strlen(benchmarks) + 1 > sizeof(name))
        continue;

      strcpy(name, benchmarks);
      benchmarks = NULL;
    } else {
      size_t len = sep - benchmarks;

      if (len + 1 > sizeof(name))
        continue;

      memcpy(name, benchmarks, len);

      name[len] = '\0';

      benchmarks = sep + 1;
    }

    /* Reset parameters that may be overridden below. */
    bench->num = FLAGS_num;
    bench->reads = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
    bench->val_size = FLAGS_value_size;
    bench->entries_per_batch = 1;
    bench->sync_writes = 0;

    method = NULL;
    fresh_db = 0;

    if (strcmp(name, "open") == 0) {
      method = &bench_open_db;
      bench->num /= 10000;
      if (bench->num < 1)
        bench->num = 1;
    } else if (strcmp(name, "fillseq") == 0) {
      fresh_db = 1;
      method = &bench_write_sequential;
    } else if (strcmp(name, "fillbatch") == 0) {
      fresh_db = 1;
      bench->entries_per_batch = 1000;
      method = &bench_write_sequential;
    } else if (strcmp(name, "fillrandom") == 0) {
      fresh_db = 1;
      method = &bench_write_random;
    } else if (strcmp(name, "overwrite") == 0) {
      fresh_db = 0;
      method = &bench_write_random;
    } else if (strcmp(name, "fillsync") == 0) {
      fresh_db = 1;
      bench->num /= 1000;
      bench->sync_writes = 1;
      method = &bench_write_random;
    } else if (strcmp(name, "fill100K") == 0) {
      fresh_db = 1;
      bench->num /= 1000;
      bench->val_size = 100 * 1000;
      method = &bench_write_random;
    } else if (strcmp(name, "readseq") == 0) {
      method = &bench_read_sequential;
    } else if (strcmp(name, "readreverse") == 0) {
      method = &bench_read_reverse;
    } else if (strcmp(name, "readrandom") == 0) {
      method = &bench_read_random;
    } else if (strcmp(name, "readmissing") == 0) {
      method = &bench_read_missing;
    } else if (strcmp(name, "seekrandom") == 0) {
      method = &bench_seek_random;
    } else if (strcmp(name, "seekordered") == 0) {
      method = &bench_seek_ordered;
    } else if (strcmp(name, "readhot") == 0) {
      method = &bench_read_hot;
    } else if (strcmp(name, "readrandomsmall") == 0) {
      bench->reads /= 1000;
      method = &bench_read_random;
    } else if (strcmp(name, "deleteseq") == 0) {
      method = &bench_delete_sequential;
    } else if (strcmp(name, "deleterandom") == 0) {
      method = &bench_delete_random;
    } else if (strcmp(name, "stat") == 0) {
      bench_print_stat(bench);
    } else if (strcmp(name, "info") == 0) {
      bench_print_info(bench);
    } else {
      if (*name) /* No error message for empty name. */
        fprintf(stderr, "unknown benchmark '%s'\n", name);
    }

    if (fresh_db) {
      if (FLAGS_use_existing_db) {
        fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n", name);
        method = NULL;
      } else {
        if (bench->env != NULL) {
          mdb_dbi_close(bench->env, bench->db);
          mdb_env_close(bench->env);
        }

        bench->env = NULL;
        bench->db = 0;

        ldb_remove_file(FLAGS_db);

        bench_open(bench);
      }
    }

    if (method != NULL)
      run_benchmark(bench, name, method);
  }
}

int
main(int argc, char **argv) {
  char db_path[LDB_PATH_MAX];
  bench_t bench;
  int i;

  for (i = 1; i < argc; i++) {
    unsigned long ul;
    char junk;
    double d;
    int n;

    if (ldb_starts_with(argv[i], "--benchmarks=")) {
      FLAGS_benchmarks = argv[i] + 13;
    } else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1) {
      FLAGS_compression_ratio = d;
    } else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_histogram = n;
    } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_use_existing_db = n;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
      FLAGS_reads = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--key_prefix=%d%c", &n, &junk) == 1) {
      FLAGS_key_prefix = n < 0 ? 0 : LDB_MIN(n, 495); /* maxkeysize=511 */
    } else if (sscanf(argv[i], "--write_map=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_write_map = n;
    } else if (sscanf(argv[i], "--meta_sync=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_meta_sync = n;
    } else if (sscanf(argv[i], "--sync=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_sync = n;
    } else if (sscanf(argv[i], "--map_async=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_map_async = n;
    } else if (sscanf(argv[i], "--tls=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_tls = n;
    } else if (sscanf(argv[i], "--locks=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_locks = n;
    } else if (sscanf(argv[i], "--readahead=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_readahead = n;
    } else if (sscanf(argv[i], "--map_size=%lu%c", &ul, &junk) == 1) {
      FLAGS_map_size = ul;
    } else if (ldb_starts_with(argv[i], "--db=")) {
      FLAGS_db = argv[i] + 5;
    } else {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  /* Choose a location for the test database if none given with --db=<path>. */
  if (FLAGS_db == NULL) {
    if (!ldb_test_filename(db_path, sizeof(db_path), "dbbench_lmdb"))
      return 1;

    FLAGS_db = db_path;
  }

  bench_init(&bench);
  bench_run(&bench);
  bench_clear(&bench);

  return 0;
}
