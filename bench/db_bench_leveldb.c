/*!
 * db_bench_leveldb.c - leveldb benchmarks for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on google/leveldb:
 *   Copyright (c) 2011, The LevelDB Authors. All rights reserved.
 *   https://github.com/google/leveldb
 *
 * See LICENSE for more information.
 */

#include <assert.h>
#include <limits.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include <leveldb/c.h>

#include "util/buffer.h"
#include "util/env.h"
#include "util/internal.h"
#include "util/options.h"
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
 *      compact     -- Compact the entire DB
 *      stats       -- Print DB stats
 *      sstables    -- Print sstable info
 */
static const char *FLAGS_benchmarks =
    "fillseq,"
    "fillsync,"
    "fillrandom,"
    "overwrite,"
    "readrandom,"
    "readrandom," /* Extra run to allow previous compactions to quiesce. */
    "readseq,"
    "readreverse,"
    "compact,"
    "readrandom,"
    "readseq,"
    "readreverse,"
    "fill100K,";

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

/* Number of bytes to buffer in memtable before compacting
   (initialized to default value by "main") */
static int FLAGS_write_buffer_size = 0;

/* Number of bytes written to each file.
   (initialized to default value by "main") */
static int FLAGS_max_file_size = 0;

/* Approximate size of user data packed per block (before compression.
   (initialized to default value by "main") */
static int FLAGS_block_size = 0;

/* Number of bytes to use as a cache of uncompressed data.
   Negative means use default settings. */
static int FLAGS_cache_size = -1;

/* Maximum number of files to keep open at the same time
   (use default if == 0) */
static int FLAGS_open_files = 0;

/* Bloom filter bits per key.
   Negative means use default settings. */
static int FLAGS_bloom_bits = -1;

/* Common key prefix length. */
static int FLAGS_key_prefix = 0;

/* If true, do not destroy the existing database. If you set this
   flag and also specify a benchmark that wants a fresh database, that
   benchmark will fail. */
static int FLAGS_use_existing_db = 0;

/* If true, use compression. */
static int FLAGS_compression = 1;

/* Use the db with the following name. */
static const char *FLAGS_db = NULL;

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

static const char *
rng_generate(rng_t *rng, size_t len) {
  if (rng->pos + len > rng->data.size) {
    rng->pos = 0;
    assert(len < rng->data.size);
  }

  rng->pos += len;

  return (char *)rng->data.data + rng->pos - len;
}

/*
 * KeyBuffer
 */

static const char *
key_encode(int k, char *buffer) {
  memset(buffer, 'a', FLAGS_key_prefix);
  sprintf(buffer + FLAGS_key_prefix, "%016d", k);
  return buffer;
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
  leveldb_cache_t *cache;
  leveldb_filterpolicy_t *policy;
  leveldb_options_t *options;
  leveldb_readoptions_t *read_options;
  leveldb_writeoptions_t *write_options;
  leveldb_t *db;
  int num;
  int key_size;
  int val_size;
  int entries_per_batch;
  int reads;
  int total_bench_count;
} bench_t;

static void
bench_init(bench_t *bench) {
  char *err = NULL;

  bench->cache = FLAGS_cache_size >= 0
               ? leveldb_cache_create_lru(FLAGS_cache_size)
               : NULL;

  bench->policy = FLAGS_bloom_bits >= 0
                ? leveldb_filterpolicy_create_bloom(FLAGS_bloom_bits)
                : NULL;

  bench->options = leveldb_options_create();
  bench->read_options = leveldb_readoptions_create();
  bench->write_options = leveldb_writeoptions_create();

  bench->db = NULL;
  bench->num = FLAGS_num;
  bench->key_size = FLAGS_key_prefix + 16;
  bench->val_size = FLAGS_value_size;
  bench->entries_per_batch = 1;
  bench->reads = FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads;
  bench->total_bench_count = 0;

  if (!FLAGS_use_existing_db) {
    leveldb_destroy_db(bench->options, FLAGS_db, &err);
    free(err);
  }
}

static void
bench_clear(bench_t *bench) {
  if (bench->db != NULL)
    leveldb_close(bench->db);

  if (bench->cache != NULL)
    leveldb_cache_destroy(bench->cache);

  if (bench->policy != NULL)
    leveldb_filterpolicy_destroy(bench->policy);

  leveldb_options_destroy(bench->options);
  leveldb_readoptions_destroy(bench->read_options);
  leveldb_writeoptions_destroy(bench->write_options);
}

static void
bench_open(bench_t *bench) {
  leveldb_options_t *options = bench->options;
  char *err = NULL;

  assert(bench->db == NULL);

  leveldb_options_set_create_if_missing(options, !FLAGS_use_existing_db);

  if (bench->cache != NULL)
    leveldb_options_set_cache(options, bench->cache);

  leveldb_options_set_write_buffer_size(options, FLAGS_write_buffer_size);

  /* Requires leveldb 1.21 (March 2019). */
  /* leveldb_options_set_max_file_size(options, FLAGS_max_file_size); */

  leveldb_options_set_block_size(options, FLAGS_block_size);
  leveldb_options_set_max_open_files(options, FLAGS_open_files);

  if (bench->policy != NULL)
    leveldb_options_set_filter_policy(options, bench->policy);

  leveldb_options_set_compression(options, FLAGS_compression);

  bench->db = leveldb_open(options, FLAGS_db, &err);

  if (err != NULL) {
    fprintf(stderr, "open error: %s\n", err);
    exit(1);
  }
}

static void
bench_print_environment(void) {
#ifdef __linux__
  time_t now = time(NULL);
  FILE *cpuinfo;
#endif

  fprintf(stderr, "LevelDB:    version %d.%d\n", leveldb_major_version(),
                                                 leveldb_minor_version());

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
  fprintf(stdout, "Values:     %d bytes each (%d bytes after compression)\n",
                  FLAGS_value_size,
                  (int)(FLAGS_value_size * FLAGS_compression_ratio + 0.5));

  fprintf(stdout, "Entries:    %d\n", bench->num);
  fprintf(stdout, "RawSize:    %.1f MB (estimated)\n",
          ((int64_t)(key_size + FLAGS_value_size) * bench->num) / 1048576.0);

  fprintf(stdout, "FileSize:   %.1f MB (estimated)\n",
    ((key_size + FLAGS_value_size * FLAGS_compression_ratio) * bench->num)
    / 1048576.0);

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
    if (bench->db != NULL)
      leveldb_close(bench->db);

    bench->db = NULL;

    bench_open(bench);

    stats_finished_single_op(&state->stats);
  }
}

static void
bench_do_write(bench_t *bench, bench_state_t *state, int seq) {
  int64_t bytes = 0;
  char buffer[1024];
  rng_t gen;
  int i, j;

  rng_init(&gen);

  if (bench->num != FLAGS_num) {
    char msg[100];
    sprintf(msg, "(%d ops)", bench->num);
    stats_add_message(&state->stats, msg);
  }

  for (i = 0; i < bench->num; i += bench->entries_per_batch) {
    leveldb_writebatch_t *batch = leveldb_writebatch_create();
    char *err = NULL;

    for (j = 0; j < bench->entries_per_batch; j++) {
      int k = seq ? i + j : (int)ldb_rand_uniform(&state->rnd, FLAGS_num);
      const char *key = key_encode(k, buffer);
      const char *val = rng_generate(&gen, bench->val_size);

      leveldb_writebatch_put(batch, key, bench->key_size,
                                    val, bench->val_size);

      bytes += bench->key_size + bench->val_size;

      stats_finished_single_op(&state->stats);
    }

    leveldb_write(bench->db, bench->write_options, batch, &err);

    if (err != NULL) {
      fprintf(stderr, "put error: %s\n", err);
      exit(1);
    }

    leveldb_writebatch_destroy(batch);
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
  size_t key_size, val_size;
  leveldb_iterator_t *iter;
  int64_t bytes = 0;
  int i = 0;

  iter = leveldb_create_iterator(bench->db, bench->read_options);

  for (leveldb_iter_seek_to_first(iter);
       i < bench->reads && leveldb_iter_valid(iter);
       i++, leveldb_iter_next(iter)) {
    (void)leveldb_iter_key(iter, &key_size);
    (void)leveldb_iter_value(iter, &val_size);

    bytes += key_size + val_size;

    stats_finished_single_op(&state->stats);
  }

  leveldb_iter_destroy(iter);

  stats_add_bytes(&state->stats, bytes);
}

static void
bench_read_reverse(bench_t *bench, bench_state_t *state) {
  size_t key_size, val_size;
  leveldb_iterator_t *iter;
  int64_t bytes = 0;
  int i = 0;

  iter = leveldb_create_iterator(bench->db, bench->read_options);

  for (leveldb_iter_seek_to_last(iter);
       i < bench->reads && leveldb_iter_valid(iter);
       i++, leveldb_iter_prev(iter)) {
    (void)leveldb_iter_key(iter, &key_size);
    (void)leveldb_iter_value(iter, &val_size);

    bytes += key_size + val_size;

    stats_finished_single_op(&state->stats);
  }

  leveldb_iter_destroy(iter);

  stats_add_bytes(&state->stats, bytes);
}

static void
bench_read_random(bench_t *bench, bench_state_t *state) {
  char buffer[1024], msg[100];
  int found = 0;
  int i;

  for (i = 0; i < bench->reads; i++) {
    const int k = ldb_rand_uniform(&state->rnd, FLAGS_num);
    const char *key = key_encode(k, buffer);
    char *err = NULL;
    size_t val_size;
    char *val;

    val = leveldb_get(bench->db,
                      bench->read_options,
                      key,
                      bench->key_size,
                      &val_size,
                      &err);

    if (val != NULL) {
      leveldb_free(val);
      found++;
    }

    free(err);

    stats_finished_single_op(&state->stats);
  }

  sprintf(msg, "(%d of %d found)", found, bench->num);
  stats_add_message(&state->stats, msg);
}

static void
bench_read_missing(bench_t *bench, bench_state_t *state) {
  char buffer[1024];
  int i;

  for (i = 0; i < bench->reads; i++) {
    const int k = ldb_rand_uniform(&state->rnd, FLAGS_num);
    const char *key = key_encode(k, buffer);
    char *err = NULL;
    size_t val_size;
    char *val;

    val = leveldb_get(bench->db,
                      bench->read_options,
                      key,
                      bench->key_size - 1,
                      &val_size,
                      &err);

    if (val != NULL)
      leveldb_free(val);

    free(err);

    stats_finished_single_op(&state->stats);
  }
}

static void
bench_read_hot(bench_t *bench, bench_state_t *state) {
  const int range = (FLAGS_num + 99) / 100;
  char buffer[1024];
  int i;

  for (i = 0; i < bench->reads; i++) {
    const int k = ldb_rand_uniform(&state->rnd, range);
    const char *key = key_encode(k, buffer);
    char *err = NULL;
    size_t val_size;
    char *val;

    val = leveldb_get(bench->db,
                      bench->read_options,
                      key,
                      bench->key_size,
                      &val_size,
                      &err);

    if (val != NULL)
      leveldb_free(val);

    free(err);

    stats_finished_single_op(&state->stats);
  }
}

static void
bench_seek_random(bench_t *bench, bench_state_t *state) {
  leveldb_readoptions_t *opt = bench->read_options;
  char buffer[1024], msg[100];
  int found = 0;
  int i;

  for (i = 0; i < bench->reads; i++) {
    leveldb_iterator_t *iter = leveldb_create_iterator(bench->db, opt);
    const int k = ldb_rand_uniform(&state->rnd, FLAGS_num);
    const char *key = key_encode(k, buffer);
    size_t cur_size;

    leveldb_iter_seek(iter, key, bench->key_size);

    if (leveldb_iter_valid(iter)) {
      const char *cur = leveldb_iter_key(iter, &cur_size);

      if (cur_size == (size_t)bench->key_size &&
          memcmp(cur, key, bench->key_size) == 0) {
        found++;
      }
    }

    leveldb_iter_destroy(iter);

    stats_finished_single_op(&state->stats);
  }

  sprintf(msg, "(%d of %d found)", found, bench->num);
  stats_add_message(&state->stats, msg);
}

static void
bench_seek_ordered(bench_t *bench, bench_state_t *state) {
  char buffer[1024], msg[100];
  leveldb_iterator_t *iter;
  int found = 0;
  int last = 0;
  int i;

  iter = leveldb_create_iterator(bench->db, bench->read_options);

  for (i = 0; i < bench->reads; i++) {
    const int k = (last + ldb_rand_uniform(&state->rnd, 100)) % FLAGS_num;
    const char *key = key_encode(k, buffer);
    size_t cur_size;

    leveldb_iter_seek(iter, key, bench->key_size);

    if (leveldb_iter_valid(iter)) {
      const char *cur = leveldb_iter_key(iter, &cur_size);

      if (cur_size == (size_t)bench->key_size &&
          memcmp(cur, key, bench->key_size) == 0) {
        found++;
      }
    }

    stats_finished_single_op(&state->stats);

    last = k;
  }

  leveldb_iter_destroy(iter);

  sprintf(msg, "(%d of %d found)", found, bench->num);
  stats_add_message(&state->stats, msg);
}

static void
bench_do_delete(bench_t *bench, bench_state_t *state, int seq) {
  char buffer[1024];
  int i, j;

  for (i = 0; i < bench->num; i += bench->entries_per_batch) {
    leveldb_writebatch_t *batch = leveldb_writebatch_create();
    char *err = NULL;

    for (j = 0; j < bench->entries_per_batch; j++) {
      int k = seq ? i + j : (int)ldb_rand_uniform(&state->rnd, FLAGS_num);
      const char *key = key_encode(k, buffer);

      leveldb_writebatch_delete(batch, key, bench->key_size);

      stats_finished_single_op(&state->stats);
    }

    leveldb_write(bench->db, bench->write_options, batch, &err);

    if (err != NULL) {
      fprintf(stderr, "del error: %s\n", err);
      exit(1);
    }

    leveldb_writebatch_destroy(batch);
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
bench_compact(bench_t *bench, bench_state_t *state) {
  (void)state;
  leveldb_compact_range(bench->db, NULL, 0, NULL, 0);
}

static void
bench_print_stats(bench_t *bench, const char *key) {
  char *stats = leveldb_property_value(bench->db, key);

  if (stats != NULL) {
    fprintf(stdout, "\n%s\n", stats);
    free(stats);
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
    leveldb_writeoptions_set_sync(bench->write_options, 0);

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
      leveldb_writeoptions_set_sync(bench->write_options, 1);
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
    } else if (strcmp(name, "compact") == 0) {
      method = &bench_compact;
    } else if (strcmp(name, "stats") == 0) {
      bench_print_stats(bench, "leveldb.stats");
    } else if (strcmp(name, "sstables") == 0) {
      bench_print_stats(bench, "leveldb.sstables");
    } else {
      if (*name) /* No error message for empty name. */
        fprintf(stderr, "unknown benchmark '%s'\n", name);
    }

    if (fresh_db) {
      if (FLAGS_use_existing_db) {
        fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n", name);
        method = NULL;
      } else {
        char *err = NULL;

        if (bench->db != NULL)
          leveldb_close(bench->db);

        bench->db = NULL;

        leveldb_destroy_db(bench->options, FLAGS_db, &err);
        free(err);

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

  FLAGS_write_buffer_size = ldb_dbopt_default->write_buffer_size;
  FLAGS_max_file_size = ldb_dbopt_default->max_file_size;
  FLAGS_block_size = ldb_dbopt_default->block_size;
  FLAGS_open_files = ldb_dbopt_default->max_open_files;

  for (i = 1; i < argc; i++) {
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
    } else if (sscanf(argv[i], "--compression=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_compression = n;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
      FLAGS_reads = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    } else if (sscanf(argv[i], "--max_file_size=%d%c", &n, &junk) == 1) {
      FLAGS_max_file_size = n;
    } else if (sscanf(argv[i], "--block_size=%d%c", &n, &junk) == 1) {
      FLAGS_block_size = n;
    } else if (sscanf(argv[i], "--key_prefix=%d%c", &n, &junk) == 1) {
      FLAGS_key_prefix = n < 0 ? 0 : LDB_MIN(n, 1000);
    } else if (sscanf(argv[i], "--cache_size=%d%c", &n, &junk) == 1) {
      FLAGS_cache_size = n;
    } else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
      FLAGS_bloom_bits = n;
    } else if (sscanf(argv[i], "--open_files=%d%c", &n, &junk) == 1) {
      FLAGS_open_files = n;
    } else if (ldb_starts_with(argv[i], "--db=")) {
      FLAGS_db = argv[i] + 5;
    } else {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  /* Choose a location for the test database if none given with --db=<path>. */
  if (FLAGS_db == NULL) {
    if (!ldb_test_filename(db_path, sizeof(db_path), "dbbench_leveldb"))
      return 1;

    FLAGS_db = db_path;
  }

  bench_init(&bench);
  bench_run(&bench);
  bench_clear(&bench);

  return 0;
}
