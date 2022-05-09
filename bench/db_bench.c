/*!
 * db_bench.c - database benchmarks for lcdb
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

#include "table/iterator.h"

#include "util/atomic.h"
#include "util/bloom.h"
#include "util/buffer.h"
#include "util/cache.h"
#include "util/comparator.h"
#include "util/crc32c.h"
#include "util/env.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/port.h"
#include "util/random.h"
#include "util/slice.h"
#include "util/snappy.h"
#include "util/status.h"
#include "util/strutil.h"
#include "util/testutil.h"

#include "db_impl.h"
#include "db_iter.h"
#include "histogram.h"
#include "write_batch.h"

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
 *      crc32c        -- repeated crc32c of 4K of data
 *   Meta operations:
 *      compact     -- Compact the entire DB
 *      stats       -- Print DB stats
 *      sstables    -- Print sstable info
 *      heapprofile -- Dump a heap profile (if supported by this port)
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
    "fill100K,"
    "crc32c,"
    "snappycomp,"
    "snappyuncomp,";

/* Number of key/values to place in database. */
static int FLAGS_num = 1000000;

/* Number of read operations to do. If negative, do FLAGS_num reads. */
static int FLAGS_reads = -1;

/* Number of concurrent threads to run. */
static int FLAGS_threads = 1;

/* Size of each value */
static int FLAGS_value_size = 100;

/* Arrange to generate values that shrink to this fraction of
   their original size after compression */
static double FLAGS_compression_ratio = 0.5;

/* Print histogram of operation timings */
static int FLAGS_histogram = 0;

/* Count the number of string comparisons performed */
static int FLAGS_comparisons = 0;

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

/* If true, reuse existing log/MANIFEST files when re-opening a database. */
static int FLAGS_reuse_logs = 0;

/* If true, use compression. */
static int FLAGS_compression = 1;

/* If true, use memory-mapped reads. */
static int FLAGS_use_mmap = 1;

/* Use the db with the following name. */
static const char *FLAGS_db = NULL;

/*
 * CountComparator
 */

typedef struct counter_state_s {
  ldb_atomic(size_t) count;
} counter_state_t;

static int
count_comparator_compare(const ldb_comparator_t *comparator,
                         const ldb_slice_t *x,
                         const ldb_slice_t *y) {
  const ldb_comparator_t *wrapped = ldb_bytewise_comparator;
  counter_state_t *state = comparator->state;

  ldb_atomic_fetch_add(&state->count, 1, ldb_order_relaxed);

  return wrapped->compare(NULL, x, y);
}

static void
count_comparator_init(ldb_comparator_t *comparator, counter_state_t *state) {
  const ldb_comparator_t *wrapped = ldb_bytewise_comparator;

  comparator->name = wrapped->name;
  comparator->compare = count_comparator_compare;
  comparator->shortest_separator = wrapped->shortest_separator;
  comparator->short_successor = wrapped->short_successor;
  comparator->user_comparator = NULL;
  comparator->state = state;
}

static size_t
count_comparator_comparisons(ldb_comparator_t *comparator) {
  counter_state_t *state = comparator->state;
  return ldb_atomic_load(&state->count, ldb_order_relaxed);
}

static void
count_comparator_reset(ldb_comparator_t *comparator) {
  counter_state_t *state = comparator->state;
  ldb_atomic_store(&state->count, 0, ldb_order_relaxed);
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

static ldb_slice_t
rng_generate(rng_t *rng, size_t len) {
  if (rng->pos + len > rng->data.size) {
    rng->pos = 0;
    assert(len < rng->data.size);
  }

  rng->pos += len;

  return ldb_slice(rng->data.data + rng->pos - len, len);
}

/*
 * KeyBuffer
 */

typedef struct keybuf_s {
  char buffer[1024];
} keybuf_t;

static void
keybuf_init(keybuf_t *kb) {
  if ((size_t)FLAGS_key_prefix >= sizeof(kb->buffer) - 32)
    abort();

  memset(kb->buffer, 'a', FLAGS_key_prefix);
}

static void
keybuf_set(keybuf_t *kb, int k) {
  sprintf(kb->buffer + FLAGS_key_prefix, "%016d", k);
}

static ldb_slice_t
keybuf_slice(const keybuf_t *kb) {
  return ldb_slice((unsigned char *)kb->buffer, FLAGS_key_prefix + 16);
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
append_with_space(ldb_buffer_t *buf, const ldb_slice_t *msg) {
  if (msg->size == 0)
    return;

  if (buf->size > 0)
    ldb_buffer_push(buf, ' ');

  ldb_buffer_concat(buf, msg);
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
stats_merge(stats_t *z, const stats_t *x) {
  histogram_merge(&z->hist, &x->hist);

  z->done += x->done;
  z->bytes += x->bytes;
  z->seconds += x->seconds;

  if (x->start < z->start)
    z->start = x->start;

  if (x->finish > z->finish)
    z->finish = x->finish;

  /* Just keep the messages from one thread. */
  if (z->message.size == 0)
    ldb_buffer_copy(&z->message, &x->message);
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
 * SharedState
 */

/* State shared by all concurrent executions of the same benchmark. */
typedef struct shared_state_s {
  ldb_mutex_t mu;
  ldb_cond_t cv;
  int total;

  /* Each thread goes through the following states:
   *    (1) initializing
   *    (2) waiting for others to be initialized
   *    (3) running
   *    (4) done
   */

  int num_initialized;
  int num_done;
  int start;
} shared_state_t;

static void
shared_state_init(shared_state_t *shared, int total) {
  ldb_mutex_init(&shared->mu);
  ldb_cond_init(&shared->cv);
  shared->total = total;
  shared->num_initialized = 0;
  shared->num_done = 0;
  shared->start = 0;
}

static void
shared_state_clear(shared_state_t *shared) {
  ldb_mutex_destroy(&shared->mu);
  ldb_cond_destroy(&shared->cv);
}

/*
 * ThreadState
 */

/* Per-thread state for concurrent executions of the same benchmark. */
typedef struct thread_state_s {
  int tid;        /* 0..n-1 when running in n threads. */
  ldb_rand_t rnd; /* Has different seeds for different threads. */
  stats_t stats;
  shared_state_t *shared;
} thread_state_t;

static void
thread_state_init(thread_state_t *thread, int index, int seed) {
  thread->tid = index;
  ldb_rand_init(&thread->rnd, seed);
  stats_init(&thread->stats);
  thread->shared = NULL;
}

static void
thread_state_clear(thread_state_t *thread) {
  stats_clear(&thread->stats);
}

/*
 * Benchmark
 */

typedef struct bench_s {
  ldb_lru_t *cache;
  ldb_bloom_t *filter_policy;
  ldb_t *db;
  int num;
  int value_size;
  int entries_per_batch;
  ldb_writeopt_t write_options;
  int reads;
  int heap_counter;
  counter_state_t count_state;
  ldb_comparator_t count_comparator;
  int total_thread_count;
} bench_t;

static void
bench_init(bench_t *bench) {
  char path[LDB_PATH_MAX];
  char **files;
  int i, len;

  bench->cache = FLAGS_cache_size >= 0
               ? ldb_lru_create(FLAGS_cache_size)
               : NULL;

  bench->filter_policy = FLAGS_bloom_bits >= 0
                       ? ldb_bloom_create(FLAGS_bloom_bits)
                       : NULL;

  bench->db = NULL;
  bench->num = FLAGS_num;
  bench->value_size = FLAGS_value_size;
  bench->entries_per_batch = 1;
  bench->write_options = *ldb_writeopt_default;
  bench->reads = FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads;
  bench->heap_counter = 0;
  bench->count_state.count = 0;
  count_comparator_init(&bench->count_comparator, &bench->count_state);
  bench->total_thread_count = 0;

  len = ldb_get_children(FLAGS_db, &files);

  for (i = 0; i < len; i++) {
    if (ldb_starts_with(files[i], "heap-") &&
        ldb_join(path, sizeof(path), FLAGS_db, files[i])) {
      ldb_remove_file(path);
    }
  }

  if (len >= 0)
    ldb_free_children(files, len);

  if (!FLAGS_use_existing_db)
    ldb_destroy(FLAGS_db, ldb_dbopt_default);
}

static void
bench_clear(bench_t *bench) {
  if (bench->db != NULL)
    ldb_close(bench->db);

  if (bench->cache != NULL)
    ldb_lru_destroy(bench->cache);

  if (bench->filter_policy != NULL)
    ldb_bloom_destroy(bench->filter_policy);
}

static void
bench_open(bench_t *bench) {
  ldb_dbopt_t options = *ldb_dbopt_default;
  int rc;

  assert(bench->db == NULL);

  options.create_if_missing = !FLAGS_use_existing_db;
  options.block_cache = bench->cache;
  options.write_buffer_size = FLAGS_write_buffer_size;
  options.max_file_size = FLAGS_max_file_size;
  options.block_size = FLAGS_block_size;

  if (FLAGS_comparisons)
    options.comparator = &bench->count_comparator;

  options.max_open_files = FLAGS_open_files;
  options.filter_policy = bench->filter_policy;
  options.reuse_logs = FLAGS_reuse_logs;
  options.compression = (enum ldb_compression)FLAGS_compression;
  options.use_mmap = FLAGS_use_mmap;

  rc = ldb_open(FLAGS_db, &options, &bench->db);

  if (rc != LDB_OK) {
    fprintf(stderr, "open error: %s\n", ldb_strerror(rc));
    exit(1);
  }
}

static void
bench_print_environment(bench_t *bench) {
#ifdef __linux__
  FILE *cpuinfo;
  time_t now;
#endif

  (void)bench;

  fprintf(stderr, "Database:   version %d.%d\n", 0, 0);

#ifdef __linux__
  now = time(NULL);

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
bench_print_warnings(bench_t *bench) {
  (void)bench;
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

  bench_print_environment(bench);

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

  bench_print_warnings(bench);

  fprintf(stdout, "------------------------------------------------\n");
}

#if defined(_WIN32) || defined(LDB_PTHREAD)

typedef struct thread_arg_s {
  bench_t *bench;
  shared_state_t *shared;
  thread_state_t *thread;
  void (*method)(bench_t *, thread_state_t *);
} thread_arg_t;

static void
thread_body(void *ptr) {
  thread_arg_t *arg = (thread_arg_t *)ptr;
  shared_state_t *shared = arg->shared;
  thread_state_t *thread = arg->thread;

  {
    ldb_mutex_lock(&shared->mu);

    shared->num_initialized++;

    if (shared->num_initialized >= shared->total)
      ldb_cond_broadcast(&shared->cv);

    while (!shared->start)
      ldb_cond_wait(&shared->cv, &shared->mu);

    ldb_mutex_unlock(&shared->mu);
  }

  stats_start(&thread->stats);

  arg->method(arg->bench, thread);

  stats_stop(&thread->stats);

  {
    ldb_mutex_lock(&shared->mu);

    shared->num_done++;

    if (shared->num_done >= shared->total)
      ldb_cond_broadcast(&shared->cv);

    ldb_mutex_unlock(&shared->mu);
  }
}

static void
run_benchmark(bench_t *bench, int n, const char *name,
              void (*method)(bench_t *, thread_state_t *)) {
  shared_state_t shared;
  thread_arg_t *arg;
  int i;

  shared_state_init(&shared, n);

  arg = ldb_malloc(sizeof(thread_arg_t) * n);

  for (i = 0; i < n; i++) {
    arg[i].bench = bench;
    arg[i].method = method;
    arg[i].shared = &shared;

    ++bench->total_thread_count;

    /* Seed the thread's random state deterministically based upon thread
       creation across all benchmarks. This ensures that the seeds are unique
       but reproducible when rerunning the same set of benchmarks. */
    arg[i].thread = ldb_malloc(sizeof(thread_state_t));

    thread_state_init(arg[i].thread, i, 1000 + bench->total_thread_count);

    arg[i].thread->shared = &shared;

    {
      ldb_thread_t thread;
      ldb_thread_create(&thread, thread_body, &arg[i]);
      ldb_thread_detach(&thread);
    }
  }

  ldb_mutex_lock(&shared.mu);

  while (shared.num_initialized < n)
    ldb_cond_wait(&shared.cv, &shared.mu);

  shared.start = 1;

  ldb_cond_broadcast(&shared.cv);

  while (shared.num_done < n)
    ldb_cond_wait(&shared.cv, &shared.mu);

  ldb_mutex_unlock(&shared.mu);

  for (i = 1; i < n; i++)
    stats_merge(&arg[0].thread->stats, &arg[i].thread->stats);

  stats_report(&arg[0].thread->stats, name);

  if (FLAGS_comparisons) {
    fprintf(stdout, "Comparisons: %lu\n",
      (unsigned long)count_comparator_comparisons(&bench->count_comparator));

    count_comparator_reset(&bench->count_comparator);

    fflush(stdout);
  }

  for (i = 0; i < n; i++) {
    thread_state_clear(arg[i].thread);
    ldb_free(arg[i].thread);
  }

  ldb_free(arg);

  shared_state_clear(&shared);
}

#else /* !_WIN32 && !LDB_PTHREAD */

static void
run_benchmark(bench_t *bench, int n, const char *name,
              void (*method)(bench_t *, thread_state_t *)) {
  shared_state_t shared;
  thread_state_t thread;

  shared_state_init(&shared, n);
  thread_state_init(&thread, 0, 1001);

  thread.shared = &shared;

  stats_start(&thread.stats);

  method(bench, &thread);

  (void)stats_merge;

  stats_stop(&thread.stats);
  stats_report(&thread.stats, name);

  if (FLAGS_comparisons) {
    fprintf(stdout, "Comparisons: %lu\n",
      (unsigned long)count_comparator_comparisons(&bench->count_comparator));

    count_comparator_reset(&bench->count_comparator);

    fflush(stdout);
  }

  thread_state_clear(&thread);
  shared_state_clear(&shared);
}

#endif /* !_WIN32 && !LDB_PTHREAD */

static void
bench_crc32c(bench_t *bench, thread_state_t *thread) {
  /* Checksum about 500MB of data total. */
  const char *label = "(4K per op)";
  uint8_t data[4096];
  int64_t bytes = 0;
  uint32_t crc = 0;

  (void)bench;

  memset(data, 'x', sizeof(data));

  while (bytes < 500 * 1048576) {
    crc = ldb_crc32c_value(data, sizeof(data));
    stats_finished_single_op(&thread->stats);
    bytes += sizeof(data);
  }

  /* Print so result is not dead. */
  fprintf(stderr, "... crc=0x%x\r", (unsigned int)crc);

  stats_add_bytes(&thread->stats, bytes);
  stats_add_message(&thread->stats, label);
}

static void
bench_snappy_compress(bench_t *bench, thread_state_t *thread) {
  ldb_buffer_t compressed;
  ldb_slice_t input;
  int64_t bytes = 0;
  int64_t produced = 0;
  size_t space = 0;
  rng_t gen;
  int ok;

  (void)bench;

  ldb_buffer_init(&compressed);
  rng_init(&gen);

  input = rng_generate(&gen, ldb_dbopt_default->block_size);

  ok = snappy_encode_size(&space, input.size);

  ldb_buffer_grow(&compressed, space);

  while (ok && bytes < 1024 * 1048576) { /* Compress 1G. */
    compressed.size = snappy_encode(compressed.data, input.data, input.size);
    produced += compressed.size;
    bytes += input.size;
    stats_finished_single_op(&thread->stats);
  }

  if (!ok) {
    stats_add_message(&thread->stats, "(snappy failure)");
  } else {
    char buf[100];

    sprintf(buf, "(output: %.1f%%)", (produced * 100.0) / bytes);

    stats_add_message(&thread->stats, buf);
    stats_add_bytes(&thread->stats, bytes);
  }

  ldb_buffer_clear(&compressed);
  rng_clear(&gen);
}

static void
bench_snappy_uncompress(bench_t *bench, thread_state_t *thread) {
  ldb_buffer_t compressed;
  uint8_t *uncompressed;
  ldb_slice_t input;
  int64_t bytes = 0;
  size_t space = 0;
  rng_t gen;
  int ok;

  (void)bench;

  ldb_buffer_init(&compressed);
  rng_init(&gen);

  input = rng_generate(&gen, ldb_dbopt_default->block_size);

  ok = snappy_encode_size(&space, input.size);

  ldb_buffer_grow(&compressed, space);

  if (ok)
    compressed.size = snappy_encode(compressed.data, input.data, input.size);

  uncompressed = ldb_malloc(input.size);

  while (ok && bytes < 1024 * 1048576) { /* Compress 1G. */
    ok = snappy_decode(uncompressed, compressed.data, compressed.size);
    bytes += input.size;
    stats_finished_single_op(&thread->stats);
  }

  ldb_free(uncompressed);

  if (!ok)
    stats_add_message(&thread->stats, "(snappy failure)");
  else
    stats_add_bytes(&thread->stats, bytes);

  ldb_buffer_clear(&compressed);
  rng_clear(&gen);
}

static void
bench_open_db(bench_t *bench, thread_state_t *thread) {
  int i;

  for (i = 0; i < bench->num; i++) {
    if (bench->db != NULL)
      ldb_close(bench->db);

    bench->db = NULL;

    bench_open(bench);

    stats_finished_single_op(&thread->stats);
  }
}

static void
bench_do_write(bench_t *bench, thread_state_t *thread, int seq) {
  ldb_batch_t batch;
  int64_t bytes = 0;
  int rc = LDB_OK;
  keybuf_t kb;
  rng_t gen;
  int i, j;

  ldb_batch_init(&batch);
  keybuf_init(&kb);
  rng_init(&gen);

  if (bench->num != FLAGS_num) {
    char msg[100];
    sprintf(msg, "(%d ops)", bench->num);
    stats_add_message(&thread->stats, msg);
  }

  for (i = 0; i < bench->num; i += bench->entries_per_batch) {
    ldb_batch_reset(&batch);

    for (j = 0; j < bench->entries_per_batch; j++) {
      int L = seq ? i + j : (int)ldb_rand_uniform(&thread->rnd, FLAGS_num);
      ldb_slice_t key, val;

      keybuf_set(&kb, L);

      key = keybuf_slice(&kb);
      val = rng_generate(&gen, bench->value_size);

      ldb_batch_put(&batch, &key, &val);

      bytes += key.size + val.size;

      stats_finished_single_op(&thread->stats);
    }

    rc = ldb_write(bench->db, &batch, &bench->write_options);

    if (rc != LDB_OK) {
      fprintf(stderr, "put error: %s\n", ldb_strerror(rc));
      exit(1);
    }
  }

  stats_add_bytes(&thread->stats, bytes);

  ldb_batch_clear(&batch);
  rng_clear(&gen);
}

static void
bench_write_sequential(bench_t *bench, thread_state_t *thread) {
  bench_do_write(bench, thread, 1);
}

static void
bench_write_random(bench_t *bench, thread_state_t *thread) {
  bench_do_write(bench, thread, 0);
}

static void
bench_read_sequential(bench_t *bench, thread_state_t *thread) {
  ldb_iter_t *iter = ldb_iterator(bench->db, ldb_readopt_default);
  int64_t bytes = 0;
  int i = 0;

  for (ldb_iter_first(iter);
       i < bench->reads && ldb_iter_valid(iter);
       i++, ldb_iter_next(iter)) {
    ldb_slice_t key = ldb_iter_key(iter);
    ldb_slice_t val = ldb_iter_value(iter);

    bytes += key.size + val.size;

    stats_finished_single_op(&thread->stats);
  }

  ldb_iter_destroy(iter);

  stats_add_bytes(&thread->stats, bytes);
}

static void
bench_read_reverse(bench_t *bench, thread_state_t *thread) {
  ldb_iter_t *iter = ldb_iterator(bench->db, ldb_readopt_default);
  int64_t bytes = 0;
  int i = 0;

  for (ldb_iter_last(iter);
       i < bench->reads && ldb_iter_valid(iter);
       i++, ldb_iter_prev(iter)) {
    ldb_slice_t key = ldb_iter_key(iter);
    ldb_slice_t val = ldb_iter_value(iter);

    bytes += key.size + val.size;

    stats_finished_single_op(&thread->stats);
  }

  ldb_iter_destroy(iter);

  stats_add_bytes(&thread->stats, bytes);
}

static void
bench_read_random(bench_t *bench, thread_state_t *thread) {
  ldb_readopt_t options = *ldb_readopt_default;
  ldb_slice_t key, val;
  int found = 0;
  char msg[100];
  keybuf_t kb;
  int i;

  keybuf_init(&kb);

  for (i = 0; i < bench->reads; i++) {
    const int L = ldb_rand_uniform(&thread->rnd, FLAGS_num);

    keybuf_set(&kb, L);

    key = keybuf_slice(&kb);

    if (ldb_get(bench->db, &key, &val, &options) == LDB_OK) {
      ldb_free(val.data);
      found++;
    }

    stats_finished_single_op(&thread->stats);
  }

  sprintf(msg, "(%d of %d found)", found, bench->num);

  stats_add_message(&thread->stats, msg);
}

static void
bench_read_missing(bench_t *bench, thread_state_t *thread) {
  ldb_readopt_t options = *ldb_readopt_default;
  ldb_slice_t key, val;
  keybuf_t kb;
  int i;

  keybuf_init(&kb);

  for (i = 0; i < bench->reads; i++) {
    const int L = ldb_rand_uniform(&thread->rnd, FLAGS_num);
    ldb_slice_t s;

    keybuf_set(&kb, L);

    key = keybuf_slice(&kb);

    s = ldb_slice(key.data, key.size - 1);

    if (ldb_get(bench->db, &s, &val, &options) == LDB_OK)
      ldb_free(val.data);

    stats_finished_single_op(&thread->stats);
  }
}

static void
bench_read_hot(bench_t *bench, thread_state_t *thread) {
  ldb_readopt_t options = *ldb_readopt_default;
  const int range = (FLAGS_num + 99) / 100;
  ldb_slice_t key, val;
  keybuf_t kb;
  int i;

  keybuf_init(&kb);

  for (i = 0; i < bench->reads; i++) {
    const int L = ldb_rand_uniform(&thread->rnd, range);

    keybuf_set(&kb, L);

    key = keybuf_slice(&kb);

    if (ldb_get(bench->db, &key, &val, &options) == LDB_OK)
      ldb_free(val.data);

    stats_finished_single_op(&thread->stats);
  }
}

static void
bench_seek_random(bench_t *bench, thread_state_t *thread) {
  ldb_readopt_t options = *ldb_readopt_default;
  ldb_slice_t key;
  int found = 0;
  char msg[100];
  keybuf_t kb;
  int i;

  keybuf_init(&kb);

  for (i = 0; i < bench->reads; i++) {
    ldb_iter_t *iter = ldb_iterator(bench->db, &options);
    const int L = ldb_rand_uniform(&thread->rnd, FLAGS_num);

    keybuf_set(&kb, L);

    key = keybuf_slice(&kb);

    ldb_iter_seek(iter, &key);

    if (ldb_iter_valid(iter) && ldb_iter_compare(iter, &key) == 0)
      found++;

    ldb_iter_destroy(iter);

    stats_finished_single_op(&thread->stats);
  }

  sprintf(msg, "(%d of %d found)", found, bench->num);

  stats_add_message(&thread->stats, msg);
}

static void
bench_seek_ordered(bench_t *bench, thread_state_t *thread) {
  ldb_readopt_t options = *ldb_readopt_default;
  ldb_iter_t *iter = ldb_iterator(bench->db, &options);
  ldb_slice_t key;
  int found = 0;
  char msg[100];
  keybuf_t kb;
  int L = 0;
  int i;

  keybuf_init(&kb);

  for (i = 0; i < bench->reads; i++) {
    L = (L + ldb_rand_uniform(&thread->rnd, 100)) % FLAGS_num;

    keybuf_set(&kb, L);

    key = keybuf_slice(&kb);

    ldb_iter_seek(iter, &key);

    if (ldb_iter_valid(iter) && ldb_iter_compare(iter, &key) == 0)
      found++;

    stats_finished_single_op(&thread->stats);
  }

  ldb_iter_destroy(iter);

  sprintf(msg, "(%d of %d found)", found, bench->num);

  stats_add_message(&thread->stats, msg);
}

static void
bench_do_delete(bench_t *bench, thread_state_t *thread, int seq) {
  ldb_batch_t batch;
  int rc = LDB_OK;
  ldb_slice_t key;
  keybuf_t kb;
  int i, j;

  ldb_batch_init(&batch);

  keybuf_init(&kb);

  for (i = 0; i < bench->num; i += bench->entries_per_batch) {
    ldb_batch_reset(&batch);

    for (j = 0; j < bench->entries_per_batch; j++) {
      int L = seq ? i + j : (int)ldb_rand_uniform(&thread->rnd, FLAGS_num);

      keybuf_set(&kb, L);

      key = keybuf_slice(&kb);

      ldb_batch_del(&batch, &key);

      stats_finished_single_op(&thread->stats);
    }

    rc = ldb_write(bench->db, &batch, &bench->write_options);

    if (rc != LDB_OK) {
      fprintf(stderr, "del error: %s\n", ldb_strerror(rc));
      exit(1);
    }
  }

  ldb_batch_clear(&batch);
}

static void
bench_delete_sequential(bench_t *bench, thread_state_t *thread) {
  bench_do_delete(bench, thread, 1);
}

static void
bench_delete_random(bench_t *bench, thread_state_t *thread) {
  bench_do_delete(bench, thread, 0);
}

#if defined(_WIN32) || defined(LDB_PTHREAD)
static void
bench_read_while_writing(bench_t *bench, thread_state_t *thread) {
  if (thread->tid > 0) {
    bench_read_random(bench, thread);
  } else {
    /* Special thread that keeps writing until other threads are done. */
    ldb_slice_t key, val;
    keybuf_t kb;
    rng_t gen;
    int L, rc;

    keybuf_init(&kb);
    rng_init(&gen);

    for (;;) {
      {
        ldb_mutex_lock(&thread->shared->mu);

        if (thread->shared->num_done + 1 >= thread->shared->num_initialized) {
          /* Other threads have finished. */
          ldb_mutex_unlock(&thread->shared->mu);
          break;
        }

        ldb_mutex_unlock(&thread->shared->mu);
      }

      L = ldb_rand_uniform(&thread->rnd, FLAGS_num);

      keybuf_set(&kb, L);

      key = keybuf_slice(&kb);
      val = rng_generate(&gen, bench->value_size);

      rc = ldb_put(bench->db, &key, &val, &bench->write_options);

      if (rc != LDB_OK) {
        fprintf(stderr, "put error: %s\n", ldb_strerror(rc));
        exit(1);
      }
    }

    /* Do not count any of the preceding work/delay in stats. */
    stats_start(&thread->stats);

    rng_clear(&gen);
  }
}
#endif /* _WIN32 || LDB_PTHREAD */

static void
bench_compact(bench_t *bench, thread_state_t *thread) {
  (void)thread;
  ldb_compact(bench->db, NULL, NULL);
}

static void
bench_print_stats(bench_t *bench, const char *key) {
  char *stats = NULL;

  if (ldb_property(bench->db, key, &stats)) {
    fprintf(stdout, "\n%s\n", stats);
    free(stats);
  } else {
    fprintf(stdout, "\n(failed)\n");
  }
}

static void
write_to_file(void *arg, const char *buf, int n) {
  ldb_slice_t data = ldb_slice((const unsigned char *)buf, n);
  ldb_wfile_t *file = (ldb_wfile_t *)arg;

  ldb_wfile_append(file, &data);
}

static int
ldb_heap_profile(void (*writer)(void *, const char *, int), void *arg) {
  (void)writer;
  (void)arg;
  return 0;
}

static void
bench_heap_profile(bench_t *bench) {
  ldb_wfile_t *file = NULL;
  char path[LDB_PATH_MAX];
  char name[100];
  int rc = LDB_OK;
  int ok;

  sprintf(name, "heap-%04d", ++bench->heap_counter);

  if (!ldb_join(path, sizeof(path), FLAGS_db, name))
    rc = LDB_INVALID;

  if (rc == LDB_OK)
    rc = ldb_truncfile_create(path, &file);

  if (rc != LDB_OK) {
    fprintf(stderr, "%s\n", ldb_strerror(rc));
    return;
  }

  ok = ldb_heap_profile(write_to_file, file);

  ldb_wfile_destroy(file);

  if (!ok) {
    fprintf(stderr, "heap profiling not supported\n");
    ldb_remove_file(path);
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
    void (*method)(bench_t *, thread_state_t *);
    int fresh_db, num_threads;

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
    bench->value_size = FLAGS_value_size;
    bench->entries_per_batch = 1;
    bench->write_options = *ldb_writeopt_default;

    method = NULL;
    fresh_db = 0;
    num_threads = FLAGS_threads;

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
      bench->write_options.sync = 1;
      method = &bench_write_random;
    } else if (strcmp(name, "fill100K") == 0) {
      fresh_db = 1;
      bench->num /= 1000;
      bench->value_size = 100 * 1000;
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
#if defined(_WIN32) || defined(LDB_PTHREAD)
    } else if (strcmp(name, "readwhilewriting") == 0) {
      num_threads++; /* Add extra thread for writing. */
      method = &bench_read_while_writing;
#endif
    } else if (strcmp(name, "compact") == 0) {
      method = &bench_compact;
    } else if (strcmp(name, "crc32c") == 0) {
      method = &bench_crc32c;
    } else if (strcmp(name, "snappycomp") == 0) {
      method = &bench_snappy_compress;
    } else if (strcmp(name, "snappyuncomp") == 0) {
      method = &bench_snappy_uncompress;
    } else if (strcmp(name, "heapprofile") == 0) {
      bench_heap_profile(bench);
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
        if (bench->db != NULL)
          ldb_close(bench->db);

        bench->db = NULL;

        ldb_destroy(FLAGS_db, ldb_dbopt_default);

        bench_open(bench);
      }
    }

    if (method != NULL)
      run_benchmark(bench, num_threads, name, method);
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
    } else if (sscanf(argv[i], "--comparisons=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_comparisons = n;
    } else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_use_existing_db = n;
    } else if (sscanf(argv[i], "--reuse_logs=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_reuse_logs = n;
    } else if (sscanf(argv[i], "--compression=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_compression = n;
    } else if (sscanf(argv[i], "--use_mmap=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_use_mmap = n;
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
      FLAGS_reads = n;
    } else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1) {
      FLAGS_threads = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1) {
      FLAGS_write_buffer_size = n;
    } else if (sscanf(argv[i], "--max_file_size=%d%c", &n, &junk) == 1) {
      FLAGS_max_file_size = n;
    } else if (sscanf(argv[i], "--block_size=%d%c", &n, &junk) == 1) {
      FLAGS_block_size = n;
    } else if (sscanf(argv[i], "--key_prefix=%d%c", &n, &junk) == 1) {
      FLAGS_key_prefix = n;
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
    if (!ldb_test_filename(db_path, sizeof(db_path), "dbbench"))
      return 1;

    FLAGS_db = db_path;
  }

  bench_init(&bench);
  bench_run(&bench);
  bench_clear(&bench);

  return 0;
}
