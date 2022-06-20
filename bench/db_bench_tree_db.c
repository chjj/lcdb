/*!
 * db_bench_tree_db.c - kyoto cabinet benchmarks for lcdb
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
#include <kclangc.h>

#include "util/buffer.h"
#include "util/env.h"
#include "util/internal.h"
#include "util/random.h"
#include "util/strutil.h"
#include "util/testutil.h"

#include "histogram.h"

/* Comma-separated list of operations to run in the specified order
 *   Actual benchmarks:
 *
 *   fillseq       -- write N values in sequential key order in async mode
 *   fillrandom    -- write N values in random key order in async mode
 *   overwrite     -- overwrite N values in random key order in async mode
 *   fillseqsync   -- write N/100 values in sequential key order in sync mode
 *   fillrandsync  -- write N/100 values in random key order in sync mode
 *   fillrand100K  -- write N/1000 100K values in random order in async mode
 *   fillseq100K   -- write N/1000 100K values in seq order in async mode
 *   readseq       -- read N times sequentially
 *   readseq100K   -- read N/1000 100K values in sequential order in async mode
 *   readrand100K  -- read N/1000 100K values in sequential order in async mode
 *   readrandom    -- read N times in random order
 */
static const char *FLAGS_benchmarks =
    "fillseq,"
    "fillseqsync,"
    "fillrandsync,"
    "fillrandom,"
    "overwrite,"
    "readrandom,"
    "readseq,"
    "fillrand100K,"
    "fillseq100K,"
    "readseq100K,"
    "readrand100K,";

/* Number of key/values to place in database. */
static int FLAGS_num = 1000000;

/* Number of read operations to do. If negative, do FLAGS_num reads. */
static int FLAGS_reads = -1;

/* Size of each value. */
static int FLAGS_value_size = 100;

/* Arrange to generate values that shrink to this fraction of
   their original size after compression. */
static double FLAGS_compression_ratio = 0.5;

/* Print histogram of operation timings. */
static int FLAGS_histogram = 0;

/* Cache size. Default 4 MB. */
static int FLAGS_cache_size = 4194304;

/* Page size. Default 1 KB. */
static int FLAGS_page_size = 1024;

/* If true, do not destroy the existing database. If you set this
   flag and also specify a benchmark that wants a fresh database, that
   benchmark will fail. */
static int FLAGS_use_existing_db = 0;

/* Compression flag. If true, compression is on. If false, compression
   is off. */
static int FLAGS_compression = 1;

/* Use the db with the following name. */
static const char *FLAGS_db = NULL;

/*
 * Helpers
 */

static void
db_synchronize(KCDB *db) {
  /* Synchronize will flush writes to disk. */
  if (!kcdbsync(db, 1, NULL, NULL))
    fprintf(stderr, "synchronize error: %s\n", kcdbemsg(db));
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
 * Benchmark
 */

enum bench_order { SEQUENTIAL, RANDOM };
enum bench_state { FRESH, EXISTING };

typedef struct bench_s {
  KCDB *db;
  int db_num;
  int num;
  int reads;
  stats_t stats;
  ldb_rand_t rnd;
  rng_t gen;
} bench_t;

static void
bench_init(bench_t *bench) {
  char path[LDB_PATH_MAX];
  char **files;
  int i, len;

  bench->db = NULL;
  bench->db_num = 0;
  bench->num = FLAGS_num;
  bench->reads = FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads;

  stats_init(&bench->stats);

  ldb_rand_init(&bench->rnd, 301);

  rng_init(&bench->gen);

  if (!FLAGS_use_existing_db) {
    len = ldb_get_children(FLAGS_db, &files);

    for (i = 0; i < len; i++) {
      if (ldb_starts_with(files[i], "dbbench_polyDB") &&
          ldb_join(path, sizeof(path), FLAGS_db, files[i])) {
        ldb_remove_file(path);
      }
    }

    if (len >= 0)
      ldb_free_children(files, len);
  }
}

static void
bench_clear(bench_t *bench) {
  if (bench->db != NULL) {
    if (!kcdbclose(bench->db))
      fprintf(stderr, "close error: %s\n", kcdbemsg(bench->db));

    kcdbdel(bench->db);
  }

  stats_clear(&bench->stats);
  rng_clear(&bench->gen);
}

static void
bench_open(bench_t *bench, int autosync) {
  int mode = KCOWRITER | KCOCREATE;
  char tune[128], name[256];
  char path[1024 + 256];

  assert(bench->db == NULL);

  bench->db = kcdbnew();
  bench->db_num++;

  /* Create tuning options and open the database. */
  sprintf(tune, "%s#opts=%s#pccap=%d#psiz=%d#msiz=%d",
          FLAGS_compression ? "#zcomp=lzo" : "", /* tune_compressor(LZO) */
          FLAGS_compression ? "slc" /* tune_options(TSMALL|TLINEAR|TCOMPRESS) */
                            : "sl", /* tune_options(TSMALL|TLINEAR) */
          FLAGS_cache_size, /* tune_page_cache(FLAGS_cache_size) */
          FLAGS_page_size, /* tune_page(FLAGS_page_size) */
          (256 << 20)); /* tune_map(256 << 20) */

  sprintf(name, "dbbench_polyDB-%d.kct%s", bench->db_num, tune);

  if (!ldb_join(path, sizeof(path), FLAGS_db, name))
    abort(); /* LCOV_EXCL_LINE */

  if (autosync)
    mode |= KCOAUTOSYNC;

  if (!kcdbopen(bench->db, path, mode)) {
    fprintf(stderr, "open error: %s\n", kcdbemsg(bench->db));
    exit(1);
  }
}

static KCDB *
bench_reopen(bench_t *bench, int autosync) {
  assert(bench->db != NULL);

  if (!kcdbclose(bench->db)) {
    fprintf(stderr, "close error: %s\n", kcdbemsg(bench->db));
    exit(1);
  }

  kcdbdel(bench->db);

  bench->db = NULL;

  bench_open(bench, autosync);

  return bench->db;
}

static void
bench_print_environment(void) {
#ifdef __linux__
  time_t now = time(NULL);
  FILE *cpuinfo;
#endif

  fprintf(stderr, "Kyoto Cabinet: version %s\n", KCVERSION);

#ifdef __linux__
  fprintf(stderr, "Date:          %s", ctime(&now)); /* ctime() adds newline. */

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

    fprintf(stderr, "CPU:           %d * %s\n", num_cpus, cpu_type);
    fprintf(stderr, "CPUCache:      %s\n", cache_size);
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
  const int key_size = 16;

  bench_print_environment();

  fprintf(stdout, "Keys:          %d bytes each\n", key_size);
  fprintf(stdout, "Values:        %d bytes each (%d bytes after compression)\n",
                  FLAGS_value_size,
                  (int)(FLAGS_value_size * FLAGS_compression_ratio + 0.5));

  fprintf(stdout, "Entries:       %d\n", bench->num);
  fprintf(stdout, "RawSize:       %.1f MB (estimated)\n",
          ((int64_t)(key_size + FLAGS_value_size) * bench->num) / 1048576.0);

  fprintf(stdout, "FileSize:      %.1f MB (estimated)\n",
    ((key_size + FLAGS_value_size * FLAGS_compression_ratio) * bench->num)
    / 1048576.0);

  bench_print_warnings();

  fprintf(stdout, "------------------------------------------------\n");
}

static void
bench_write(bench_t *bench,
            int autosync,
            enum bench_order order,
            enum bench_state state,
            int num_entries,
            int value_size,
            int entries_per_batch) {
  KCDB *db = bench->db;
  int i;

  (void)entries_per_batch;

  /* Create new database if state == FRESH. */
  if (state == FRESH) {
    if (FLAGS_use_existing_db) {
      stats_add_message(&bench->stats, "skipping (--use_existing_db is true)");
      return;
    }

    db = bench_reopen(bench, autosync);

    stats_start(&bench->stats); /* Do not count time taken to destroy/open. */
  }

  if (num_entries != bench->num) {
    char msg[100];
    sprintf(msg, "(%d ops)", num_entries);
    stats_add_message(&bench->stats, msg);
  }

  /* Write to database. */
  for (i = 0; i < num_entries; i++) {
    uint32_t next = ldb_rand_next(&bench->rnd);
    const int k = (order == SEQUENTIAL) ? i : (int)(next % num_entries);
    const char *value = rng_generate(&bench->gen, value_size);
    char key[100];

    sprintf(key, "%016d", k);

    if (!kcdbset(db, key, 16, value, value_size))
      fprintf(stderr, "set error: %s\n", kcdbemsg(db));

    stats_add_bytes(&bench->stats, 16 + value_size);
    stats_finished_single_op(&bench->stats);
  }
}

static void
bench_read_sequential(bench_t *bench) {
  KCDB *db = bench->db;
  KCCUR *cur = kcdbcursor(db);
  size_t ksize, vsize;
  const char *val;
  int found = 0;
  char msg[100];
  char *key;

  if (!kccurjump(cur)) {
    fprintf(stderr, "jump error: %s\n", kcdbemsg(db));
    exit(1);
  }

  while ((key = kccurget(cur, &ksize, &val, &vsize, 1)) != NULL) {
    stats_add_bytes(&bench->stats, ksize + vsize);
    stats_finished_single_op(&bench->stats);
    kcfree(key);
    found++;
  }

  kccurdel(cur);

  sprintf(msg, "(%d of %d found)", found, bench->num);
  stats_add_message(&bench->stats, msg);
}

static void
bench_read_random(bench_t *bench) {
  KCDB *db = bench->db;
  char val[4096];
  char msg[100];
  int found = 0;
  int i, len;

  for (i = 0; i < bench->reads; i++) {
    const int k = ldb_rand_next(&bench->rnd) % bench->reads;
    char key[100];

    sprintf(key, "%016d", k);

    len = kcdbgetbuf(db, key, 16, val, sizeof(val));

    if (len >= 0)
      found++;
    else
      len = 0;

    stats_add_bytes(&bench->stats, 16 + len);
    stats_finished_single_op(&bench->stats);
  }

  sprintf(msg, "(%d of %d found)", found, bench->num);
  stats_add_message(&bench->stats, msg);
}

static void
bench_run(bench_t *bench) {
  const char *benchmarks = FLAGS_benchmarks;
  char name[128];

  bench_print_header(bench);
  bench_open(bench, 0);

  while (benchmarks != NULL) {
    const char *sep = strchr(benchmarks, ',');
    int known, write_sync;

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

    stats_start(&bench->stats);

    known = 1;
    write_sync = 0;

    if (strcmp(name, "fillseq") == 0) {
      bench_write(bench, write_sync, SEQUENTIAL, FRESH,
                  bench->num, FLAGS_value_size, 1);
      db_synchronize(bench->db);
    } else if (strcmp(name, "fillrandom") == 0) {
      bench_write(bench, write_sync, RANDOM, FRESH,
                  bench->num, FLAGS_value_size, 1);
      db_synchronize(bench->db);
    } else if (strcmp(name, "overwrite") == 0) {
      bench_write(bench, write_sync, RANDOM, EXISTING,
                  bench->num, FLAGS_value_size, 1);
      db_synchronize(bench->db);
    } else if (strcmp(name, "fillrandsync") == 0) {
      write_sync = 1;
      bench_write(bench, write_sync, RANDOM, FRESH,
                  bench->num / 100, FLAGS_value_size, 1);
      db_synchronize(bench->db);
    } else if (strcmp(name, "fillseqsync") == 0) {
      write_sync = 1;
      bench_write(bench, write_sync, SEQUENTIAL, FRESH,
                  bench->num / 100, FLAGS_value_size, 1);
      db_synchronize(bench->db);
    } else if (strcmp(name, "fillrand100K") == 0) {
      bench_write(bench, write_sync, RANDOM, FRESH,
                  bench->num / 1000, 100 * 1000, 1);
      db_synchronize(bench->db);
    } else if (strcmp(name, "fillseq100K") == 0) {
      bench_write(bench, write_sync, SEQUENTIAL, FRESH,
                  bench->num / 1000, 100 * 1000, 1);
      db_synchronize(bench->db);
    } else if (strcmp(name, "readseq") == 0) {
      bench_read_sequential(bench);
    } else if (strcmp(name, "readrandom") == 0) {
      bench_read_random(bench);
    } else if (strcmp(name, "readrand100K") == 0) {
      int n = bench->reads;
      bench->reads /= 1000;
      bench_read_random(bench);
      bench->reads = n;
    } else if (strcmp(name, "readseq100K") == 0) {
      int n = bench->reads;
      bench->reads /= 1000;
      bench_read_sequential(bench);
      bench->reads = n;
    } else {
      if (*name) /* No error message for empty name. */
        fprintf(stderr, "unknown benchmark '%s'\n", name);

      known = 0;
    }

    if (known) {
      stats_stop(&bench->stats);
      stats_report(&bench->stats, name);
    }
  }
}

int
main(int argc, char **argv) {
  char db_path[LDB_PATH_MAX];
  bench_t bench;
  int i;

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
    } else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1) {
      FLAGS_num = n;
    } else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1) {
      FLAGS_reads = n;
    } else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1) {
      FLAGS_value_size = n;
    } else if (sscanf(argv[i], "--cache_size=%d%c", &n, &junk) == 1) {
      FLAGS_cache_size = n;
    } else if (sscanf(argv[i], "--page_size=%d%c", &n, &junk) == 1) {
      FLAGS_page_size = n;
    } else if (sscanf(argv[i], "--compression=%d%c", &n, &junk) == 1 &&
               (n == 0 || n == 1)) {
      FLAGS_compression = n;
    } else if (ldb_starts_with(argv[i], "--db=")) {
      FLAGS_db = argv[i] + 5;
    } else {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  /* Choose a location for the test database if none given with --db=<path>. */
  if (FLAGS_db == NULL) {
    if (!ldb_test_directory(db_path, sizeof(db_path)))
      return 1;

    FLAGS_db = db_path;
  } else {
    ldb_create_dir(FLAGS_db);
  }

  bench_init(&bench);
  bench_run(&bench);
  bench_clear(&bench);

  return 0;
}
