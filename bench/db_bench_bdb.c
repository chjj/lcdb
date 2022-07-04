/*!
 * db_bench_bdb.c - berkeley db benchmarks for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Ported from:
 *   http://www.lmdb.tech/bench/microbench/db_bench_bdb.cc
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

#include <db.h>

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
 *   fillseqbatch  -- batch write N values in sequential key order in async mode
 *   fillrandsync  -- write N/100 values in random key order in sync mode
 *   fillrandbatch  -- batch write N values in random key order in async mode
 *   fillrand100K  -- write N/1000 100K values in random order in async mode
 *   fillseq100K   -- write N/1000 100K values in seq order in async mode
 *   readseq       -- read N times sequentially
 *   readreverse   -- read N times in reverse order
 *   readseq100K   -- read N/1000 100K values in sequential order in async mode
 *   readrand100K  -- read N/1000 100K values in sequential order in async mode
 *   readrandom    -- read N times in random order
 */
static const char *FLAGS_benchmarks =
    "fillseqsync,"
    "fillrandsync,"
    "fillseq,"
    "fillseqbatch,"
    "fillrandom,"
    "fillrandbatch,"
    "overwrite,"
#if 0
    "overwritebatch,"
#endif
    "readrandom,"
    "readseq,"
    "readreverse,"
#if 0
    "fillrand100K,"
    "fillseq100K,"
    "readseq100K,"
    "readrand100K,"
#endif
    ;

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

/* If true, do not destroy the existing database. If you set this
   flag and also specify a benchmark that wants a fresh database, that
   benchmark will fail. */
static int FLAGS_use_existing_db = 0;

/* Use the db with the following name. */
static const char *FLAGS_db = NULL;

/*
 * Helpers
 */

static void
db_destroy(const char *dbname) {
  char path[LDB_PATH_MAX];
  char **files;
  int i, len;

  len = ldb_get_children(dbname, &files);

  for (i = 0; i < len; i++) {
    if (ldb_join(path, sizeof(path), dbname, files[i]))
      ldb_remove_file(path);
  }

  ldb_free_children(files, len);
  ldb_remove_dir(dbname);
}

static void
db_destroy_all(const char *root) {
  char path[LDB_PATH_MAX];
  char **files;
  int i, len;

  len = ldb_get_children(root, &files);

  for (i = 0; i < len; i++) {
    if (ldb_starts_with(files[i], "dbbench_bdb") &&
        ldb_join(path, sizeof(path), root, files[i])) {
      db_destroy(path);
    }
  }

  ldb_free_children(files, len);
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
enum bench_flags { NONE = 0, SYNC, INT };

typedef struct bench_s {
  DB_ENV *env;
  DB *db;
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
      if (ldb_starts_with(files[i], "dbbench_bdb") &&
          ldb_join(path, sizeof(path), FLAGS_db, files[i])) {
        db_destroy(path);
      }
    }

    if (len >= 0)
      ldb_free_children(files, len);
  }
}

static void
bench_clear(bench_t *bench) {
  if (bench->db != NULL)
    bench->db->close(bench->db, 0);

  if (bench->env != NULL)
    bench->env->close(bench->env, 0);

  stats_clear(&bench->stats);
  rng_clear(&bench->gen);
}

static void
bench_open(bench_t *bench, enum bench_flags db_flags) {
  DB_ENV *env = bench->env;
  char path[LDB_PATH_MAX];
  DB *db = bench->db;
  char name[100];
  int rc;

  assert(env == NULL);
  assert(db == NULL);

  bench->db_num++;

  sprintf(name, "dbbench_bdb-%d", bench->db_num);

  if (!ldb_join(path, sizeof(path), FLAGS_db, name))
    abort(); /* LCOV_EXCL_LINE */

  ldb_create_dir(path);

  /* Create tuning options and open the database. */
  rc = db_env_create(&env, 0);

  if (rc == 0)
    rc = env->set_cachesize(env, 0, FLAGS_cache_size, 1);

  if (rc == 0)
    rc = env->set_lk_max_locks(env, 100000);

  if (rc == 0)
    rc = env->set_lk_max_objects(env, 100000);

  if (rc == 0) {
    int flags = DB_REGION_INIT;

    if (db_flags != SYNC)
      flags |= DB_TXN_WRITE_NOSYNC;

    rc = env->set_flags(env, flags, 1);
  }

  if (rc == 0)
    rc = env->log_set_config(env, DB_LOG_AUTO_REMOVE, 1);

  if (rc == 0) {
    int flags = DB_INIT_LOCK | DB_INIT_LOG | DB_INIT_TXN |
                DB_INIT_MPOOL | DB_CREATE | DB_THREAD;

    rc = env->open(env, path, flags, 0664);
  }

  if (rc == 0)
    rc = db_create(&db, env, 0);

  if (rc == 0) {
    int flags = DB_AUTO_COMMIT | DB_CREATE | DB_THREAD;

    rc = db->open(db, NULL, "data.bdb", NULL, DB_BTREE, flags, 0664);
  }

  if (rc != 0) {
    fprintf(stderr, "open error: %s\n", db_strerror(rc));

    if (db != NULL)
      db->close(db, 0);

    if (env != NULL)
      env->close(env, 0);

    exit(1);
  }

  bench->env = env;
  bench->db = db;
}

static DB *
bench_reopen(bench_t *bench, enum bench_flags db_flags) {
  if (bench->db != NULL)
    bench->db->close(bench->db, 0);

  if (bench->env != NULL)
    bench->env->close(bench->env, 0);

  /* rm -rf ${FLAGS_db}* */
  db_destroy_all(FLAGS_db);

  bench->env = NULL;
  bench->db = NULL;

  bench_open(bench, db_flags);

  return bench->db;
}

static void
bench_print_environment(void) {
#ifdef __linux__
  time_t now = time(NULL);
  FILE *cpuinfo;
#endif

  fprintf(stderr, "BerkeleyDB: version %s\n", DB_VERSION_STRING);

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
  const int key_size = 16;

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
bench_write(bench_t *bench,
            enum bench_flags flags,
            enum bench_order order,
            enum bench_state state,
            int num_entries,
            int value_size,
            int entries_per_batch) {
  DB_ENV *env = bench->env;
  DB *db = bench->db;
  int i, j, rc;
  DB_TXN *txn;

  /* Create new database if state == FRESH. */
  if (state == FRESH) {
    if (FLAGS_use_existing_db) {
      stats_add_message(&bench->stats, "skipping (--use_existing_db is true)");
      return;
    }

    db = bench_reopen(bench, flags);
    env = bench->env;

    stats_start(&bench->stats); /* Do not count time taken to destroy/open. */
  } else {
    env->txn_checkpoint(env, 0, 0, DB_FORCE);
  }

  if (num_entries != bench->num) {
    char msg[100];
    sprintf(msg, "(%d ops)", num_entries);
    stats_add_message(&bench->stats, msg);
  }

  /* Write to database. */
  for (i = 0; i < num_entries; i += entries_per_batch) {
    env->txn_begin(env, NULL, &txn, 0);

    for (j = 0; j < entries_per_batch; j++) {
      uint32_t next = ldb_rand_next(&bench->rnd);
      const int k = (order == SEQUENTIAL) ? (i + j) : (int)(next % num_entries);
      const char *value = rng_generate(&bench->gen, value_size);
      DBT mkey, mval;
      char key[100];

      mkey.data = key;
      mkey.size = snprintf(key, sizeof(key), "%016d", k);
      mkey.flags = 0;

      mval.data = (void *)value;
      mval.size = value_size;
      mval.flags = 0;

      rc = db->put(db, txn, &mkey, &mval, 0);

      if (rc != 0)
        fprintf(stderr, "set error: %s\n", db_strerror(rc));

      stats_add_bytes(&bench->stats, 16 + value_size);
      stats_finished_single_op(&bench->stats);
    }

    txn->commit(txn, 0);
  }
}

static void
bench_read_sequential(bench_t *bench) {
  DB_ENV *env = bench->env;
  DB *db = bench->db;
  char msg[100];
  int found = 0;
  DBT key, data;
  DB_TXN *txn;
  DBC *cursor;

  key.flags = 0;
  data.flags = 0;

  env->txn_begin(env, NULL, &txn, 0);
  db->cursor(db, txn, &cursor, 0);

  while (cursor->get(cursor, &key, &data, DB_NEXT) == 0) {
    stats_add_bytes(&bench->stats, key.size + data.size);
    stats_finished_single_op(&bench->stats);
    found++;
  }

  cursor->close(cursor);
  txn->abort(txn);

  sprintf(msg, "(%d of %d found)", found, bench->num);
  stats_add_message(&bench->stats, msg);
}

static void
bench_read_reverse(bench_t *bench) {
  DB_ENV *env = bench->env;
  DB *db = bench->db;
  char msg[100];
  int found = 0;
  DBT key, data;
  DB_TXN *txn;
  DBC *cursor;

  key.flags = 0;
  data.flags = 0;

  env->txn_begin(env, NULL, &txn, 0);
  db->cursor(db, txn, &cursor, 0);

  while (cursor->get(cursor, &key, &data, DB_PREV) == 0) {
    stats_add_bytes(&bench->stats, key.size + data.size);
    stats_finished_single_op(&bench->stats);
    found++;
  }

  cursor->close(cursor);
  txn->abort(txn);

  sprintf(msg, "(%d of %d found)", found, bench->num);
  stats_add_message(&bench->stats, msg);
}

static void
bench_read_random(bench_t *bench) {
  DB_ENV *env = bench->env;
  DB *db = bench->db;
  char msg[100];
  int found = 0;
  DBT key, data;
  DB_TXN *txn;
  DBC *cursor;
  int i;

  key.flags = 0;
  data.flags = 0;

  env->txn_begin(env, NULL, &txn, 0);
  db->cursor(db, txn, &cursor, 0);

  for (i = 0; i < bench->reads; i++) {
    const int k = ldb_rand_next(&bench->rnd) % bench->reads;
    char ckey[100];

    key.data = ckey;
    key.size = sprintf(ckey, "%016d", k);

    if (cursor->get(cursor, &key, &data, DB_SET) == 0)
      found++;
    else
      data.size = 0;

    stats_add_bytes(&bench->stats, 16 + data.size);
    stats_finished_single_op(&bench->stats);
  }

  cursor->close(cursor);
  txn->abort(txn);

  sprintf(msg, "(%d of %d found)", found, bench->num);
  stats_add_message(&bench->stats, msg);
}

static void
bench_run(bench_t *bench) {
  const char *benchmarks = FLAGS_benchmarks;
  char name[128];

  bench_print_header(bench);
  bench_open(bench, NONE);

  while (benchmarks != NULL) {
    const char *sep = strchr(benchmarks, ',');
    enum bench_flags flags;
    int known;

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

    bench->num = FLAGS_num;

    stats_start(&bench->stats);

    known = 1;
    flags = NONE;

    if (strcmp(name, "fillseq") == 0) {
      bench_write(bench, flags, SEQUENTIAL, FRESH,
                  bench->num, FLAGS_value_size, 1);
    } else if (strcmp(name, "fillseqbatch") == 0) {
      bench_write(bench, flags, SEQUENTIAL, FRESH,
                  bench->num, FLAGS_value_size, 1000);
    } else if (strcmp(name, "fillrandom") == 0) {
      bench_write(bench, flags, RANDOM, FRESH,
                  bench->num, FLAGS_value_size, 1);
    } else if (strcmp(name, "fillrandbatch") == 0) {
      bench_write(bench, flags, RANDOM, FRESH,
                  bench->num, FLAGS_value_size, 1000);
    } else if (strcmp(name, "overwrite") == 0) {
      bench_write(bench, flags, RANDOM, EXISTING,
                  bench->num, FLAGS_value_size, 1);
    } else if (strcmp(name, "overwritebatch") == 0) {
      bench_write(bench, flags, RANDOM, EXISTING,
                  bench->num, FLAGS_value_size, 1000);
    } else if (strcmp(name, "fillrandsync") == 0) {
      flags = SYNC;
#if 1
      bench->num /= 1000;
      if (bench->num < 10)
        bench->num = 10;
#endif
      bench_write(bench, flags, RANDOM, FRESH,
                  bench->num, FLAGS_value_size, 1);
    } else if (strcmp(name, "fillseqsync") == 0) {
      flags = SYNC;
#if 1
      bench->num /= 1000;
      if (bench->num < 10)
        bench->num = 10;
#endif
      bench_write(bench, flags, SEQUENTIAL, FRESH,
                  bench->num, FLAGS_value_size, 1);
    } else if (strcmp(name, "fillrand100K") == 0) {
      bench_write(bench, flags, RANDOM, FRESH,
                  bench->num / 1000, 100 * 1000, 1);
    } else if (strcmp(name, "fillseq100K") == 0) {
      bench_write(bench, flags, SEQUENTIAL, FRESH,
                  bench->num / 1000, 100 * 1000, 1);
    } else if (strcmp(name, "readseq") == 0) {
      bench_read_sequential(bench);
    } else if (strcmp(name, "readreverse") == 0) {
      bench_read_reverse(bench);
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
