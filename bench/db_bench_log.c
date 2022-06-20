/*!
 * db_bench_log.c - version set log benchmarks for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on google/leveldb:
 *   Copyright (c) 2011, The LevelDB Authors. All rights reserved.
 *   https://github.com/google/leveldb
 *
 * See LICENSE for more information.
 */

#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "util/comparator.h"
#include "util/env.h"
#include "util/internal.h"
#include "util/options.h"
#include "util/port.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/strutil.h"
#include "util/testutil.h"

#include "db_impl.h"
#include "version_edit.h"
#include "version_set.h"

/*
 * Helpers
 */

static ldb_slice_t
make_key(unsigned int num, char *buf) {
  sprintf(buf, "%016u", num);
  return ldb_string(buf);
}

/*
 * Benchmark
 */

static void
bench_apply(int range, int iterations) {
  const int num_base_files = range;
  char dbname[LDB_PATH_MAX];
  ldb_ikey_t start, limit;
  uint64_t start_micros;
  uint64_t stop_micros;
  ldb_versions_t *vset;
  ldb_comparator_t cmp;
  int save_manifest;
  ldb_dbopt_t opts;
  ldb_edit_t vbase;
  ldb_t *db = NULL;
  unsigned int us;
  ldb_mutex_t mu;
  uint64_t fnum;
  char buf[16];
  int i, rc;

  ldb_mutex_init(&mu);
  ldb_ikey_init(&start);
  ldb_ikey_init(&limit);
  ldb_edit_init(&vbase);

  ASSERT(ldb_test_filename(dbname, sizeof(dbname), "leveldb_test_benchmark"));

  ldb_destroy(dbname, NULL);

  opts = *ldb_dbopt_default;
  opts.create_if_missing = 1;

  rc = ldb_open(dbname, &opts, &db);

  ASSERT(rc == LDB_OK);
  ASSERT(db != NULL);

  ldb_close(db);
  db = NULL;

  ldb_mutex_lock(&mu);

  ldb_ikc_init(&cmp, ldb_bytewise_comparator);

  vset = ldb_versions_create(dbname, &opts, NULL, &cmp);

  ASSERT(ldb_versions_recover(vset, &save_manifest) == LDB_OK);

  fnum = 1;

  for (i = 0; i < num_base_files; i++) {
    char buf1[30], buf2[30];
    ldb_slice_t k1, k2;

    k1 = make_key(2 * fnum + 0, buf1);
    k2 = make_key(2 * fnum + 1, buf2);

    ldb_ikey_set(&start, &k1, 1, LDB_TYPE_VALUE);
    ldb_ikey_set(&limit, &k2, 1, LDB_TYPE_DELETION);

    ldb_edit_add_file(&vbase, 2, fnum++, 1 /* file size */, &start, &limit);
  }

  ASSERT(ldb_versions_apply(vset, &vbase, &mu) == LDB_OK);

  start_micros = ldb_now_usec();

  for (i = 0; i < iterations; i++) {
    char buf1[30], buf2[30];
    ldb_slice_t k1, k2;
    ldb_edit_t vedit;

    ldb_edit_init(&vedit);
    ldb_edit_remove_file(&vedit, 2, fnum);

    k1 = make_key(2 * fnum + 0, buf1);
    k2 = make_key(2 * fnum + 1, buf2);

    ldb_ikey_set(&start, &k1, 1, LDB_TYPE_VALUE);
    ldb_ikey_set(&limit, &k2, 1, LDB_TYPE_DELETION);

    ldb_edit_add_file(&vedit, 2, fnum++, 1 /* file size */, &start, &limit);

    ldb_versions_apply(vset, &vedit, &mu);

    ldb_edit_clear(&vedit);
  }

  stop_micros = ldb_now_usec();
  us = stop_micros - start_micros;

  sprintf(buf, "%d", num_base_files);
  fprintf(stderr, "bench_apply/%-6s   %8d"
                  " iters : %9u us (%7.0f us / iter)\n",
                  buf, iterations, us, ((float)us) / iterations);

  ldb_mutex_unlock(&mu);

  ldb_versions_destroy(vset);
  ldb_edit_clear(&vbase);
  ldb_ikey_clear(&limit);
  ldb_ikey_clear(&start);
  ldb_mutex_destroy(&mu);
}

/*
 * Main
 */

int
main(int argc, char **argv) {
  int iter = argc < 2 ? 1024 : atoi(argv[1]);

  bench_apply(1, iter);
  bench_apply(100, iter);
  bench_apply(10000, iter);
  bench_apply(100000, iter);

  return 0;
}
