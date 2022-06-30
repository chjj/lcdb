/*!
 * t-tar.c - tar test for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 */

#include <stdlib.h>
#include <string.h>

#include "util/buffer.h"
#include "util/env.h"
#include "util/random.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/strutil.h"
#include "util/tar.h"
#include "util/testutil.h"

/*
 * Helpers
 */

#undef P
#define P test_join

static const char *
test_join(const char *xp, const char *yp) {
  static char path[LDB_PATH_MAX];
  ASSERT(ldb_join(path, sizeof(path), xp, yp));
  return path;
}

static void
test_write_random(const char *fname, ldb_rand_t *rnd, size_t len) {
  ldb_buffer_t data;

  ldb_buffer_init(&data);

  ldb_random_string(&data, rnd, len);

  ASSERT(ldb_write_file(fname, &data, 0) == LDB_OK);

  ldb_buffer_clear(&data);
}

static void
test_compare_files(const char *R1, const char *R2, const char *name) {
  ldb_buffer_t x, y;

  ldb_buffer_init(&x);
  ldb_buffer_init(&y);

  ASSERT(ldb_read_file(P(R1, name), &x) == LDB_OK);
  ASSERT(ldb_read_file(P(R2, name), &y) == LDB_OK);
  ASSERT(ldb_slice_equal(&x, &y));

  ldb_buffer_clear(&x);
  ldb_buffer_clear(&y);
}

static void
test_cleanup(const char *R1, const char *R2) {
  ldb_remove_file(P(R1, "test.tar"));
  ldb_remove_file(P(R1, "foo.txt"));
  ldb_remove_file(P(R1, "bar.txt"));
  ldb_remove_file(P(R1, "baz.txt"));
  ldb_remove_file(P(R1, "big.txt"));
  ldb_remove_file(P(R2, "foo.txt"));
  ldb_remove_file(P(R2, "bar.txt"));
  ldb_remove_file(P(R2, "baz.txt"));
  ldb_remove_file(P(R2, "big.txt"));

  ldb_remove_dir(R1);
  ldb_remove_dir(R2);

  ASSERT(!ldb_file_exists(R1));
  ASSERT(!ldb_file_exists(R2));
}

/*
 * Main
 */

int
main(void) {
  char R1[LDB_PATH_MAX];
  char R2[LDB_PATH_MAX];
  ldb_wfile_t *file;
  ldb_rand_t rnd;
  ldb_tar_t tar;

  ldb_rand_init(&rnd, 301);

  ASSERT(ldb_test_filename(R1, sizeof(R1), "archived"));
  ASSERT(ldb_test_filename(R2, sizeof(R2), "extracted"));

  test_cleanup(R1, R2);

  ASSERT(ldb_create_dir(R1) == LDB_OK);

  test_write_random(P(R1, "foo.txt"), &rnd, 73);
  test_write_random(P(R1, "bar.txt"), &rnd, 971);
  test_write_random(P(R1, "baz.txt"), &rnd, 3119);
  test_write_random(P(R1, "big.txt"), &rnd, (4 << 20) + 17);

  ASSERT(ldb_truncfile_create(P(R1, "test.tar"), &file) == LDB_OK);

  ldb_tar_init(&tar, file);

  ASSERT(ldb_tar_append(&tar, "foo.txt", P(R1, "foo.txt")) == LDB_OK);
  ASSERT(ldb_tar_append(&tar, "bar.txt", P(R1, "bar.txt")) == LDB_OK);
  ASSERT(ldb_tar_append(&tar, "baz.txt", P(R1, "baz.txt")) == LDB_OK);
  ASSERT(ldb_tar_append(&tar, "big.txt", P(R1, "big.txt")) == LDB_OK);
  ASSERT(ldb_tar_finish(&tar) == LDB_OK);

  ASSERT(ldb_wfile_close(file) == LDB_OK);

  ldb_tar_clear(&tar);
  ldb_wfile_destroy(file);

  ASSERT(ldb_tar_extract(P(R1, "test.tar"), R2, 0, NULL) == LDB_OK);

  test_compare_files(R1, R2, "foo.txt");
  test_compare_files(R1, R2, "bar.txt");
  test_compare_files(R1, R2, "baz.txt");
  test_compare_files(R1, R2, "big.txt");

  test_cleanup(R1, R2);

  return 0;
}
