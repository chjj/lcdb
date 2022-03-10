/*!
 * version_set_test.c - version_set test for lcdb
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
#include <stdlib.h>
#include <string.h>

#include "util/buffer.h"
#include "util/comparator.h"
#include "util/extern.h"
#include "util/internal.h"
#include "util/slice.h"
#include "util/testutil.h"
#include "util/vector.h"

#include "dbformat.h"
#include "version_edit.h"
#include "version_set.h"

/*
 * FindFileTest
 */

typedef struct fftest_s {
  int disjoint_sorted_files;
  ldb_vector_t files;
} fftest_t;

static void
fftest_init(fftest_t *t) {
  t->disjoint_sorted_files = 1;

  ldb_vector_init(&t->files);
}

static void
fftest_clear(fftest_t *t) {
  size_t i;

  for (i = 0; i < t->files.length; i++)
    ldb_filemeta_destroy(t->files.items[i]);

  ldb_vector_clear(&t->files);
}

static void
fftest_add4(fftest_t *t,
            const char *smallest,
            const char *largest,
            ldb_seqnum_t smallest_seq,
            ldb_seqnum_t largest_seq) {
  ldb_filemeta_t *f = ldb_filemeta_create();
  ldb_slice_t sk = ldb_string(smallest);
  ldb_slice_t lk = ldb_string(largest);

  f->number = t->files.length + 1;

  ldb_ikey_set(&f->smallest, &sk, smallest_seq, LDB_TYPE_VALUE);
  ldb_ikey_set(&f->largest, &lk, largest_seq, LDB_TYPE_VALUE);

  ldb_vector_push(&t->files, f);
}

static void
fftest_add(fftest_t *t, const char *smallest, const char *largest) {
  fftest_add4(t, smallest, largest, 100, 100);
}

static int
fftest_find(fftest_t *t, const char *key) {
  ldb_slice_t k = ldb_string(key);
  ldb_comparator_t cmp;
  ldb_ikey_t target;
  int r;

  ldb_ikc_init(&cmp, ldb_bytewise_comparator);
  ldb_ikey_init(&target);

  ldb_ikey_set(&target, &k, 100, LDB_TYPE_VALUE);

  r = find_file(&cmp, &t->files, &target);

  ldb_ikey_clear(&target);

  return r;
}

static int
fftest_overlaps(fftest_t *t, const char *smallest, const char *largest) {
  ldb_slice_t sbuf, lbuf;
  ldb_slice_t *s = NULL;
  ldb_slice_t *l = NULL;
  ldb_comparator_t cmp;

  ldb_ikc_init(&cmp, ldb_bytewise_comparator);

  if (smallest != NULL) {
    sbuf = ldb_string(smallest);
    s = &sbuf;
  }

  if (largest != NULL) {
    lbuf = ldb_string(largest);
    l = &lbuf;
  }

  return some_file_overlaps_range(&cmp, t->disjoint_sorted_files,
                                  &t->files, s, l);
}

/*
 * AddBoundaryInputsTest
 */

typedef struct addtest_s {
  ldb_comparator_t icmp;
  ldb_vector_t level_files;
  ldb_vector_t compaction_files;
  ldb_vector_t all_files;
} addtest_t;

static void
addtest_init(addtest_t *t) {
  ldb_ikc_init(&t->icmp, ldb_bytewise_comparator);
  ldb_vector_init(&t->level_files);
  ldb_vector_init(&t->compaction_files);
  ldb_vector_init(&t->all_files);
}

static void
addtest_clear(addtest_t *t) {
  size_t i;

  for (i = 0; i < t->all_files.length; i++)
    ldb_filemeta_destroy(t->all_files.items[i]);

  ldb_vector_clear(&t->level_files);
  ldb_vector_clear(&t->compaction_files);
  ldb_vector_clear(&t->all_files);
}

static ldb_filemeta_t *
addtest_file(addtest_t *t,
            uint64_t number,
            const ldb_ikey_t *smallest,
            const ldb_ikey_t *largest) {
  ldb_filemeta_t *f = ldb_filemeta_create();

  f->number = number;

  ldb_ikey_copy(&f->smallest, smallest);
  ldb_ikey_copy(&f->largest, largest);

  ldb_vector_push(&t->all_files, f);

  return f;
}

/*
 * Find File Tests
 */

static void
test_find_file_empty(fftest_t *t) {
  ASSERT(0 == fftest_find(t, "foo"));
  ASSERT(!fftest_overlaps(t, "a", "z"));
  ASSERT(!fftest_overlaps(t, NULL, "z"));
  ASSERT(!fftest_overlaps(t, "a", NULL));
  ASSERT(!fftest_overlaps(t, NULL, NULL));
}

static void
test_find_file_single(fftest_t *t) {
  fftest_add(t, "p", "q");

  ASSERT(0 == fftest_find(t, "a"));
  ASSERT(0 == fftest_find(t, "p"));
  ASSERT(0 == fftest_find(t, "p1"));
  ASSERT(0 == fftest_find(t, "q"));
  ASSERT(1 == fftest_find(t, "q1"));
  ASSERT(1 == fftest_find(t, "z"));

  ASSERT(!fftest_overlaps(t, "a", "b"));
  ASSERT(!fftest_overlaps(t, "z1", "z2"));
  ASSERT(fftest_overlaps(t, "a", "p"));
  ASSERT(fftest_overlaps(t, "a", "q"));
  ASSERT(fftest_overlaps(t, "a", "z"));
  ASSERT(fftest_overlaps(t, "p", "p1"));
  ASSERT(fftest_overlaps(t, "p", "q"));
  ASSERT(fftest_overlaps(t, "p", "z"));
  ASSERT(fftest_overlaps(t, "p1", "p2"));
  ASSERT(fftest_overlaps(t, "p1", "z"));
  ASSERT(fftest_overlaps(t, "q", "q"));
  ASSERT(fftest_overlaps(t, "q", "q1"));

  ASSERT(!fftest_overlaps(t, NULL, "j"));
  ASSERT(!fftest_overlaps(t, "r", NULL));
  ASSERT(fftest_overlaps(t, NULL, "p"));
  ASSERT(fftest_overlaps(t, NULL, "p1"));
  ASSERT(fftest_overlaps(t, "q", NULL));
  ASSERT(fftest_overlaps(t, NULL, NULL));
}

static void
test_find_file_multiple(fftest_t *t) {
  fftest_add(t, "150", "200");
  fftest_add(t, "200", "250");
  fftest_add(t, "300", "350");
  fftest_add(t, "400", "450");

  ASSERT(0 == fftest_find(t, "100"));
  ASSERT(0 == fftest_find(t, "150"));
  ASSERT(0 == fftest_find(t, "151"));
  ASSERT(0 == fftest_find(t, "199"));
  ASSERT(0 == fftest_find(t, "200"));
  ASSERT(1 == fftest_find(t, "201"));
  ASSERT(1 == fftest_find(t, "249"));
  ASSERT(1 == fftest_find(t, "250"));
  ASSERT(2 == fftest_find(t, "251"));
  ASSERT(2 == fftest_find(t, "299"));
  ASSERT(2 == fftest_find(t, "300"));
  ASSERT(2 == fftest_find(t, "349"));
  ASSERT(2 == fftest_find(t, "350"));
  ASSERT(3 == fftest_find(t, "351"));
  ASSERT(3 == fftest_find(t, "400"));
  ASSERT(3 == fftest_find(t, "450"));
  ASSERT(4 == fftest_find(t, "451"));

  ASSERT(!fftest_overlaps(t, "100", "149"));
  ASSERT(!fftest_overlaps(t, "251", "299"));
  ASSERT(!fftest_overlaps(t, "451", "500"));
  ASSERT(!fftest_overlaps(t, "351", "399"));

  ASSERT(fftest_overlaps(t, "100", "150"));
  ASSERT(fftest_overlaps(t, "100", "200"));
  ASSERT(fftest_overlaps(t, "100", "300"));
  ASSERT(fftest_overlaps(t, "100", "400"));
  ASSERT(fftest_overlaps(t, "100", "500"));
  ASSERT(fftest_overlaps(t, "375", "400"));
  ASSERT(fftest_overlaps(t, "450", "450"));
  ASSERT(fftest_overlaps(t, "450", "500"));
}

static void
test_find_file_multiple_null_boundaries(fftest_t *t) {
  fftest_add(t, "150", "200");
  fftest_add(t, "200", "250");
  fftest_add(t, "300", "350");
  fftest_add(t, "400", "450");

  ASSERT(!fftest_overlaps(t, NULL, "149"));
  ASSERT(!fftest_overlaps(t, "451", NULL));
  ASSERT(fftest_overlaps(t, NULL, NULL));
  ASSERT(fftest_overlaps(t, NULL, "150"));
  ASSERT(fftest_overlaps(t, NULL, "199"));
  ASSERT(fftest_overlaps(t, NULL, "200"));
  ASSERT(fftest_overlaps(t, NULL, "201"));
  ASSERT(fftest_overlaps(t, NULL, "400"));
  ASSERT(fftest_overlaps(t, NULL, "800"));
  ASSERT(fftest_overlaps(t, "100", NULL));
  ASSERT(fftest_overlaps(t, "200", NULL));
  ASSERT(fftest_overlaps(t, "449", NULL));
  ASSERT(fftest_overlaps(t, "450", NULL));
}

static void
test_find_file_overlap_sequence_checks(fftest_t *t) {
  fftest_add4(t, "200", "200", 5000, 3000);

  ASSERT(!fftest_overlaps(t, "199", "199"));
  ASSERT(!fftest_overlaps(t, "201", "300"));
  ASSERT(fftest_overlaps(t, "200", "200"));
  ASSERT(fftest_overlaps(t, "190", "200"));
  ASSERT(fftest_overlaps(t, "200", "210"));
}

static void
test_find_file_overlapping_files(fftest_t *t) {
  fftest_add(t, "150", "600");
  fftest_add(t, "400", "500");

  t->disjoint_sorted_files = 0;

  ASSERT(!fftest_overlaps(t, "100", "149"));
  ASSERT(!fftest_overlaps(t, "601", "700"));
  ASSERT(fftest_overlaps(t, "100", "150"));
  ASSERT(fftest_overlaps(t, "100", "200"));
  ASSERT(fftest_overlaps(t, "100", "300"));
  ASSERT(fftest_overlaps(t, "100", "400"));
  ASSERT(fftest_overlaps(t, "100", "500"));
  ASSERT(fftest_overlaps(t, "375", "400"));
  ASSERT(fftest_overlaps(t, "450", "450"));
  ASSERT(fftest_overlaps(t, "450", "500"));
  ASSERT(fftest_overlaps(t, "450", "700"));
  ASSERT(fftest_overlaps(t, "600", "700"));
}

/*
 * Add Boundary Inputs Tests
 */

static void
test_boundary_empty_file_sets(addtest_t *t) {
  add_boundary_inputs(&t->icmp, &t->level_files, &t->compaction_files);

  ASSERT(t->compaction_files.length == 0);
  ASSERT(t->level_files.length == 0);
}

static void
test_boundary_empty_level_files(addtest_t *t) {
  ldb_slice_t k100 = ldb_string("100");
  ldb_filemeta_t *f1;
  ldb_ikey_t k0, k1;

  ldb_ikey_init(&k0);
  ldb_ikey_init(&k1);

  ldb_ikey_set(&k0, &k100, 2, LDB_TYPE_VALUE);
  ldb_ikey_set(&k1, &k100, 1, LDB_TYPE_VALUE);

  f1 = addtest_file(t, 1, &k0, &k1);

  ldb_vector_push(&t->compaction_files, f1);

  add_boundary_inputs(&t->icmp, &t->level_files, &t->compaction_files);

  ASSERT(1 == t->compaction_files.length);
  ASSERT(f1 == t->compaction_files.items[0]);
  ASSERT(t->level_files.length == 0);

  ldb_ikey_clear(&k0);
  ldb_ikey_clear(&k1);
}

static void
test_boundary_empty_compaction_files(addtest_t *t) {
  ldb_slice_t k100 = ldb_string("100");
  ldb_filemeta_t *f1;
  ldb_ikey_t k0, k1;

  ldb_ikey_init(&k0);
  ldb_ikey_init(&k1);

  ldb_ikey_set(&k0, &k100, 2, LDB_TYPE_VALUE);
  ldb_ikey_set(&k1, &k100, 1, LDB_TYPE_VALUE);

  f1 = addtest_file(t, 1, &k0, &k1);

  ldb_vector_push(&t->level_files, f1);

  add_boundary_inputs(&t->icmp, &t->level_files, &t->compaction_files);

  ASSERT(t->compaction_files.length == 0);
  ASSERT(1 == t->level_files.length);
  ASSERT(f1 == t->level_files.items[0]);

  ldb_ikey_clear(&k0);
  ldb_ikey_clear(&k1);
}

static void
test_boundary_no_boundary_files(addtest_t *t) {
  ldb_slice_t k100 = ldb_string("100");
  ldb_slice_t k200 = ldb_string("200");
  ldb_slice_t k300 = ldb_string("300");
  ldb_filemeta_t *f1, *f2, *f3;
  ldb_ikey_t k0, k1;

  ldb_ikey_init(&k0);
  ldb_ikey_init(&k1);

  ldb_ikey_set(&k0, &k100, 2, LDB_TYPE_VALUE);
  ldb_ikey_set(&k1, &k100, 1, LDB_TYPE_VALUE);

  f1 = addtest_file(t, 1, &k0, &k1);

  ldb_ikey_set(&k0, &k200, 2, LDB_TYPE_VALUE);
  ldb_ikey_set(&k1, &k200, 1, LDB_TYPE_VALUE);

  f2 = addtest_file(t, 1, &k0, &k1);

  ldb_ikey_set(&k0, &k300, 2, LDB_TYPE_VALUE);
  ldb_ikey_set(&k1, &k300, 1, LDB_TYPE_VALUE);

  f3 = addtest_file(t, 1, &k0, &k1);

  ldb_vector_push(&t->level_files, f3);
  ldb_vector_push(&t->level_files, f2);
  ldb_vector_push(&t->level_files, f1);
  ldb_vector_push(&t->compaction_files, f2);
  ldb_vector_push(&t->compaction_files, f3);

  add_boundary_inputs(&t->icmp, &t->level_files, &t->compaction_files);

  ASSERT(2 == t->compaction_files.length);

  ldb_ikey_clear(&k0);
  ldb_ikey_clear(&k1);
}

static void
test_boundary_one_boundary_files(addtest_t *t) {
  ldb_slice_t k100 = ldb_string("100");
  ldb_slice_t k200 = ldb_string("200");
  ldb_slice_t k300 = ldb_string("300");
  ldb_filemeta_t *f1, *f2, *f3;
  ldb_ikey_t k0, k1;

  ldb_ikey_init(&k0);
  ldb_ikey_init(&k1);

  ldb_ikey_set(&k0, &k100, 3, LDB_TYPE_VALUE);
  ldb_ikey_set(&k1, &k100, 2, LDB_TYPE_VALUE);

  f1 = addtest_file(t, 1, &k0, &k1);

  ldb_ikey_set(&k0, &k100, 1, LDB_TYPE_VALUE);
  ldb_ikey_set(&k1, &k200, 3, LDB_TYPE_VALUE);

  f2 = addtest_file(t, 1, &k0, &k1);

  ldb_ikey_set(&k0, &k300, 2, LDB_TYPE_VALUE);
  ldb_ikey_set(&k1, &k300, 1, LDB_TYPE_VALUE);

  f3 = addtest_file(t, 1, &k0, &k1);

  ldb_vector_push(&t->level_files, f3);
  ldb_vector_push(&t->level_files, f2);
  ldb_vector_push(&t->level_files, f1);
  ldb_vector_push(&t->compaction_files, f1);

  add_boundary_inputs(&t->icmp, &t->level_files, &t->compaction_files);

  ASSERT(2 == t->compaction_files.length);
  ASSERT(f1 == t->compaction_files.items[0]);
  ASSERT(f2 == t->compaction_files.items[1]);

  ldb_ikey_clear(&k0);
  ldb_ikey_clear(&k1);
}

static void
test_boundary_two_boundary_files(addtest_t *t) {
  ldb_slice_t k100 = ldb_string("100");
  ldb_slice_t k300 = ldb_string("300");
  ldb_filemeta_t *f1, *f2, *f3;
  ldb_ikey_t k0, k1;

  ldb_ikey_init(&k0);
  ldb_ikey_init(&k1);

  ldb_ikey_set(&k0, &k100, 6, LDB_TYPE_VALUE);
  ldb_ikey_set(&k1, &k100, 5, LDB_TYPE_VALUE);

  f1 = addtest_file(t, 1, &k0, &k1);

  ldb_ikey_set(&k0, &k100, 2, LDB_TYPE_VALUE);
  ldb_ikey_set(&k1, &k300, 1, LDB_TYPE_VALUE);

  f2 = addtest_file(t, 1, &k0, &k1);

  ldb_ikey_set(&k0, &k100, 4, LDB_TYPE_VALUE);
  ldb_ikey_set(&k1, &k100, 3, LDB_TYPE_VALUE);

  f3 = addtest_file(t, 1, &k0, &k1);

  ldb_vector_push(&t->level_files, f2);
  ldb_vector_push(&t->level_files, f3);
  ldb_vector_push(&t->level_files, f1);
  ldb_vector_push(&t->compaction_files, f1);

  add_boundary_inputs(&t->icmp, &t->level_files, &t->compaction_files);

  ASSERT(3 == t->compaction_files.length);
  ASSERT(f1 == t->compaction_files.items[0]);
  ASSERT(f3 == t->compaction_files.items[1]);
  ASSERT(f2 == t->compaction_files.items[2]);

  ldb_ikey_clear(&k0);
  ldb_ikey_clear(&k1);
}

static void
test_boundary_disjoin_file_pointers(addtest_t *t) {
  ldb_slice_t k100 = ldb_string("100");
  ldb_slice_t k300 = ldb_string("300");
  ldb_filemeta_t *f1, *f2, *f3, *f4;
  ldb_ikey_t k0, k1;

  ldb_ikey_init(&k0);
  ldb_ikey_init(&k1);

  ldb_ikey_set(&k0, &k100, 6, LDB_TYPE_VALUE);
  ldb_ikey_set(&k1, &k100, 5, LDB_TYPE_VALUE);

  f1 = addtest_file(t, 1, &k0, &k1);

  ldb_ikey_set(&k0, &k100, 6, LDB_TYPE_VALUE);
  ldb_ikey_set(&k1, &k100, 5, LDB_TYPE_VALUE);

  f2 = addtest_file(t, 1, &k0, &k1);

  ldb_ikey_set(&k0, &k100, 2, LDB_TYPE_VALUE);
  ldb_ikey_set(&k1, &k300, 1, LDB_TYPE_VALUE);

  f3 = addtest_file(t, 1, &k0, &k1);

  ldb_ikey_set(&k0, &k100, 4, LDB_TYPE_VALUE);
  ldb_ikey_set(&k1, &k100, 3, LDB_TYPE_VALUE);

  f4 = addtest_file(t, 1, &k0, &k1);

  ldb_vector_push(&t->level_files, f2);
  ldb_vector_push(&t->level_files, f3);
  ldb_vector_push(&t->level_files, f4);

  ldb_vector_push(&t->compaction_files, f1);

  add_boundary_inputs(&t->icmp, &t->level_files, &t->compaction_files);

  ASSERT(3 == t->compaction_files.length);
  ASSERT(f1 == t->compaction_files.items[0]);
  ASSERT(f4 == t->compaction_files.items[1]);
  ASSERT(f3 == t->compaction_files.items[2]);

  ldb_ikey_clear(&k0);
  ldb_ikey_clear(&k1);
}

/*
 * Execute
 */

LDB_EXTERN int
ldb_test_version_set(void);

int
ldb_test_version_set(void) {
  static void (*fftests[])(fftest_t *) = {
    test_find_file_empty,
    test_find_file_single,
    test_find_file_multiple,
    test_find_file_multiple_null_boundaries,
    test_find_file_overlap_sequence_checks,
    test_find_file_overlapping_files
  };

  static void (*addtests[])(addtest_t *) = {
    test_boundary_empty_file_sets,
    test_boundary_empty_level_files,
    test_boundary_empty_compaction_files,
    test_boundary_no_boundary_files,
    test_boundary_one_boundary_files,
    test_boundary_two_boundary_files,
    test_boundary_disjoin_file_pointers
  };

  size_t i;

  for (i = 0; i < lengthof(fftests); i++) {
    fftest_t t;

    fftest_init(&t);

    fftests[i](&t);

    fftest_clear(&t);
  }

  for (i = 0; i < lengthof(addtests); i++) {
    addtest_t t;

    addtest_init(&t);

    addtests[i](&t);

    addtest_clear(&t);
  }

  return 0;
}
