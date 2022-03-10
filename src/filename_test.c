/*!
 * filename_test.c - filename test for lcdb
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

#include "util/extern.h"
#include "util/internal.h"
#include "util/strutil.h"
#include "util/testutil.h"

#include "dbformat.h"
#include "filename.h"

static void
test_parse(void) {
  ldb_filetype_t type;
  uint64_t number;
  size_t i;

  /* Successful parses. */
  static struct {
    const char *fname;
    uint64_t number;
    ldb_filetype_t type;
  } cases[] = {
    {"100.log", 100, LDB_FILE_LOG},
    {"0.log", 0, LDB_FILE_LOG},
    {"0.sst", 0, LDB_FILE_TABLE},
    {"0.ldb", 0, LDB_FILE_TABLE},
    {"CURRENT", 0, LDB_FILE_CURRENT},
    {"LOCK", 0, LDB_FILE_LOCK},
    {"MANIFEST-2", 2, LDB_FILE_DESC},
    {"MANIFEST-7", 7, LDB_FILE_DESC},
    {"LOG", 0, LDB_FILE_INFO},
    {"LOG.old", 0, LDB_FILE_INFO},
    {"18446744073709551615.log", UINT64_C(18446744073709551615), LDB_FILE_LOG},
  };

  /* Errors. */
  static const char *errors[] = {"",
                                 "foo",
                                 "foo-dx-100.log",
                                 ".log",
                                 "",
                                 "manifest",
                                 "CURREN",
                                 "CURRENTX",
                                 "MANIFES",
                                 "MANIFEST",
                                 "MANIFEST-",
                                 "XMANIFEST-3",
                                 "MANIFEST-3x",
                                 "LOC",
                                 "LOCKx",
                                 "LO",
                                 "LOGx",
                                 "18446744073709551616.log",
                                 "184467440737095516150.log",
                                 "100",
                                 "100.",
                                 "100.lop"};

  for (i = 0; i < lengthof(cases); i++) {
    const char *f = cases[i].fname;

    ASSERT(ldb_parse_filename(&type, &number, f));
    ASSERT(cases[i].type == type);
    ASSERT(cases[i].number == number);
  }

  for (i = 0; i < lengthof(errors); i++) {
    const char *f = errors[i];

    ASSERT(!ldb_parse_filename(&type, &number, f));
  }
}

static void
test_construction(void) {
  ldb_filetype_t type;
  uint64_t number;
  char fname[1024];

#ifdef _WIN32
#  define S "\\"
#else
#  define S "/"
#endif

  ASSERT(ldb_current_filename(fname, sizeof(fname), "foo"));
  ASSERT(ldb_starts_with(fname, "foo" S));
  ASSERT(ldb_parse_filename(&type, &number, fname + 4));
  ASSERT(0 == number);
  ASSERT(LDB_FILE_CURRENT == type);

  ASSERT(ldb_lock_filename(fname, sizeof(fname), "foo"));
  ASSERT(ldb_starts_with(fname, "foo" S));
  ASSERT(ldb_parse_filename(&type, &number, fname + 4));
  ASSERT(0 == number);
  ASSERT(LDB_FILE_LOCK == type);

  ASSERT(ldb_log_filename(fname, sizeof(fname), "foo", 192));
  ASSERT(ldb_starts_with(fname, "foo" S));
  ASSERT(ldb_parse_filename(&type, &number, fname + 4));
  ASSERT(192 == number);
  ASSERT(LDB_FILE_LOG == type);

  ASSERT(ldb_table_filename(fname, sizeof(fname), "bar", 200));
  ASSERT(ldb_starts_with(fname, "bar" S));
  ASSERT(ldb_parse_filename(&type, &number, fname + 4));
  ASSERT(200 == number);
  ASSERT(LDB_FILE_TABLE == type);

  ASSERT(ldb_desc_filename(fname, sizeof(fname), "bar", 100));
  ASSERT(ldb_starts_with(fname, "bar" S));
  ASSERT(ldb_parse_filename(&type, &number, fname + 4));
  ASSERT(100 == number);
  ASSERT(LDB_FILE_DESC == type);

  ASSERT(ldb_temp_filename(fname, sizeof(fname), "tmp", 999));
  ASSERT(ldb_starts_with(fname, "tmp" S));
  ASSERT(ldb_parse_filename(&type, &number, fname + 4));
  ASSERT(999 == number);
  ASSERT(LDB_FILE_TEMP == type);

  ASSERT(ldb_info_filename(fname, sizeof(fname), "foo"));
  ASSERT(ldb_starts_with(fname, "foo" S));
  ASSERT(ldb_parse_filename(&type, &number, fname + 4));
  ASSERT(0 == number);
  ASSERT(LDB_FILE_INFO == type);

  ASSERT(ldb_oldinfo_filename(fname, sizeof(fname), "foo"));
  ASSERT(ldb_starts_with(fname, "foo" S));
  ASSERT(ldb_parse_filename(&type, &number, fname + 4));
  ASSERT(0 == number);
  ASSERT(LDB_FILE_INFO == type);

#undef S
}

LDB_EXTERN int
ldb_test_filenames(void);

int
ldb_test_filenames(void) {
  test_parse();
  test_construction();
  return 0;
}
