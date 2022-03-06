/*!
 * filename_test.c - filename test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
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
  rdb_filetype_t type;
  uint64_t number;
  size_t i;

  /* Successful parses. */
  static struct {
    const char *fname;
    uint64_t number;
    rdb_filetype_t type;
  } cases[] = {
    {"100.log", 100, RDB_FILE_LOG},
    {"0.log", 0, RDB_FILE_LOG},
    {"0.sst", 0, RDB_FILE_TABLE},
    {"0.ldb", 0, RDB_FILE_TABLE},
    {"CURRENT", 0, RDB_FILE_CURRENT},
    {"LOCK", 0, RDB_FILE_LOCK},
    {"MANIFEST-2", 2, RDB_FILE_DESC},
    {"MANIFEST-7", 7, RDB_FILE_DESC},
    {"LOG", 0, RDB_FILE_INFO},
    {"LOG.old", 0, RDB_FILE_INFO},
    {"18446744073709551615.log", UINT64_C(18446744073709551615), RDB_FILE_LOG},
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

    ASSERT(rdb_parse_filename(&type, &number, f));
    ASSERT(cases[i].type == type);
    ASSERT(cases[i].number == number);
  }

  for (i = 0; i < lengthof(errors); i++) {
    const char *f = errors[i];

    ASSERT(!rdb_parse_filename(&type, &number, f));
  }
}

static void
test_construction(void) {
  rdb_filetype_t type;
  uint64_t number;
  char fname[1024];

  ASSERT(rdb_current_filename(fname, sizeof(fname), "foo"));
  ASSERT(rdb_starts_with(fname, "foo/"));
  ASSERT(rdb_parse_filename(&type, &number, fname + 4));
  ASSERT(0 == number);
  ASSERT(RDB_FILE_CURRENT == type);

  ASSERT(rdb_lock_filename(fname, sizeof(fname), "foo"));
  ASSERT(rdb_starts_with(fname, "foo/"));
  ASSERT(rdb_parse_filename(&type, &number, fname + 4));
  ASSERT(0 == number);
  ASSERT(RDB_FILE_LOCK == type);

  ASSERT(rdb_log_filename(fname, sizeof(fname), "foo", 192));
  ASSERT(rdb_starts_with(fname, "foo/"));
  ASSERT(rdb_parse_filename(&type, &number, fname + 4));
  ASSERT(192 == number);
  ASSERT(RDB_FILE_LOG == type);

  ASSERT(rdb_table_filename(fname, sizeof(fname), "bar", 200));
  ASSERT(rdb_starts_with(fname, "bar/"));
  ASSERT(rdb_parse_filename(&type, &number, fname + 4));
  ASSERT(200 == number);
  ASSERT(RDB_FILE_TABLE == type);

  ASSERT(rdb_desc_filename(fname, sizeof(fname), "bar", 100));
  ASSERT(rdb_starts_with(fname, "bar/"));
  ASSERT(rdb_parse_filename(&type, &number, fname + 4));
  ASSERT(100 == number);
  ASSERT(RDB_FILE_DESC == type);

  ASSERT(rdb_temp_filename(fname, sizeof(fname), "tmp", 999));
  ASSERT(rdb_starts_with(fname, "tmp/"));
  ASSERT(rdb_parse_filename(&type, &number, fname + 4));
  ASSERT(999 == number);
  ASSERT(RDB_FILE_TEMP == type);

  ASSERT(rdb_info_filename(fname, sizeof(fname), "foo"));
  ASSERT(rdb_starts_with(fname, "foo/"));
  ASSERT(rdb_parse_filename(&type, &number, fname + 4));
  ASSERT(0 == number);
  ASSERT(RDB_FILE_INFO == type);

  ASSERT(rdb_oldinfo_filename(fname, sizeof(fname), "foo"));
  ASSERT(rdb_starts_with(fname, "foo/"));
  ASSERT(rdb_parse_filename(&type, &number, fname + 4));
  ASSERT(0 == number);
  ASSERT(RDB_FILE_INFO == type);
}

RDB_EXTERN int
rdb_test_filenames(void);

int
rdb_test_filenames(void) {
  test_parse();
  test_construction();
  return 0;
}
