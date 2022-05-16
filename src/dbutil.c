/*!
 * dbutil.c - database utility for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on google/leveldb:
 *   Copyright (c) 2011, The LevelDB Authors. All rights reserved.
 *   https://github.com/google/leveldb
 *
 * See LICENSE for more information.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "util/bloom.h"
#include "util/options.h"
#include "util/status.h"

#include "db_impl.h"
#include "dumpfile.h"

/*
 * Commands
 */

static int
handle_dump_command(char **argv, int argc) {
  int ok = 1;
  int i;

  for (i = 0; i < argc; i++) {
    int rc = ldb_dump_file(argv[i], stdout);

    if (rc != LDB_OK) {
      fprintf(stderr, "%s: %s\n", argv[i], ldb_strerror(rc));
      ok = 0;
    }
  }

  return ok;
}

static int
handle_repair_command(char **argv, int argc) {
  ldb_dbopt_t options = *ldb_dbopt_default;
  const char *dbname = NULL;
  ldb_bloom_t policy;
  int rc = LDB_OK;
  int ok = 1;
  int i;

  for (i = 0; i < argc; i++) {
    char junk;
    int n;

    if (sscanf(argv[i], "--block_size=%d%c", &n, &junk) == 1) {
      if (n >= 1024)
        options.block_size = n;
      else
        rc = LDB_INVALID;
    } else if (sscanf(argv[i], "--restart_interval=%d%c", &n, &junk) == 1) {
      if (n >= 0)
        options.block_restart_interval = n;
      else
        rc = LDB_INVALID;
    } else if (sscanf(argv[i], "--compression=%d%c", &n, &junk) == 1) {
      if (n >= 0 && n <= 1)
        options.compression = (enum ldb_compression)n;
      else
        rc = LDB_INVALID;
    } else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1) {
      if (n >= 1 && n <= 10000) {
        ldb_bloom_init(&policy, n);
        options.filter_policy = &policy;
      } else {
        rc = LDB_INVALID;
      }
    } else if (dbname == NULL) {
      dbname = argv[i];
    } else {
      rc = LDB_INVALID;
    }
  }

  if (dbname == NULL)
    rc = LDB_INVALID;

  if (rc == LDB_OK)
    rc = ldb_repair(dbname, &options);

  if (rc != LDB_OK) {
    fprintf(stderr, "%s\n", ldb_strerror(rc));
    ok = 0;
  }

  return ok;
}

static int
handle_copy_command(char **argv, int argc) {
  int rc = LDB_OK;
  int ok = 1;

  if (argc >= 2)
    rc = ldb_copy(argv[0], argv[1], NULL);
  else
    rc = LDB_INVALID;

  if (rc != LDB_OK) {
    fprintf(stderr, "%s\n", ldb_strerror(rc));
    ok = 0;
  }

  return ok;
}

static int
handle_destroy_command(char **argv, int argc) {
  int ok = 1;
  int i;

  for (i = 0; i < argc; i++) {
    int rc = ldb_destroy(argv[i], NULL);

    if (rc != LDB_OK) {
      fprintf(stderr, "%s: %s\n", argv[i], ldb_strerror(rc));
      ok = 0;
    }
  }

  return ok;
}

/*
 * Usage
 */

static void
print_usage(void) {
  fprintf(stderr,
    "Usage: dbutil command...\n"
    "   dump files...         -- dump contents of specified files\n"
    "   repair name [options] -- repair database\n"
    "   copy source dest      -- copy database\n"
    "   destroy name...       -- destroy database\n");
}

/*
 * Main
 */

int
main(int argc, char **argv) {
  const char *command;
  int ok = 1;

  if (argc < 2) {
    print_usage();
    ok = 0;
  } else {
    command = argv[1];

    if (strcmp(command, "dump") == 0) {
      ok = handle_dump_command(argv + 2, argc - 2);
    } else if (strcmp(command, "repair") == 0) {
      ok = handle_repair_command(argv + 2, argc - 2);
    } else if (strcmp(command, "copy") == 0) {
      ok = handle_copy_command(argv + 2, argc - 2);
    } else if (strcmp(command, "destroy") == 0) {
      ok = handle_destroy_command(argv + 2, argc - 2);
    } else {
      print_usage();
      ok = 0;
    }
  }

  return (ok ? 0 : 1);
}
