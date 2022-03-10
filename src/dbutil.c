/*!
 * dbutil.c - database utility for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "util/status.h"

#include "dumpfile.h"

static int
handle_dump_command(char **files, int num) {
  int ok = 1;
  int i;

  for (i = 0; i < num; i++) {
    int rc = ldb_dump_file(files[i], stdout);

    if (rc != LDB_OK) {
      fprintf(stderr, "%s\n", ldb_strerror(rc));
      ok = 0;
    }
  }

  return ok;
}

static void
print_usage(void) {
  fprintf(stderr,
    "Usage: dbutil command...\n"
    "   dump files...         -- dump contents of specified files\n");
}

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
    } else {
      print_usage();
      ok = 0;
    }
  }

  return (ok ? 0 : 1);
}
