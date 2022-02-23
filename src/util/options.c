/*!
 * options.c - options for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stddef.h>
#include "comparator.h"
#include "options.h"

/*
 * DB Options
 */

static const rdb_dbopt_t db_options = {
  /* .comparator = */ NULL,
  /* .create_if_missing = */ 0,
  /* .error_if_exists = */ 0,
  /* .paranoid_checks = */ 0,
  /* .write_buffer_size = */ 4 * 1024 * 1024,
  /* .max_open_files = */ 1000,
  /* .block_cache = */ NULL,
  /* .block_size = */ 4 * 1024,
  /* .block_restart_interval = */ 16,
  /* .max_file_size = */ 2 * 1024 * 1024,
  /* .compression = */ RDB_NO_COMPRESSION,
  /* .reuse_logs = */ 0,
  /* .filter_policy = */ NULL
};

/*
 * Read Options
 */

static const rdb_readopt_t read_options = {
  /* .verify_checksums = */ 0,
  /* .fill_cache = */ 1,
  /* .snapshot = */ NULL
};

/*
 * Write Options
 */

static const rdb_writeopt_t write_options = {
  /* .sync = */ 0
};

/*
 * Globals
 */

const rdb_dbopt_t *rdb_dbopt_default = &db_options;
const rdb_readopt_t *rdb_readopt_default = &read_options;
const rdb_writeopt_t *rdb_writeopt_default = &write_options;
