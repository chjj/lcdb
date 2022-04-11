/*!
 * env_test.c - env test for lcdb
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

#include "util/buffer.h"
#include "util/env.h"
#include "util/internal.h"
#include "util/port.h"
#include "util/random.h"
#include "util/thread_pool.h"
#include "util/slice.h"
#include "util/status.h"
#include "util/strutil.h"
#include "util/testutil.h"

/*
 * Filesystem
 */

static void
test_read_write(void) {
  ldb_buffer_t data, str, result, scratch;
  size_t data_size = 10 * 1048576;
  char path[LDB_PATH_MAX];
  ldb_wfile_t *wfile;
  ldb_rfile_t *rfile;
  ldb_rand_t rnd;

  ldb_buffer_init(&data);
  ldb_buffer_init(&str);
  ldb_buffer_init(&result);
  ldb_buffer_init(&scratch);

  ldb_rand_init(&rnd, ldb_random_seed());

  ASSERT(ldb_test_filename(path, sizeof(path), "open_on_read.txt"));
  ASSERT(ldb_truncfile_create(path, &wfile) == LDB_OK);

  /* Fill a file with data generated via a sequence of randomly sized writes. */
  while (data.size < data_size) {
    size_t len = ldb_rand_skewed(&rnd, 18);

    ldb_random_string(&str, &rnd, len);

    ASSERT(ldb_wfile_append(wfile, &str) == LDB_OK);

    ldb_buffer_concat(&data, &str);

    if (ldb_rand_one_in(&rnd, 10))
      ASSERT(ldb_wfile_flush(wfile) == LDB_OK);
  }

  ASSERT(ldb_wfile_sync(wfile) == LDB_OK);
  ASSERT(ldb_wfile_close(wfile) == LDB_OK);

  ldb_wfile_destroy(wfile);

  /* Read all data using a sequence of randomly sized reads. */
  ASSERT(ldb_seqfile_create(path, &rfile) == LDB_OK);

  while (result.size < data.size) {
    size_t tmp = ldb_rand_skewed(&rnd, 18);
    size_t len = LDB_MIN(tmp, data.size - result.size);
    ldb_slice_t chunk;

    ldb_buffer_resize(&scratch, LDB_MAX(len, 1));

    ASSERT(ldb_rfile_read(rfile, &chunk, scratch.data, len) == LDB_OK);
    ASSERT(len == 0 || chunk.size > 0);
    ASSERT(chunk.size <= len);

    ldb_buffer_concat(&result, &chunk);
  }

  ASSERT(ldb_buffer_equal(&result, &data));

  ldb_rfile_destroy(rfile);

  ASSERT(ldb_remove_file(path) == LDB_OK);

  ldb_buffer_clear(&data);
  ldb_buffer_clear(&str);
  ldb_buffer_clear(&result);
  ldb_buffer_clear(&scratch);
}

static void
test_open_non_existent_file(void) {
  char path[LDB_PATH_MAX];
  ldb_rfile_t *rfile;

  ASSERT(ldb_test_filename(path, sizeof(path), "non_existent_file"));
  ASSERT(!ldb_file_exists(path));
  ASSERT(ldb_randfile_create(path, &rfile, 1) == LDB_NOTFOUND);
  ASSERT(ldb_seqfile_create(path, &rfile) == LDB_NOTFOUND);
}

static void
test_reopen_writable_file(void) {
  char path[LDB_PATH_MAX];
  ldb_buffer_t result;
  ldb_wfile_t *wfile;
  ldb_slice_t data;

  ldb_buffer_init(&result);

  ASSERT(ldb_test_filename(path, sizeof(path), "reopen_writable_file.txt"));

  ldb_remove_file(path);

  ASSERT(ldb_truncfile_create(path, &wfile) == LDB_OK);

  data = ldb_string("hello world!");

  ASSERT(ldb_wfile_append(wfile, &data) == LDB_OK);
  ASSERT(ldb_wfile_close(wfile) == LDB_OK);

  ldb_wfile_destroy(wfile);

  ASSERT(ldb_truncfile_create(path, &wfile) == LDB_OK);

  data = ldb_string("42");

  ASSERT(ldb_wfile_append(wfile, &data) == LDB_OK);
  ASSERT(ldb_wfile_close(wfile) == LDB_OK);

  ldb_wfile_destroy(wfile);

  ASSERT(ldb_read_file(path, &result) == LDB_OK);
  ASSERT(ldb_buffer_equal(&result, &data));

  ldb_remove_file(path);
  ldb_buffer_clear(&result);
}

static void
test_reopen_appendable_file(void) {
  char path[LDB_PATH_MAX];
  ldb_buffer_t result;
  ldb_wfile_t *wfile;
  ldb_slice_t data;

  ldb_buffer_init(&result);

  ASSERT(ldb_test_filename(path, sizeof(path), "reopen_appendable_file.txt"));

  ldb_remove_file(path);

  ASSERT(ldb_appendfile_create(path, &wfile) == LDB_OK);

  data = ldb_string("hello world!");

  ASSERT(ldb_wfile_append(wfile, &data) == LDB_OK);
  ASSERT(ldb_wfile_close(wfile) == LDB_OK);

  ldb_wfile_destroy(wfile);

  ASSERT(ldb_appendfile_create(path, &wfile) == LDB_OK);

  data = ldb_string("42");

  ASSERT(ldb_wfile_append(wfile, &data) == LDB_OK);
  ASSERT(ldb_wfile_close(wfile) == LDB_OK);

  ldb_wfile_destroy(wfile);

  ASSERT(ldb_read_file(path, &result) == LDB_OK);

  data = ldb_string("hello world!42");

  ASSERT(ldb_buffer_equal(&result, &data));

  ldb_remove_file(path);
  ldb_buffer_clear(&result);
}

static void
test_open_on_read(void) {
  /* Write some test data to a single file that will be opened |n| times. */
  static const int num_files = 4 + 4 + 5;
  static const char file_data[] = "abcdefghijklmnopqrstuvwxyz";
  ldb_rfile_t *files[4 + 4 + 5];
  char path[LDB_PATH_MAX];
  ldb_slice_t chunk;
  uint8_t scratch;
  int i;

  ASSERT(ldb_test_filename(path, sizeof(path), "open_on_read.txt"));

  {
    ldb_slice_t data = ldb_string(file_data);

    ASSERT(ldb_write_file(path, &data, 0) == LDB_OK);
  }

  for (i = 0; i < num_files; i++)
    ASSERT(ldb_randfile_create(path, &files[i], (i & 1)) == LDB_OK);

  for (i = 0; i < num_files; i++) {
    ASSERT(ldb_rfile_pread(files[i], &chunk, &scratch, 1, i) == LDB_OK);
    ASSERT(chunk.size == 1);
    ASSERT(file_data[i] == chunk.data[0]);
  }

  for (i = 0; i < num_files; i++)
    ldb_rfile_destroy(files[i]);

  ASSERT(ldb_remove_file(path) == LDB_OK);
}

/*
 * Threads
 */

#if defined(_WIN32) || defined(LDB_PTHREAD)

struct run_state {
  ldb_mutex_t mu;
  ldb_cond_t cvar;
  int called;
  int last_id;
  int val;
  int num_running;
};

struct callback {
  struct run_state *state;
  int id;
};

/*
 * Thread Pool
 */

static void
run_thread_1(void *arg) {
  struct run_state *state = arg;

  ldb_mutex_lock(&state->mu);

  ASSERT(state->called == 0);

  state->called = 1;

  ldb_cond_signal(&state->cvar);
  ldb_mutex_unlock(&state->mu);
}

static void
test_run_immediately(ldb_pool_t *pool) {
  struct run_state state;

  ldb_mutex_init(&state.mu);
  ldb_cond_init(&state.cvar);

  state.called = 0;

  ldb_pool_schedule(pool, run_thread_1, &state);

  ldb_mutex_lock(&state.mu);

  while (!state.called)
    ldb_cond_wait(&state.cvar, &state.mu);

  ldb_mutex_unlock(&state.mu);

  ldb_cond_destroy(&state.cvar);
  ldb_mutex_destroy(&state.mu);
}

static void
run_thread_2(void *arg) {
  struct callback *callback = arg;
  struct run_state *state = callback->state;

  ldb_mutex_lock(&state->mu);

  ASSERT(state->last_id == callback->id - 1);

  state->last_id = callback->id;

  ldb_cond_signal(&state->cvar);
  ldb_mutex_unlock(&state->mu);
}

static void
test_run_many(ldb_pool_t *pool) {
  struct callback cb1, cb2, cb3, cb4;
  struct run_state state;

  ldb_mutex_init(&state.mu);
  ldb_cond_init(&state.cvar);

  state.last_id = 0;

  cb1.state = &state;
  cb1.id = 1;
  cb2.state = &state;
  cb2.id = 2;
  cb3.state = &state;
  cb3.id = 3;
  cb4.state = &state;
  cb4.id = 4;

  ldb_pool_schedule(pool, run_thread_2, &cb1);
  ldb_pool_schedule(pool, run_thread_2, &cb2);
  ldb_pool_schedule(pool, run_thread_2, &cb3);
  ldb_pool_schedule(pool, run_thread_2, &cb4);

  ldb_mutex_lock(&state.mu);

  while (state.last_id != 4)
    ldb_cond_wait(&state.cvar, &state.mu);

  ldb_mutex_unlock(&state.mu);

  ldb_cond_destroy(&state.cvar);
  ldb_mutex_destroy(&state.mu);
}

static void
run_thread_3(void *arg) {
  struct run_state *state = arg;

  ldb_mutex_lock(&state->mu);

  state->val += 1;
  state->num_running -= 1;

  ldb_cond_signal(&state->cvar);
  ldb_mutex_unlock(&state->mu);
}

/*
 * Port
 */

static void
test_start_thread(void) {
  struct run_state state;
  int i;

  ldb_mutex_init(&state.mu);
  ldb_cond_init(&state.cvar);

  state.val = 0;
  state.num_running = 3;

  for (i = 0; i < 3; i++) {
    ldb_thread_t thread;
    ldb_thread_create(&thread, run_thread_3, &state);
    ldb_thread_detach(&thread);
  }

  ldb_mutex_lock(&state.mu);

  while (state.num_running != 0)
    ldb_cond_wait(&state.cvar, &state.mu);

  ASSERT(state.val == 3);

  ldb_mutex_unlock(&state.mu);

  ldb_cond_destroy(&state.cvar);
  ldb_mutex_destroy(&state.mu);
}

#endif /* _WIN32 || LDB_PTHREAD */

/*
 * Execute
 */

int
main(void) {
  test_read_write();
  test_open_non_existent_file();
  test_reopen_writable_file();
  test_reopen_appendable_file();
  test_open_on_read();

#if defined(_WIN32) || defined(LDB_PTHREAD)
  {
    ldb_pool_t *pool = ldb_pool_create(1);

    test_run_immediately(pool);
    test_run_many(pool);
    test_start_thread();

    ldb_pool_destroy(pool);
  }
#endif

  return 0;
}
