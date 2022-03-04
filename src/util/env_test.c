/*!
 * env_test.c - env test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#undef NDEBUG

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "buffer.h"
#include "env.h"
#include "extern.h"
#include "internal.h"
#include "port.h"
#include "random.h"
#include "thread_pool.h"
#include "slice.h"
#include "status.h"
#include "strutil.h"
#include "testutil.h"

/*
 * Filesystem
 */

static void
test_read_write(void) {
  rdb_buffer_t data, str, result, scratch;
  size_t data_size = 10 * 1048576;
  char path[RDB_PATH_MAX];
  rdb_wfile_t *wfile;
  rdb_rfile_t *rfile;
  rdb_rand_t rnd;

  rdb_buffer_init(&data);
  rdb_buffer_init(&str);
  rdb_buffer_init(&result);
  rdb_buffer_init(&scratch);

  rdb_rand_init(&rnd, rdb_random_seed());

  assert(rdb_test_directory(path, sizeof(path)));
  assert(rdb_join(path, sizeof(path), path, "open_on_read.txt"));
  assert(rdb_truncfile_create(path, &wfile) == RDB_OK);

  /* Fill a file with data generated via a sequence of randomly sized writes. */
  while (data.size < data_size) {
    size_t len = rdb_rand_skewed(&rnd, 18);

    rdb_random_string(&str, &rnd, len);

    assert(rdb_wfile_append(wfile, &str) == RDB_OK);

    rdb_buffer_concat(&data, &str);

    if (rdb_rand_one_in(&rnd, 10))
      assert(rdb_wfile_flush(wfile) == RDB_OK);
  }

  assert(rdb_wfile_sync(wfile) == RDB_OK);
  assert(rdb_wfile_close(wfile) == RDB_OK);

  rdb_wfile_destroy(wfile);

  /* Read all data using a sequence of randomly sized reads. */
  assert(rdb_seqfile_create(path, &rfile) == RDB_OK);

  while (result.size < data.size) {
    size_t tmp = rdb_rand_skewed(&rnd, 18);
    size_t len = RDB_MIN(tmp, data.size - result.size);
    rdb_slice_t chunk;

    rdb_buffer_resize(&scratch, RDB_MAX(len, 1));

    assert(rdb_rfile_read(rfile, &chunk, scratch.data, len) == RDB_OK);
    assert(len == 0 || chunk.size > 0);
    assert(chunk.size <= len);

    rdb_buffer_concat(&result, &chunk);
  }

  assert(rdb_buffer_equal(&result, &data));

  rdb_rfile_destroy(rfile);

  assert(rdb_remove_file(path) == RDB_OK);

  rdb_buffer_clear(&data);
  rdb_buffer_clear(&str);
  rdb_buffer_clear(&result);
  rdb_buffer_clear(&scratch);
}

static void
test_open_non_existent_file(void) {
  char path[RDB_PATH_MAX];
  rdb_rfile_t *rfile;

  assert(rdb_test_directory(path, sizeof(path)));
  assert(rdb_join(path, sizeof(path), path, "non_existent_file"));
  assert(!rdb_file_exists(path));
  assert(rdb_randfile_create(path, &rfile, 1) == RDB_NOTFOUND);
  assert(rdb_seqfile_create(path, &rfile) == RDB_NOTFOUND);
}

static void
test_reopen_writable_file(void) {
  char path[RDB_PATH_MAX];
  rdb_buffer_t result;
  rdb_wfile_t *wfile;
  rdb_slice_t data;

  rdb_buffer_init(&result);

  assert(rdb_test_directory(path, sizeof(path)));
  assert(rdb_join(path, sizeof(path), path, "reopen_writable_file.txt"));

  rdb_remove_file(path);

  assert(rdb_truncfile_create(path, &wfile) == RDB_OK);

  data = rdb_string("hello world!");

  assert(rdb_wfile_append(wfile, &data) == RDB_OK);
  assert(rdb_wfile_close(wfile) == RDB_OK);

  rdb_wfile_destroy(wfile);

  assert(rdb_truncfile_create(path, &wfile) == RDB_OK);

  data = rdb_string("42");

  assert(rdb_wfile_append(wfile, &data) == RDB_OK);
  assert(rdb_wfile_close(wfile) == RDB_OK);

  rdb_wfile_destroy(wfile);

  assert(rdb_read_file(path, &result) == RDB_OK);
  assert(rdb_buffer_equal(&result, &data));

  rdb_remove_file(path);
  rdb_buffer_clear(&result);
}

static void
test_reopen_appendable_file(void) {
  char path[RDB_PATH_MAX];
  rdb_buffer_t result;
  rdb_wfile_t *wfile;
  rdb_slice_t data;

  rdb_buffer_init(&result);

  assert(rdb_test_directory(path, sizeof(path)));
  assert(rdb_join(path, sizeof(path), path, "reopen_appendable_file.txt"));

  rdb_remove_file(path);

  assert(rdb_appendfile_create(path, &wfile) == RDB_OK);

  data = rdb_string("hello world!");

  assert(rdb_wfile_append(wfile, &data) == RDB_OK);
  assert(rdb_wfile_close(wfile) == RDB_OK);

  rdb_wfile_destroy(wfile);

  assert(rdb_appendfile_create(path, &wfile) == RDB_OK);

  data = rdb_string("42");

  assert(rdb_wfile_append(wfile, &data) == RDB_OK);
  assert(rdb_wfile_close(wfile) == RDB_OK);

  rdb_wfile_destroy(wfile);

  assert(rdb_read_file(path, &result) == RDB_OK);

  data = rdb_string("hello world!42");

  assert(rdb_buffer_equal(&result, &data));

  rdb_remove_file(path);
  rdb_buffer_clear(&result);
}

static void
test_open_on_read(void) {
  /* Write some test data to a single file that will be opened |n| times. */
  static const int num_files = 4 + 4 + 5;
  static const char file_data[] = "abcdefghijklmnopqrstuvwxyz";
  rdb_rfile_t *files[4 + 4 + 5];
  char path[RDB_PATH_MAX];
  rdb_slice_t chunk;
  uint8_t scratch;
  FILE *f;
  int i;

  assert(rdb_test_directory(path, sizeof(path)));
  assert(rdb_join(path, sizeof(path), path, "open_on_read.txt"));

  f = fopen(path, "we");

  assert(f != NULL);

  fputs(file_data, f);
  fclose(f);

  /* Open test file some number above the sum of the two limits to force
     open-on-read behavior of POSIX Env leveldb::RandomAccessFile. */
  /* https://github.com/google/leveldb/blob/main/util/env_posix.cc#L911 */
  /* EnvPosixTestHelper::SetReadOnlyFDLimit(4); */
  /* EnvPosixTestHelper::SetReadOnlyMMapLimit(4); */

  for (i = 0; i < num_files; i++)
    assert(rdb_randfile_create(path, &files[i], 1) == RDB_OK);

  for (i = 0; i < num_files; i++) {
    assert(rdb_rfile_pread(files[i], &chunk, &scratch, 1, i) == RDB_OK);
    assert(chunk.size == 1);
    assert(file_data[i] == chunk.data[0]);
  }

  for (i = 0; i < num_files; i++)
    rdb_rfile_destroy(files[i]);

  assert(rdb_remove_file(path) == RDB_OK);
}

/*
 * Threads
 */

struct run_state {
  rdb_mutex_t mu;
  rdb_cond_t cvar;
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

  rdb_mutex_lock(&state->mu);

  assert(state->called == 0);

  state->called = 1;

  rdb_cond_signal(&state->cvar);
  rdb_mutex_unlock(&state->mu);
}

static void
test_run_immediately(rdb_pool_t *pool) {
  struct run_state state;

  rdb_mutex_init(&state.mu);
  rdb_cond_init(&state.cvar);

  state.called = 0;

  rdb_pool_schedule(pool, run_thread_1, &state);

  rdb_mutex_lock(&state.mu);

  while (!state.called)
    rdb_cond_wait(&state.cvar, &state.mu);

  rdb_mutex_unlock(&state.mu);

  rdb_cond_destroy(&state.cvar);
  rdb_mutex_destroy(&state.mu);
}

static void
run_thread_2(void *arg) {
  struct callback *callback = arg;
  struct run_state *state = callback->state;

  rdb_mutex_lock(&state->mu);

  assert(state->last_id == callback->id - 1);

  state->last_id = callback->id;

  rdb_cond_signal(&state->cvar);
  rdb_mutex_unlock(&state->mu);
}

static void
test_run_many(rdb_pool_t *pool) {
  struct callback cb1, cb2, cb3, cb4;
  struct run_state state;

  rdb_mutex_init(&state.mu);
  rdb_cond_init(&state.cvar);

  state.last_id = 0;

  cb1.state = &state;
  cb1.id = 1;
  cb2.state = &state;
  cb2.id = 2;
  cb3.state = &state;
  cb3.id = 3;
  cb4.state = &state;
  cb4.id = 4;

  rdb_pool_schedule(pool, run_thread_2, &cb1);
  rdb_pool_schedule(pool, run_thread_2, &cb2);
  rdb_pool_schedule(pool, run_thread_2, &cb3);
  rdb_pool_schedule(pool, run_thread_2, &cb4);

  rdb_mutex_lock(&state.mu);

  while (state.last_id != 4)
    rdb_cond_wait(&state.cvar, &state.mu);

  rdb_mutex_unlock(&state.mu);

  rdb_cond_destroy(&state.cvar);
  rdb_mutex_destroy(&state.mu);
}

static void
run_thread_3(void *arg) {
  struct run_state *state = arg;

  rdb_mutex_lock(&state->mu);

  state->val += 1;
  state->num_running -= 1;

  rdb_cond_signal(&state->cvar);
  rdb_mutex_unlock(&state->mu);
}

/*
 * Port
 */

static void
test_start_thread(void) {
  struct run_state state;
  int i;

  rdb_mutex_init(&state.mu);
  rdb_cond_init(&state.cvar);

  state.val = 0;
  state.num_running = 3;

  for (i = 0; i < 3; i++) {
    rdb_thread_t thread;
    rdb_thread_create(&thread, run_thread_3, &state);
    rdb_thread_detach(&thread);
  }

  rdb_mutex_lock(&state.mu);

  while (state.num_running != 0)
    rdb_cond_wait(&state.cvar, &state.mu);

  assert(state.val == 3);

  rdb_mutex_unlock(&state.mu);

  rdb_cond_destroy(&state.cvar);
  rdb_mutex_destroy(&state.mu);
}

/*
 * Execute
 */

RDB_EXTERN int
rdb_test_env(void);

int
rdb_test_env(void) {
#if defined(_WIN32) || defined(RDB_PTHREAD)
  rdb_pool_t *pool;
#endif

  rdb_env_init();

  test_read_write();
  test_open_non_existent_file();
  test_reopen_writable_file();
  test_reopen_appendable_file();
  test_open_on_read();

  rdb_env_clear();

#if defined(_WIN32) || defined(RDB_PTHREAD)
  pool = rdb_pool_create(1);

  test_run_immediately(pool);
  test_run_many(pool);
  test_start_thread();

  rdb_pool_destroy(pool);
#endif

  return 0;
}
