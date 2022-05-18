/*!
 * t-skiplist.c - skiplist test for lcdb
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

#include "util/arena.h"
#include "util/atomic.h"
#include "util/coding.h"
#include "util/comparator.h"
#include "util/hash.h"
#include "util/internal.h"
#include "util/port.h"
#include "util/rbt.h"
#include "util/slice.h"
#include "util/testutil.h"
#include "util/thread_pool.h"

#include "skiplist.h"

/*
 * Comparator
 */

static int
integer_compare(const ldb_comparator_t *cmp,
                const ldb_slice_t *x,
                const ldb_slice_t *y) {
  uint64_t xn = ldb_fixed64_decode(x->data);
  uint64_t yn = ldb_fixed64_decode(y->data);

  (void)cmp;

  if (xn < yn)
    return -1;

  if (xn > yn)
    return +1;

  return 0;
}

static const ldb_comparator_t integer_comparator = {
  /* .name = */ "leveldb.IntegerComparator",
  /* .compare = */ integer_compare,
  /* .shortest_separator = */ NULL,
  /* .short_successor = */ NULL,
  /* .user_comparator = */ NULL,
  /* .state = */ NULL
};

/*
 * Helpers
 */

static uint8_t *
encode_key(uint64_t key, uint8_t *buf) {
  buf[0] = 8;
  ldb_fixed64_write(buf + 1, key);
  return buf;
}

/*
 * SkipList
 */

#define skiplist_t ldb_skiplist_t
#define skiplist_init ldb_skiplist_init

static void
skiplist_insert(skiplist_t *list, uint64_t key) {
  uint8_t *buf = ldb_arena_alloc(list->arena, 9);
  ldb_skiplist_insert(list, encode_key(key, buf));
}

static int
skiplist_contains(const skiplist_t *list, uint64_t key) {
  uint8_t buf[9];
  return ldb_skiplist_contains(list, encode_key(key, buf));
}

/*
 * SkipList::Iterator
 */

#define skipiter_t ldb_skipiter_t
#define skipiter_init ldb_skipiter_init
#define skipiter_valid ldb_skipiter_valid
#define skipiter_next ldb_skipiter_next
#define skipiter_prev ldb_skipiter_prev
#define skipiter_first ldb_skipiter_first
#define skipiter_last ldb_skipiter_last

static uint64_t
skipiter_key(const skipiter_t *iter) {
  const uint8_t *buf = ldb_skipiter_key(iter);
  return ldb_fixed64_decode(buf + 1);
}

static void
skipiter_seek(skipiter_t *iter, uint64_t target) {
  uint8_t buf[9];
  ldb_skipiter_seek(iter, encode_key(target, buf));
}

/*
 * SkipList Tests
 */

static void
test_skip_empty(void) {
  ldb_arena_t arena;
  skiplist_t list;
  skipiter_t iter;

  ldb_arena_init(&arena);

  skiplist_init(&list, &integer_comparator, &arena);

  ASSERT(!skiplist_contains(&list, 10));

  skipiter_init(&iter, &list);

  ASSERT(!skipiter_valid(&iter));

  skipiter_first(&iter);

  ASSERT(!skipiter_valid(&iter));

  skipiter_seek(&iter, 100);

  ASSERT(!skipiter_valid(&iter));

  skipiter_last(&iter);

  ASSERT(!skipiter_valid(&iter));

  ldb_arena_clear(&arena);
}

static void
test_skip_insert_and_lookup(void) {
  const int N = 2000;
  const int R = 5000;
  ldb_arena_t arena;
  skiplist_t list;
  rb_set64_t keys;
  ldb_rand_t rnd;
  int i, j;

  ldb_arena_init(&arena);

  skiplist_init(&list, &integer_comparator, &arena);

  rb_set64_init(&keys);
  ldb_rand_init(&rnd, 1000);

  for (i = 0; i < N; i++) {
    uint64_t key = ldb_rand_next(&rnd) % R;

    if (rb_set64_put(&keys, key))
      skiplist_insert(&list, key);
  }

  for (i = 0; i < R; i++) {
    if (skiplist_contains(&list, i))
      ASSERT(rb_set64_has(&keys, i) == 1);
    else
      ASSERT(rb_set64_has(&keys, i) == 0);
  }

  /* Simple iterator tests. */
  {
    rb_iter_t it = rb_tree_iterator(&keys);
    skipiter_t iter;
    rb_val_t val;

    val.ui = 0;

    skipiter_init(&iter, &list);

    ASSERT(!skipiter_valid(&iter));

    skipiter_seek(&iter, 0);
    rb_iter_seek(&it, val);

    ASSERT(skipiter_valid(&iter));
    ASSERT(rb_iter_key(&it).ui == skipiter_key(&iter));

    skipiter_first(&iter);
    rb_iter_first(&it);

    ASSERT(skipiter_valid(&iter));
    ASSERT(rb_iter_key(&it).ui == skipiter_key(&iter));

    skipiter_last(&iter);
    rb_iter_last(&it);

    ASSERT(skipiter_valid(&iter));
    ASSERT(rb_iter_key(&it).ui == skipiter_key(&iter));
  }

  /* Forward iteration test. */
  for (i = 0; i < R; i++) {
    rb_iter_t it = rb_tree_iterator(&keys);
    skipiter_t iter;
    rb_val_t val;

    val.ui = i;

    skipiter_init(&iter, &list);

    skipiter_seek(&iter, i);
    rb_iter_seek(&it, val);

    /* Compare against model iterator. */
    for (j = 0; j < 3; j++) {
      ASSERT(rb_iter_valid(&it) == skipiter_valid(&iter));

      if (!rb_iter_valid(&it))
        break;

      ASSERT(rb_iter_key(&it).ui == skipiter_key(&iter));

      skipiter_next(&iter);
      rb_iter_next(&it);
    }
  }

  /* Backward iteration test. */
  {
    rb_iter_t it = rb_tree_iterator(&keys);
    skipiter_t iter;

    skipiter_init(&iter, &list);

    skipiter_last(&iter);
    rb_iter_last(&it);

    /* Compare against model iterator. */
    while (rb_iter_valid(&it)) {
      ASSERT(skipiter_valid(&iter));
      ASSERT(rb_iter_key(&it).ui == skipiter_key(&iter));

      skipiter_prev(&iter);
      rb_iter_prev(&it);
    }

    ASSERT(!skipiter_valid(&iter));
  }

  ldb_arena_clear(&arena);
  rb_set64_clear(&keys);
}

/* We want to make sure that with a single writer and multiple
 * concurrent readers (with no synchronization other than when a
 * reader's iterator is created), the reader always observes all the
 * data that was present in the skip list when the iterator was
 * constructed. Because insertions are happening concurrently, we may
 * also observe new values that were inserted since the iterator was
 * constructed, but we should never miss any values that were present
 * at iterator construction time.
 *
 * We generate multi-part keys:
 *     <key,gen,hash>
 * where:
 *     key is in range [0..K-1]
 *     gen is a generation number for key
 *     hash is hash(key,gen)
 *
 * The insertion code picks a random key, sets gen to be 1 + the last
 * generation number inserted for that key, and sets hash to Hash(key,gen).
 *
 * At the beginning of a read, we snapshot the last inserted
 * generation number for each key. We then iterate, including random
 * calls to next() and seek(). For every key we encounter, we
 * check that it is either expected given the initial snapshot or has
 * been concurrently added since the iterator started.
 */

static const uint32_t K = 4;

static uint64_t
int_key(uint64_t key) {
  return (key >> 40);
}

static uint64_t
int_gen(uint64_t key) {
  return (key >> 8) & 0xffffffffu;
}

static uint64_t
int_hash(uint64_t key) {
  return key & 0xff;
}

static uint64_t
hash_numbers(uint64_t k, uint64_t g) {
  uint64_t data[2];

  data[0] = k;
  data[1] = g;

  return ldb_hash((uint8_t *)data, sizeof(data), 0);
}

static uint64_t
make_key(uint64_t k, uint64_t g) {
  ASSERT(k <= K); /* We sometimes pass K to seek to the end of the skiplist. */
  ASSERT(g <= 0xffffffffu);
  return ((k << 40) | (g << 8) | (hash_numbers(k, g) & 0xff));
}

static int
is_valid_key(uint64_t k) {
  return int_hash(k) == (hash_numbers(int_key(k), int_gen(k)) & 0xff);
}

static uint64_t
random_target(ldb_rand_t *rnd) {
  switch (ldb_rand_next(rnd) % 10) {
    case 0:
      /* Seek to beginning. */
      return make_key(0, 0);
    case 1:
      /* Seek to end. */
      return make_key(K, 0);
    default:
      /* Seek to middle. */
      return make_key(ldb_rand_next(rnd) % K, 0);
  }
}

/*
 * ConcurrentTest::State
 */

/* Per-key generation. */
typedef struct cstate_s {
  ldb_atomic(int) generation[4 /* K */];
} cstate_t;

static void
cstate_set(cstate_t *s, int k, int v) {
  ldb_atomic_store(&s->generation[k], v, ldb_order_release);
}

static int
cstate_get(cstate_t *s, int k) {
  return ldb_atomic_load(&s->generation[k], ldb_order_acquire);
}

static void
cstate_init(cstate_t *s) {
  int k;

  for (k = 0; k < (int)K; k++)
    cstate_set(s, k, 0);
}

/*
 * ConcurrentTest
 */

typedef struct ctest_s {
  /* Current state of the test. */
  cstate_t current;

  ldb_arena_t arena;

  /* SkipList is not protected by mu. We just use a single writer
     thread to modify it. */
  skiplist_t list;
} ctest_t;

static void
ctest_init(ctest_t *t) {
  cstate_init(&t->current);
  ldb_arena_init(&t->arena);
  skiplist_init(&t->list, &integer_comparator, &t->arena);
}

static void
ctest_clear(ctest_t *t) {
  ldb_arena_clear(&t->arena);
}

/* REQUIRES: External synchronization. */
static void
ctest_write_step(ctest_t *t, ldb_rand_t *rnd) {
  uint32_t k = ldb_rand_next(rnd) % K;
  intptr_t g = cstate_get(&t->current, k) + 1;
  uint64_t key = make_key(k, g);

  skiplist_insert(&t->list, key);
  cstate_set(&t->current, k, g);
}

static void
ctest_read_step(ctest_t *t, ldb_rand_t *rnd) {
  /* Remember the initial committed state of the skiplist. */
  cstate_t initial;
  skipiter_t iter;
  uint64_t pos;
  int k;

  cstate_init(&initial);

  for (k = 0; k < (int)K; k++)
    cstate_set(&initial, k, cstate_get(&t->current, k));

  pos = random_target(rnd);

  skipiter_init(&iter, &t->list);
  skipiter_seek(&iter, pos);

  for (;;) {
    uint64_t current;

    if (!skipiter_valid(&iter)) {
      current = make_key(K, 0);
    } else {
      current = skipiter_key(&iter);

      ASSERT(is_valid_key(current));
    }

    ASSERT(pos <= current);

    /* Verify that everything in [pos,current]
       was not present in initial state. */
    while (pos < current) {
      ASSERT(int_key(pos) < K);

      /* Note that generation 0 is never inserted,
         so it is ok if <*,0,*> is missing. */
      ASSERT((int_gen(pos) == 0) ||
             (int_gen(pos) > (uint64_t)cstate_get(&initial, int_key(pos))));

      /* Advance to next key in the valid key space. */
      if (int_key(pos) < int_key(current))
        pos = make_key(int_key(pos) + 1, 0);
      else
        pos = make_key(int_key(pos), int_gen(pos) + 1);
    }

    if (!skipiter_valid(&iter))
      break;

    if (ldb_rand_next(rnd) % 2) {
      skipiter_next(&iter);
      pos = make_key(int_key(pos), int_gen(pos) + 1);
    } else {
      uint64_t new_target = random_target(rnd);

      if (new_target > pos) {
        pos = new_target;
        skipiter_seek(&iter, new_target);
      }
    }
  }
}

/* Simple test that does single-threaded
   testing of the ConcurrentTest scaffolding. */
static void
test_skip_concurrent_without_threads(void) {
  ldb_rand_t rnd;
  ctest_t t;
  int i;

  ctest_init(&t);

  ldb_rand_init(&rnd, ldb_random_seed());

  for (i = 0; i < 10000; i++) {
    ctest_read_step(&t, &rnd);
    ctest_write_step(&t, &rnd);
  }

  ctest_clear(&t);
}

#if defined(_WIN32) || defined(LDB_PTHREAD)

/*
 * TestState
 */

enum reader_state {
  STATE_STARTING,
  STATE_RUNNING,
  STATE_DONE
};

typedef struct tstate_s {
  ctest_t test;
  int seed;
  ldb_atomic(int) quit_flag;
  enum reader_state state;
  ldb_mutex_t mu;
  ldb_cond_t state_cv;
} tstate_t;

static void
tstate_init(tstate_t *t, int seed) {
  ctest_init(&t->test);

  t->seed = seed;
  t->quit_flag = 0;
  t->state = STATE_STARTING;

  ldb_mutex_init(&t->mu);
  ldb_cond_init(&t->state_cv);
}

static void
tstate_clear(tstate_t *t) {
  ldb_cond_destroy(&t->state_cv);
  ldb_mutex_destroy(&t->mu);
  ctest_clear(&t->test);
}

static void
tstate_wait(tstate_t *t, enum reader_state s) {
  ldb_mutex_lock(&t->mu);

  while (t->state != s)
    ldb_cond_wait(&t->state_cv, &t->mu);

  ldb_mutex_unlock(&t->mu);
}

static void
tstate_change(tstate_t *t, enum reader_state s) {
  ldb_mutex_lock(&t->mu);

  t->state = s;

  ldb_cond_signal(&t->state_cv);
  ldb_mutex_unlock(&t->mu);
}

static void
concurrent_reader(void *arg) {
  tstate_t *state = (tstate_t *)arg;
  int64_t reads = 0;
  ldb_rand_t rnd;

  ldb_rand_init(&rnd, state->seed);

  tstate_change(state, STATE_RUNNING);

  while (!ldb_atomic_load(&state->quit_flag, ldb_order_acquire)) {
    ctest_read_step(&state->test, &rnd);
    ++reads;
  }

  tstate_change(state, STATE_DONE);
}

static void
test_skip_concurrent(ldb_pool_t *pool, int run) {
  const int size = 1000;
  const int N = 1000;
  ldb_rand_t rnd;
  int i, j, seed;

  seed = ldb_random_seed() + (run * 100);

  ldb_rand_init(&rnd, seed);

  for (i = 0; i < N; i++) {
    tstate_t state;

    tstate_init(&state, seed + 1);

    if ((i % 100) == 0)
      fprintf(stderr, "Run %d of %d\n", i, N);

    ldb_pool_schedule(pool, &concurrent_reader, &state);

    tstate_wait(&state, STATE_RUNNING);

    for (j = 0; j < size; j++)
      ctest_write_step(&state.test, &rnd);

    ldb_atomic_store(&state.quit_flag, 1, ldb_order_release);

    tstate_wait(&state, STATE_DONE);
    tstate_clear(&state);
  }
}

#endif /* _WIN32 || LDB_PTHREAD */

/*
 * Execute
 */

int
main(void) {
  test_skip_empty();
  test_skip_insert_and_lookup();
  test_skip_concurrent_without_threads();

#if defined(_WIN32) || defined(LDB_PTHREAD)
  {
    ldb_pool_t *pool = ldb_pool_create(1);

    test_skip_concurrent(pool, 1);
    test_skip_concurrent(pool, 2);
    test_skip_concurrent(pool, 3);
    test_skip_concurrent(pool, 4);
    test_skip_concurrent(pool, 5);

    ldb_pool_destroy(pool);
  }
#endif /* _WIN32 || LDB_PTHREAD */

  return 0;
}
