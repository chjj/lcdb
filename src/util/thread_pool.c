/*!
 * thread_pool.c - thread pool for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <stdlib.h>
#include <string.h>
#include "internal.h"
#include "port.h"
#include "thread_pool.h"

/*
 * Work
 */

typedef struct rdb_work_s {
  rdb_work_f *func;
  void *arg;
  struct rdb_work_s *next;
} rdb_work_t;

static rdb_work_t *
rdb_work_create(rdb_work_f *func, void *arg) {
  rdb_work_t *work = rdb_malloc(sizeof(rdb_work_t));

  work->func = func;
  work->arg = arg;
  work->next = NULL;

  return work;
}

static rdb_work_t *
rdb_work_destroy(rdb_work_t *work) {
  rdb_work_t *next = work->next;
  rdb_free(work);
  return next;
}

static rdb_work_t *
rdb_work_execute(rdb_work_t *work) {
  work->func(work->arg);
  return rdb_work_destroy(work);
}

/*
 * Work Queue
 */

typedef struct rdb_queue_s {
  rdb_work_t *head;
  rdb_work_t *tail;
  int length;
} rdb_queue_t;

static void
rdb_queue_init(rdb_queue_t *queue) {
  queue->head = NULL;
  queue->tail = NULL;
  queue->length = 0;
}

static void
rdb_queue_clear(rdb_queue_t *queue) {
  rdb_work_t *work, *next;

  for (work = queue->head; work != NULL; work = next)
    next = rdb_work_destroy(work);

  rdb_queue_init(queue);
}

static void
rdb_queue_push(rdb_queue_t *queue, rdb_work_f *func, void *arg) {
  rdb_work_t *work = rdb_work_create(func, arg);

  if (queue->head == NULL)
    queue->head = work;

  if (queue->tail != NULL)
    queue->tail->next = work;

  queue->tail = work;
  queue->length++;
}

static rdb_work_t *
rdb_queue_shift(rdb_queue_t *queue) {
  rdb_work_t *work = queue->head;

  if (work == NULL)
    abort(); /* LCOV_EXCL_LINE */

  queue->head = work->next;

  if (queue->head == NULL)
    queue->tail = NULL;

  queue->length--;

  work->next = NULL;

  return work;
}

/*
 * Workers
 */

struct rdb_pool_s {
  rdb_mutex_t mutex;
  rdb_cond_t master;
  rdb_cond_t worker;
  rdb_queue_t queue;
  int threads;
  int running;
  int left;
  int stop;
};

static void
worker_thread(void *arg);

rdb_pool_t *
rdb_pool_create(int threads) {
  rdb_pool_t *pool = rdb_malloc(sizeof(rdb_pool_t));

  if (threads < 1)
    threads = 1;

  rdb_mutex_init(&pool->mutex);
  rdb_cond_init(&pool->master);
  rdb_cond_init(&pool->worker);
  rdb_queue_init(&pool->queue);

  pool->threads = threads;
  pool->running = 0;
  pool->left = 0;
  pool->stop = 0;

  return pool;
}

void
rdb_pool_destroy(rdb_pool_t *pool) {
  rdb_mutex_lock(&pool->mutex);

  rdb_queue_clear(&pool->queue);

  pool->stop = 1;

  rdb_cond_broadcast(&pool->worker);
  rdb_mutex_unlock(&pool->mutex);

  rdb_mutex_lock(&pool->mutex);

  while (pool->running > 0)
    rdb_cond_wait(&pool->master, &pool->mutex);

  rdb_mutex_unlock(&pool->mutex);

  rdb_mutex_destroy(&pool->mutex);
  rdb_cond_destroy(&pool->worker);
  rdb_cond_destroy(&pool->master);

  rdb_free(pool);
}

void
rdb_pool_schedule(rdb_pool_t *pool, rdb_work_f *func, void *arg) {
  rdb_mutex_lock(&pool->mutex);

  if (pool->running == 0) {
    rdb_thread_t thread;
    int i;

    pool->running = pool->threads;

    for (i = 0; i < pool->threads; i++) {
      rdb_thread_create(&thread, worker_thread, pool);
      rdb_thread_detach(&thread);
    }
  }

  rdb_queue_push(&pool->queue, func, arg);

  pool->left++;

  rdb_cond_signal(&pool->worker);
  rdb_mutex_unlock(&pool->mutex);
}

void
rdb_pool_wait(rdb_pool_t *pool) {
  rdb_mutex_lock(&pool->mutex);

  while (pool->left > 0)
    rdb_cond_wait(&pool->master, &pool->mutex);

  rdb_mutex_unlock(&pool->mutex);
}

static void
worker_thread(void *arg) {
  rdb_pool_t *pool = arg;
  rdb_work_t *work;
  int ran = 0;

  for (;;) {
    rdb_mutex_lock(&pool->mutex);

    if (ran) {
      pool->left--;

      if (!pool->stop && pool->left == 0)
        rdb_cond_signal(&pool->master);
    }

    while (!pool->stop && pool->queue.length == 0)
      rdb_cond_wait(&pool->worker, &pool->mutex);

    if (pool->stop)
      break;

    work = rdb_queue_shift(&pool->queue);
    ran = 1;

    rdb_mutex_unlock(&pool->mutex);

    rdb_work_execute(work);
  }

  if (--pool->running == 0)
    rdb_cond_signal(&pool->master);

  rdb_mutex_unlock(&pool->mutex);
}
