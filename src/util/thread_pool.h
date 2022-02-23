/*!
 * thread_pool.h - thread pool for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_THREAD_POOL_H
#define RDB_THREAD_POOL_H

/*
 * Types
 */

typedef void rdb_work_f(void *arg);
typedef struct rdb_pool_s rdb_pool_t;

/*
 * Workers
 */

rdb_pool_t *
rdb_pool_create(int threads);

void
rdb_pool_destroy(rdb_pool_t *pool);

void
rdb_pool_schedule(rdb_pool_t *pool, rdb_work_f *func, void *arg);

void
rdb_pool_wait(rdb_pool_t *pool);

#endif /* RDB_THREAD_POOL_H */
