/*!
 * rbt.h - red-black tree for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#ifndef RDB_RBT_H
#define RDB_RBT_H

#include <stddef.h>
#include <stdint.h>

/*
 * Types
 */

typedef union rb_val_s {
  void *p;
  int64_t si;
  uint64_t ui;
} rb_val_t;

typedef enum rb_color {
  RB_BLACK = 0,
  RB_RED = 1
} rb_color_t;

typedef struct rb_node_s {
  rb_val_t key;
  rb_val_t value;
  rb_color_t color;
  struct rb_node_s *parent;
  struct rb_node_s *left;
  struct rb_node_s *right;
} rb_node_t;

struct rb_tree_s;

typedef struct rb_iter_s {
  const struct rb_tree_s *tree;
  const rb_node_t *root;
  const rb_node_t *node;
} rb_iter_t;

typedef struct rb_tree_s {
  rb_node_t *root;
  int (*compare)(rb_val_t, rb_val_t, void *);
  void *arg;
  int unique;
  size_t size;
  rb_iter_t iter;
} rb_tree_t;

/*
 * Node
 */

void
rb_node_destroy(rb_node_t *node);

/*
 * Tree
 */

void
rb_tree_init(rb_tree_t *tree,
             int (*compare)(rb_val_t, rb_val_t, void *),
             void *arg,
             int unique);

void
rb_tree_clear(rb_tree_t *tree, void (*clear)(rb_node_t *));

rb_tree_t *
rb_tree_create(int (*compare)(rb_val_t, rb_val_t, void *),
               void *arg,
               int unique);

void
rb_tree_destroy(rb_tree_t *tree, void (*clear)(rb_node_t *));

void
rb_tree_reset(rb_tree_t *tree, void (*clear)(rb_node_t *));

const rb_node_t *
rb_tree_search(const rb_tree_t *tree, rb_val_t key);

rb_node_t *
rb_tree_insert(rb_tree_t *tree, rb_val_t key, rb_val_t value);

rb_node_t *
rb_tree_remove(rb_tree_t *tree, rb_val_t key);

rb_iter_t
rb_tree_iterator(const rb_tree_t *tree);

#define rb_tree_iterate(t, k, v) \
  rb_iter_iterate(t, (rb_iter_t *)&(t)->iter, k, v)

#define rb_tree_keys(t, k) rb_iter_keys(t, (rb_iter_t *)&(t)->iter, k)
#define rb_tree_values(t, v) rb_iter_values(t, (rb_iter_t *)&(t)->iter, v)

/*
 * Iterator
 */

void
rb_iter_init(rb_iter_t *iter, const rb_tree_t *tree);

int
rb_iter_compare(const rb_iter_t *iter, rb_val_t key);

int
rb_iter_valid(const rb_iter_t *iter);

void
rb_iter_reset(rb_iter_t *iter);

void
rb_iter_seek_first(rb_iter_t *iter);

void
rb_iter_seek_last(rb_iter_t *iter);

void
rb_iter_seek_min(rb_iter_t *iter, rb_val_t key);

void
rb_iter_seek_max(rb_iter_t *iter, rb_val_t key);

void
rb_iter_seek(rb_iter_t *iter, rb_val_t key);

int
rb_iter_prev(rb_iter_t *iter);

int
rb_iter_next(rb_iter_t *iter);

rb_val_t
rb_iter_key(const rb_iter_t *iter);

rb_val_t
rb_iter_value(const rb_iter_t *iter);

int
rb_iter_start(rb_iter_t *iter, const rb_tree_t *tree);

int
rb_iter_kv(rb_iter_t *iter, rb_val_t *key, rb_val_t *value);

int
rb_iter_k(rb_iter_t *iter, rb_val_t *key);

int
rb_iter_v(rb_iter_t *iter, rb_val_t *value);

#define rb_iter_iterate(t, it, k, v) \
  for (rb_iter_start(it, t); rb_iter_kv(it, &(k), &(v)); rb_iter_next(it))

#define rb_iter_keys(t, it, k) \
  for (rb_iter_start(it, t); rb_iter_k(it, &(k)); rb_iter_next(it))

#define rb_iter_values(t, it, v) \
  for (rb_iter_start(it, t); rb_iter_v(it, &(v)); rb_iter_next(it))

/*
 * Set64
 */

void
rb_set64_init(rb_tree_t *tree);

void
rb_set64_clear(rb_tree_t *tree);

int
rb_set64_has(rb_tree_t *tree, uint64_t item);

int
rb_set64_put(rb_tree_t *tree, uint64_t item);

int
rb_set64_del(rb_tree_t *tree, uint64_t item);

int
rb_set64_k(rb_iter_t *iter, uint64_t *key);

#define rb__set64_keys(t, it, k) \
  for (rb_iter_start(it, t); rb_set64_k(it, &(k)); rb_iter_next(it))

#define rb_set64_iterate(t, k) rb__set64_keys(t, (rb_iter_t *)&(t)->iter, k)

/*
 * Set
 */

void
rb_set_init(rb_tree_t *tree,
            int (*compare)(rb_val_t, rb_val_t, void *),
            void *arg);

void
rb_set_clear(rb_tree_t *tree, void (*clear)(rb_node_t *));

int
rb_set_has(rb_tree_t *tree, const void *item);

int
rb_set_put(rb_tree_t *tree, const void *item);

void *
rb_set_del(rb_tree_t *tree, const void *item);

int
rb_set_k(rb_iter_t *iter, void **key);

#define rb__set_keys(t, it, k) \
  for (rb_iter_start(it, t); rb_set_k(it, &(k)); rb_iter_next(it))

#define rb_set_iterate(t, k) rb__set_keys(t, (rb_iter_t *)&(t)->iter, k)

#endif /* RDB_RBT_H */
