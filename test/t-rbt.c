/*!
 * t-rbt.c - red-black tree test for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 */

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "util/internal.h"
#include "util/random.h"
#include "util/rbt.h"
#include "util/testutil.h"

/*
 * Constants
 */

#define COUNT 1000
#define HALF (COUNT / 2)
#define ITEM_SIZE (sizeof(struct item_s))

/*
 * Types
 */

struct item_s {
  char *key;
  uint32_t val;
  char *ptr;
};

/*
 * Helpers
 */

static char *
test_strdup(const char *xp) {
  size_t xn = strlen(xp);
  return memcpy(ldb_malloc(xn + 1), xp, xn + 1);
}

static int
compare_string(rb_val_t x, rb_val_t y, void *arg) {
  (void)arg;
  return strcmp(x.ptr, y.ptr);
}

static void
clear_string(rb_node_t *node) {
  free(node->key.ptr);
}

static void
clear_strings(rb_node_t *node) {
  free(node->key.ptr);
  free(node->val.ptr);
}

static void
copy_string(rb_node_t *z, const rb_node_t *x) {
  z->key.ptr = test_strdup(x->key.ptr);
}

static void
copy_strings(rb_node_t *z, const rb_node_t *x) {
  z->key.ptr = test_strdup(x->key.ptr);
  z->val.ptr = test_strdup(x->val.ptr);
}

static int
item_qsort(const void *x, const void *y) {
  const struct item_s *a = x;
  const struct item_s *b = y;
  return strcmp(a->key, b->key);
}

static int
string_qsort(const void *x, const void *y) {
  return strcmp(*(const char **)x, *(const char **)y);
}

static int
integer_qsort(const void *x, const void *y) {
  uint64_t a = *(const uint64_t *)x;
  uint64_t b = *(const uint64_t *)y;
  return LDB_CMP(a, b);
}

static char *
random_string(ldb_rand_t *rnd, size_t len) {
  char *str = ldb_malloc(len + 1);
  size_t i;

  for (i = 0; i < len; i++)
    str[i] = ' ' + ldb_rand_uniform(rnd, 95);

  str[len] = '\0';

  return str;
}

/*
 * Tree API
 */

static void
test_tree_api(void) {
  struct item_s *items, *sorted;
  rb_tree_t tree, copy;
  ldb_rand_t rnd;
  int total = 0;
  rb_iter_t it;
  int i;

  items = ldb_malloc(COUNT * ITEM_SIZE);
  sorted = ldb_malloc(COUNT * ITEM_SIZE);

  ldb_rand_init(&rnd, 301);

  rb_tree_init(&tree, compare_string, NULL);

  /* Populate data. */
  while (total < COUNT) {
    size_t len = 20 + ldb_rand_uniform(&rnd, 108);
    char *key = random_string(&rnd, len);
    uint32_t val = ldb_rand_next(&rnd);
    struct item_s *item;
    rb_node_t *node;

    if (!rb_tree_put(&tree, rb_ptr(key), &node)) {
      ldb_free(key);
      continue;
    }

    node->val = rb_ui(val);

    item = &items[total++];
    item->key = key;
    item->val = val;
    item->ptr = NULL;
  }

  ASSERT(tree.size == COUNT);

  /* Sort data. */
  memcpy(sorted, items, COUNT * ITEM_SIZE);
  qsort(sorted, COUNT, ITEM_SIZE, item_qsort);

  /* Insert duplicates. */
  for (i = 0; i < COUNT; i++) {
    rb_node_t *node;

    ASSERT(!rb_tree_put(&tree, rb_ptr(items[i].key), &node));
    ASSERT(strcmp(node->key.ptr, items[i].key) == 0);
    ASSERT(node->val.ui == items[i].val);
  }

  ASSERT(tree.size == COUNT);

  /* Random read. */
  for (i = 0; i < COUNT; i++) {
    const rb_node_t *node = rb_tree_get(&tree, rb_ptr(items[i].key));

    ASSERT(node != NULL);
    ASSERT(rb_tree_has(&tree, rb_ptr(items[i].key)));
    ASSERT(strcmp(node->key.ptr, items[i].key) == 0);
    ASSERT(node->val.ui == items[i].val);
  }

  /* Random read/delete (non-existent). */
  for (i = 0; i < COUNT; i++) {
    size_t len = ldb_rand_uniform(&rnd, 20);
    char *key = random_string(&rnd, len);

    ASSERT(rb_tree_get(&tree, rb_ptr(key)) == NULL);
    ASSERT(!rb_tree_has(&tree, rb_ptr(key)));
    ASSERT(!rb_tree_del(&tree, rb_ptr(key), NULL));

    ldb_free(key);
  }

  ASSERT(tree.size == COUNT);

  /* Sequential read (forward). */
  it = rb_tree_iterator(&tree);
  i = 0;

  for (rb_iter_first(&it); rb_iter_valid(&it); rb_iter_next(&it)) {
    ASSERT(i < COUNT);
    ASSERT(strcmp(rb_iter_key(&it).ptr, sorted[i].key) == 0);
    ASSERT(rb_iter_val(&it).ui == sorted[i].val);
    i++;
  }

  ASSERT(i == COUNT);

  /* Sequential read (backwards). */
  it = rb_tree_iterator(&tree);
  i = COUNT - 1;

  for (rb_iter_last(&it); rb_iter_valid(&it); rb_iter_prev(&it)) {
    ASSERT(i >= 0);
    ASSERT(strcmp(rb_iter_key(&it).ptr, sorted[i].key) == 0);
    ASSERT(rb_iter_val(&it).ui == sorted[i].val);
    i--;
  }

  ASSERT(i == -1);

  /* Sequential read (seek). */
  it = rb_tree_iterator(&tree);
  i = COUNT / 2;

  for (rb_iter_seek(&it, rb_ptr(sorted[i].key));
       rb_iter_valid(&it);
       rb_iter_next(&it)) {
    ASSERT(i < COUNT);
    ASSERT(strcmp(rb_iter_key(&it).ptr, sorted[i].key) == 0);
    ASSERT(rb_iter_val(&it).ui == sorted[i].val);
    i++;
  }

  ASSERT(i == COUNT);

  /* Deletion. */
  for (i = 0; i < HALF; i++) {
    rb_node_t node;

    ASSERT(rb_tree_del(&tree, rb_ptr(items[i].key), &node));
    ASSERT(strcmp(node.key.ptr, items[i].key) == 0);
    ASSERT(node.val.ui == items[i].val);

    ldb_free(node.key.ptr);
  }

  ASSERT(tree.size == HALF);

  /* Reorganize. */
  memcpy(items, items + HALF, HALF * ITEM_SIZE);
  memcpy(sorted, items, HALF * ITEM_SIZE);
  qsort(sorted, HALF, ITEM_SIZE, item_qsort);

  /* Verify deletion. */
  i = 0;

  rb_tree_each(&tree, it) {
    ASSERT(i < HALF);
    ASSERT(strcmp(rb_key_ptr(it), sorted[i].key) == 0);
    ASSERT(rb_val_ui(it) == sorted[i].val);
    i++;
  }

  ASSERT(i == HALF);

  /* Snapshot. */
  rb_tree_copy(&copy, &tree, copy_string);

  /* Verify snapshot. */
  i = 0;

  rb_tree_each(&copy, it) {
    ASSERT(i < HALF);
    ASSERT(strcmp(rb_key_ptr(it), sorted[i].key) == 0);
    ASSERT(rb_val_ui(it) == sorted[i].val);
    i++;
  }

  ASSERT(i == HALF);

  /* Clear. */
  rb_tree_clear(&tree, clear_string);
  rb_tree_clear(&copy, clear_string);

  /* Cleanup. */
  ldb_free(items);
  ldb_free(sorted);
}

/*
 * Map API
 */

static void
test_map_api(void) {
  struct item_s *items, *sorted;
  rb_map_t map, copy;
  ldb_rand_t rnd;
  int total = 0;
  rb_iter_t it;
  int i;

  items = ldb_malloc(COUNT * ITEM_SIZE);
  sorted = ldb_malloc(COUNT * ITEM_SIZE);

  ldb_rand_init(&rnd, 301);

  rb_map_init(&map, compare_string, NULL);

  /* Populate data. */
  while (total < COUNT) {
    size_t len = 20 + ldb_rand_uniform(&rnd, 108);
    char *key = random_string(&rnd, len);
    char *val = random_string(&rnd, 32);
    struct item_s *item;

    if (!rb_map_put(&map, key, val)) {
      ldb_free(key);
      ldb_free(val);
      continue;
    }

    item = &items[total++];
    item->key = key;
    item->val = 0;
    item->ptr = val;
  }

  ASSERT(map.size == COUNT);

  /* Sort data. */
  memcpy(sorted, items, COUNT * ITEM_SIZE);
  qsort(sorted, COUNT, ITEM_SIZE, item_qsort);

  /* Insert duplicates. */
  for (i = 0; i < COUNT; i++)
    ASSERT(!rb_map_put(&map, items[i].key, items[i].ptr));

  ASSERT(map.size == COUNT);

  /* Random read. */
  for (i = 0; i < COUNT; i++) {
    const char *val = rb_map_get(&map, items[i].key);

    ASSERT(val != NULL);
    ASSERT(rb_map_has(&map, items[i].key));
    ASSERT(strcmp(val, items[i].ptr) == 0);
  }

  /* Random read/delete (non-existent). */
  for (i = 0; i < COUNT; i++) {
    size_t len = ldb_rand_uniform(&rnd, 20);
    char *key = random_string(&rnd, len);

    ASSERT(rb_map_get(&map, key) == NULL);
    ASSERT(!rb_map_has(&map, key));
    ASSERT(!rb_map_del(&map, key, NULL));

    ldb_free(key);
  }

  ASSERT(map.size == COUNT);

  /* Sequential read. */
  i = 0;

  rb_map_each(&map, it) {
    ASSERT(i < COUNT);
    ASSERT(strcmp(rb_key_ptr(it), sorted[i].key) == 0);
    ASSERT(strcmp(rb_val_ptr(it), sorted[i].ptr) == 0);
    i++;
  }

  ASSERT(i == COUNT);

  /* Deletion. */
  for (i = 0; i < HALF; i++) {
    rb_entry_t entry;

    ASSERT(rb_map_del(&map, items[i].key, &entry));
    ASSERT(strcmp(entry.key, items[i].key) == 0);
    ASSERT(strcmp(entry.val, items[i].ptr) == 0);

    ldb_free(entry.key);
    ldb_free(entry.val);
  }

  ASSERT(map.size == HALF);

  /* Reorganize. */
  memcpy(items, items + HALF, HALF * ITEM_SIZE);
  memcpy(sorted, items, HALF * ITEM_SIZE);
  qsort(sorted, HALF, ITEM_SIZE, item_qsort);

  /* Verify deletion. */
  i = 0;

  rb_map_each(&map, it) {
    ASSERT(i < HALF);
    ASSERT(strcmp(rb_key_ptr(it), sorted[i].key) == 0);
    ASSERT(strcmp(rb_val_ptr(it), sorted[i].ptr) == 0);
    i++;
  }

  ASSERT(i == HALF);

  /* Snapshot. */
  rb_map_copy(&copy, &map, copy_strings);

  /* Verify snapshot. */
  i = 0;

  rb_map_each(&copy, it) {
    ASSERT(i < HALF);
    ASSERT(strcmp(rb_key_ptr(it), sorted[i].key) == 0);
    ASSERT(strcmp(rb_val_ptr(it), sorted[i].ptr) == 0);
    i++;
  }

  ASSERT(i == HALF);

  /* Clear. */
  rb_map_clear(&map, clear_strings);
  rb_map_clear(&copy, clear_strings);

  /* Cleanup. */
  ldb_free(items);
  ldb_free(sorted);
}

/*
 * Set API
 */

static void
test_set_api(void) {
  char **items, **sorted;
  rb_set_t set, copy;
  ldb_rand_t rnd;
  int total = 0;
  rb_iter_t it;
  int i;

  items = ldb_malloc(COUNT * sizeof(char *));
  sorted = ldb_malloc(COUNT * sizeof(char *));

  ldb_rand_init(&rnd, 301);

  rb_set_init(&set, compare_string, NULL);

  /* Populate data. */
  while (total < COUNT) {
    size_t len = 20 + ldb_rand_uniform(&rnd, 108);
    char *key = random_string(&rnd, len);

    if (!rb_set_put(&set, key)) {
      ldb_free(key);
      continue;
    }

    items[total++] = key;
  }

  ASSERT(set.size == COUNT);

  /* Sort data. */
  memcpy(sorted, items, COUNT * sizeof(char *));
  qsort(sorted, COUNT, sizeof(char *), string_qsort);

  /* Insert duplicates. */
  for (i = 0; i < COUNT; i++)
    ASSERT(!rb_set_put(&set, items[i]));

  ASSERT(set.size == COUNT);

  /* Random read. */
  for (i = 0; i < COUNT; i++)
    ASSERT(rb_set_has(&set, items[i]));

  /* Random read/delete (non-existent). */
  for (i = 0; i < COUNT; i++) {
    size_t len = ldb_rand_uniform(&rnd, 20);
    char *key = random_string(&rnd, len);

    ASSERT(!rb_set_has(&set, key));
    ASSERT(rb_set_del(&set, key) == NULL);

    ldb_free(key);
  }

  ASSERT(set.size == COUNT);

  /* Sequential read. */
  i = 0;

  rb_set_each(&set, it) {
    ASSERT(i < COUNT);
    ASSERT(strcmp(rb_key_ptr(it), sorted[i]) == 0);
    i++;
  }

  ASSERT(i == COUNT);

  /* Deletion. */
  for (i = 0; i < HALF; i++) {
    char *key = rb_set_del(&set, items[i]);

    ASSERT(key != NULL);
    ASSERT(strcmp(key, items[i]) == 0);

    ldb_free(key);
  }

  ASSERT(set.size == HALF);

  /* Reorganize. */
  memcpy(items, items + HALF, HALF * sizeof(char *));
  memcpy(sorted, items, HALF * sizeof(char *));
  qsort(sorted, HALF, sizeof(char *), string_qsort);

  /* Verify deletion. */
  i = 0;

  rb_set_each(&set, it) {
    ASSERT(i < HALF);
    ASSERT(strcmp(rb_key_ptr(it), sorted[i]) == 0);
    i++;
  }

  ASSERT(i == HALF);

  /* Snapshot. */
  rb_set_copy(&copy, &set, copy_string);

  /* Verify snapshot. */
  i = 0;

  rb_set_each(&copy, it) {
    ASSERT(i < HALF);
    ASSERT(strcmp(rb_key_ptr(it), sorted[i]) == 0);
    i++;
  }

  ASSERT(i == HALF);

  /* Clear. */
  rb_set_clear(&set, clear_string);
  rb_set_clear(&copy, clear_string);

  /* Cleanup. */
  ldb_free(items);
  ldb_free(sorted);
}

/*
 * Set64 API
 */

static void
test_set64_api(void) {
  uint64_t *items, *sorted;
  rb_set64_t set, copy;
  ldb_rand_t rnd;
  int total = 0;
  rb_iter_t it;
  int i;

  items = ldb_malloc(COUNT * sizeof(uint64_t));
  sorted = ldb_malloc(COUNT * sizeof(uint64_t));

  ldb_rand_init(&rnd, 301);

  rb_set64_init(&set);

  /* Populate data. */
  while (total < COUNT) {
    uint64_t key = ldb_rand_next(&rnd);

    if (!rb_set64_put(&set, key))
      continue;

    items[total++] = key;
  }

  ASSERT(set.size == COUNT);

  /* Sort data. */
  memcpy(sorted, items, COUNT * sizeof(uint64_t));
  qsort(sorted, COUNT, sizeof(uint64_t), integer_qsort);

  /* Insert duplicates. */
  for (i = 0; i < COUNT; i++)
    ASSERT(!rb_set64_put(&set, items[i]));

  ASSERT(set.size == COUNT);

  /* Random read. */
  for (i = 0; i < COUNT; i++)
    ASSERT(rb_set64_has(&set, items[i]));

  /* Random read/delete (non-existent). */
  for (i = 0; i < COUNT; i++) {
    uint64_t key = ldb_rand_next(&rnd);

    ASSERT(!rb_set64_has(&set, key));
    ASSERT(!rb_set64_del(&set, key));
  }

  ASSERT(set.size == COUNT);

  /* Sequential read. */
  i = 0;

  rb_set64_each(&set, it) {
    ASSERT(i < COUNT);
    ASSERT(rb_key_ui(it) == sorted[i]);
    i++;
  }

  ASSERT(i == COUNT);

  /* Deletion. */
  for (i = 0; i < HALF; i++)
    ASSERT(rb_set64_del(&set, items[i]));

  ASSERT(set.size == HALF);

  /* Reorganize. */
  memcpy(items, items + HALF, HALF * sizeof(uint64_t));
  memcpy(sorted, items, HALF * sizeof(uint64_t));
  qsort(sorted, HALF, sizeof(uint64_t), integer_qsort);

  /* Verify deletion. */
  i = 0;

  rb_set64_each(&set, it) {
    ASSERT(i < HALF);
    ASSERT(rb_key_ui(it) == sorted[i]);
    i++;
  }

  ASSERT(i == HALF);

  /* Snapshot. */
  rb_set64_copy(&copy, &set);

  /* Verify snapshot. */
  i = 0;

  rb_set64_each(&copy, it) {
    ASSERT(i < HALF);
    ASSERT(rb_key_ui(it) == sorted[i]);
    i++;
  }

  ASSERT(i == HALF);

  /* Clear. */
  rb_set64_clear(&set);
  rb_set64_clear(&copy);

  /* Cleanup. */
  ldb_free(items);
  ldb_free(sorted);
}

static void
test_static_api(void) {
  static rb_set64_t set = RB_SET64_INIT;

  ASSERT(!rb_set64_has(&set, 1));
  ASSERT(!rb_set64_del(&set, 2));
  ASSERT(rb_set64_put(&set, 1));
  ASSERT(rb_set64_has(&set, 1));
  ASSERT(rb_set64_del(&set, 1));
  ASSERT(!rb_set64_has(&set, 1));
}

/*
 * Main
 */

int
main(void) {
  test_tree_api();
  test_map_api();
  test_set_api();
  test_set64_api();
  test_static_api();
  return 0;
}
