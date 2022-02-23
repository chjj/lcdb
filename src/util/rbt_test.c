/*!
 * rbt_test.c - red-black tree tests for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "rbt.h"

/*
 * Testing
 */

static int my_cmp(rb_val_t x, rb_val_t y) {
  return memcmp(x.p, y.p, 10);
}

void just_testing(void) {
  rb_tree_t tree;
  void *item;
  rb_val_t k, v;

  rb_set_init(&tree, my_cmp);

  rb_set_iterate(&tree, item)
    puts((char *)item);

  rb_tree_iterate(&tree, k, v) {
    puts((char *)k.p);
    puts((char *)v.p);
  }

  rb_tree_keys(&tree, k)
    puts((char *)k.p);

  rb_tree_values(&tree, v)
    puts((char *)v.p);
}

static int my_compare(rb_val_t x, rb_val_t y) {
  return strcmp(x.p, y.p);
}

static void my_clear(rb_node_t *node) {
  free(node->key.p);
}

int main(void) {
  rb_tree_t tree;
  void *item;
  int i;

  rb_set_init(&tree, my_compare);

  for (i = 0; i < 1000; i++) {
    char *s = malloc(20 + 1);
    sprintf(s, "%d", i);
    assert(rb_set_put(&tree, s));
  }

  assert(tree.size == 1000);

  rb_set_iterate(&tree, item)
    puts((char *)item);

  for (i = 0; i < 1000; i += 2) {
    char s[21];
    sprintf(s, "%d", i);
    free(rb_set_del(&tree, s));
  }

  assert(tree.size == 500);

  rb_set_iterate(&tree, item)
    puts((char *)item);

  for (i = 0; i < 1000; i++) {
    char s[21];
    sprintf(s, "%d", i);
    assert(rb_set_has(&tree, s) == (i & 1));
  }

  rb_set_clear(&tree, my_clear);

  return 0;
}

#if 0
int main(void) {
  size_t total = 0;
  rb_tree_t tree;
  uint64_t item;
  int i;

  rb_set64_init(&tree);

  for (i = 0; i < 1000; i++)
    total += rb_set64_put(&tree, rand());

  assert(tree.size == total);

  rb_set64_iterate(&tree, item)
    printf("%d\n", (int)item);

  rb_set64_clear(&tree);

  return 0;
}
#endif
