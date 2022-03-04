/*!
 * rbt_test.c - red-black tree test for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#undef NDEBUG

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "extern.h"
#include "rbt.h"

RDB_EXTERN int
rdb_test_rbt(void);

static int
my_compare(rb_val_t x, rb_val_t y, void *arg) {
  (void)arg;
  return strcmp(x.p, y.p);
}

static void
my_clear(rb_node_t *node) {
  free(node->key.p);
}

int
rdb_test_rbt(void) {
  rb_tree_t tree;
  void *item;
  int i;

  rb_set_init(&tree, my_compare, NULL);

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
