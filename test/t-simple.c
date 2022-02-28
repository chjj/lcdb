#undef NDEBUG

#include <assert.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <rdb.h>

int
main(void) {
  rdb_slice_t key, val;
  rdb_t *db;
  int rc;

  key.data = (void *)"hello";
  key.size = 5;

  val.data = (void *)"world";
  val.size = 5;

  rc = rdb_open("tmp", 0, &db);

  assert(rc == RDB_OK);

  rc = rdb_put(db, &key, &val, 0);

  assert(rc == RDB_OK);

  val.data = NULL;
  val.size = 0;

  rc = rdb_get(db, &key, &val, 0);

  assert(rc == RDB_OK);
  assert(val.size == 5);
  assert(memcmp(val.data, "world", 5) == 0);

  rdb_free(val.data);

  rdb_close(db);

  return 0;
}
