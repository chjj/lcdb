#undef NDEBUG

#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <rdb.h>

int rdb_env_clear(void);

int
main(void) {
  rdb_dbopt_t opt = *rdb_dbopt_default;
  rdb_slice_t key, val;
  rdb_t *db;
  int rc;

  opt.create_if_missing = 1;

  key.data = (void *)"hello";
  key.size = 5;

  val.data = (void *)"world";
  val.size = 5;

  rc = rdb_open("tmp", &opt, &db);

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

  {
    rc = rdb_open("tmp", &opt, &db);

    assert(rc == RDB_OK);

    val.data = NULL;
    val.size = 0;

    rc = rdb_get(db, &key, &val, 0);

    assert(rc == RDB_OK);
    assert(val.size == 5);
    assert(memcmp(val.data, "world", 5) == 0);

    rdb_free(val.data);

    rdb_close(db);
  }

  rdb_env_clear();

  return 0;
}
