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
  rdb_slice_t exp = rdb_string("world 999");
  rdb_dbopt_t opt = *rdb_dbopt_default;
  rdb_slice_t key, val;
  char key_buf[64];
  char val_buf[64];
  rdb_t *db;
  int i, rc;

  opt.create_if_missing = 1;

  rc = rdb_open("tmp", &opt, &db);

  assert(rc == RDB_OK);

  for (i = 0; i < 1000; i++) {
    sprintf(key_buf, "hello %d", rand());
    sprintf(val_buf, "world %d", i);

    key = rdb_string(key_buf);
    val = rdb_string(val_buf);

    rc = rdb_put(db, &key, &val, 0);

    assert(rc == RDB_OK);
  }

  val = rdb_slice(0, 0);

  rc = rdb_get(db, &key, &val, 0);

  assert(rc == RDB_OK);
  assert(rdb_compare(&val, &exp) == 0);
  /*
  assert(val.size == 9);
  assert(memcmp(val.data, "world 999", 9) == 0);
  */

  rdb_free(val.data);

  rdb_close(db);

  {
    rc = rdb_open("tmp", &opt, &db);

    assert(rc == RDB_OK);

    val = rdb_slice(0, 0);

    rc = rdb_get(db, &key, &val, 0);

    assert(rc == RDB_OK);
    assert(rdb_compare(&val, &exp) == 0);
    /*
    assert(val.size == 9);
    assert(memcmp(val.data, "world 999", 9) == 0);
    */

    rdb_free(val.data);

    rdb_close(db);
  }

  rdb_env_clear();

  return 0;
}
