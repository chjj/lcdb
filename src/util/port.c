/*!
 * port.c - ported functions for lcdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/lcdb
 *
 * Parts of this software are based on google/leveldb:
 *   Copyright (c) 2011, The LevelDB Authors. All rights reserved.
 *   https://github.com/google/leveldb
 *
 * See LICENSE for more information.
 */

#if defined(_WIN32)
#  include "port_win_impl.h"
#elif defined(LDB_PTHREAD)
#  include "port_unix_impl.h"
#else
#  include "port_none_impl.h"
#endif
