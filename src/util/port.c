/*!
 * port.c - ported functions for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#if defined(_WIN32)
#  include "port_win_impl.h"
#else
#  include "port_unix_impl.h"
#endif
