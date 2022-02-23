/*!
 * env.c - platform-specific functions for rdb
 * Copyright (c) 2022, Christopher Jeffrey (MIT License).
 * https://github.com/chjj/rdb
 */

#if defined(_WIN32)
#  include "env_win_impl.h"
#else
#  include "env_unix_impl.h"
#endif
