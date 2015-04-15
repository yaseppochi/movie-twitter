#! /usr/bin/env python3.4

import os
import stat

for i in range(1000000, 1000001):
    fn = "stream-results-{0:d}.json".format(i)
    s = os.stat(f)
    sz = s[ST_SIZE]
    print("{0:s} has size {1:d}: ").format(fn, sz, end='')
    if sz == 0:
        print("removing")
        print("os.unlink(fn)")
    else:
        print("keeping")
