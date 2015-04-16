#! /usr/bin/env python3.4

import os
import stat

for i in range(10000, 999999):
    fn = os.path.join(os.getcwd(), "stream-results-{0:d}.json".format(i))
    if os.path.exists(fn):
        s = os.stat(fn)
        sz = s[stat.ST_SIZE]
        print("{0:s} has size {1:d}: ".format(fn, sz), end='')
        if sz == 0:
            print("removing")
            os.unlink(fn)
        else:
            print("keeping")
    else:
        print("{0:s} does not exist")
