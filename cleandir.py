#! /usr/bin/env python3.4

import os
import stat

ERROR = -1
NONEXISTENT = 0
KEPT = 1
REMOVED = 2
template = "Files #{0:d} to #{1:d} were {2:s}"
status_names = ("nonexistent", "kept", "removed")

def cleanfile(path):
    val = ERROR
    if os.path.exists(path):
        s = os.stat(path)
        sz = s[stat.ST_SIZE]
        if sz == 0:
            os.unlink(fn)
            val = REMOVED
        else:
            val = KEPT
    else:
        val = NONEXISTENT
    return val

lastchange = None
old_status = None
for i in range(0, 1500000):
    print("{0:d}: ".format(i), end="")
    fn = "stream-results-{0:d}.json".format(i)
    status = cleanfile(os.path.join(os.getcwd(), fn))
    if old_status is None:
        lastchange = i
        oldstatus = status
    elif old_status == status:
        pass
    else:
        if status == ERROR:
            print("INTERNAL ERROR PROCESSING {0:s}".format(fn))
            status = old_status          # Hack!
        else:
            print(template.format(lastchange, i, status_names[old_status]))
        lastchange = i
        old_status = status
print(template.format(lastchange, i, status_names[old_status]))
