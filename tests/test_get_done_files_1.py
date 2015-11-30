# test setup
import sys
sys.path.append("..")

import os.path
from partition_tweets import get_done_files

log = "/var/local/twitterdb/source.log"

if not os.path.exists(log):
    print(log, "not found.  Are you on the right host?")
else:
    done_sources = list(get_done_files(log))
    done_sources.sort()
    print("There are ", len(done_sources), " done entries in ", log, ".",
          sep="")
    print("First five:")
    for i in range(5):
        print("   ", done_sources[i])
    print("Last five:")
    for i in range(5, 0, -1):
        print("   ", done_sources[-i])
