#! /opt/local/bin/python3.4

# test setup
import sys
sys.path.append("..")

# test data
datafiles = [
    "data/20151122.215510/stream-results-0.json",
    "data/20151122.215510/stream-results-1.json",
    "data/20151122.215510/stream-results-2.json",
    ]

# test scaffold
from partition_tweets import (
    json_stream,
    count_keys,
    )

def print_dists(files):
    print("Determining key distribution for files:")
    for f in files:
        print(f)

    tdist, udist, rtdist = count_keys(json_stream(files))

    print("The distribution of status keys is:")
    for key in sorted(list(tdist.keys())):
        print(" ", key, ":", tdist[key])
    print("The distribution of user keys is:")
    for key in sorted(list(udist.keys())):
        print(" ", key, ":", udist[key])
    print("The distribution of retweeted_status keys is:")
    for key in sorted(list(rtdist.keys())):
        print(" ", key, ":", rtdist[key])

for i in range(3):
    print_dists(datafiles[2 - i : 3 - i])

print_dists(datafiles)

