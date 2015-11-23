#! /opt/local/bin/python3.4

# test setup
import sys
sys.path.append("..")

# test data
datafiles = [
    # Reverse order to correspond to tweet, retweet, many tweets.
    "data/20151122.215510/stream-results-2.json",
    "data/20151122.215510/stream-results-1.json",
    "data/20151122.215510/stream-results-0.json",
    ]

# test scaffold
from partition_tweets import (
    count_keys,
    json_source,
    parse_command_line,
    )

def print_dists(files):
    print("Determining key distribution for files:")
    for f in files:
        print(f)

    tdist, udist, rtdist = count_keys(json_source(files))

    print("The distribution of status keys is:")
    for key in sorted(list(tdist.keys())):
        print(" ", key, ":", tdist[key])
    print("The distribution of user keys is:")
    for key in sorted(list(udist.keys())):
        print(" ", key, ":", udist[key])
    print("The distribution of retweeted_status keys is:")
    for key in sorted(list(rtdist.keys())):
        print(" ", key, ":", rtdist[key])

sources = parse_command_line()

if not sources:
    sources = datafiles
    for i in range(3):
        # The slice is used because print_dists expects a list.
        print_dists(sources[i : i + 1])

print_dists(sources)

