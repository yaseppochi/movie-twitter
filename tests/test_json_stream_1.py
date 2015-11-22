#! /opt/local/bin/python3.4

# test setup
import sys
sys.path.append("..")
from collections import Counter

# test data
datafiles = [
    "data/20151122.215510/stream-results-0.json",
    "data/20151122.215510/stream-results-1.json",
    "data/20151122.215510/stream-results-2.json",
    ]

# test scaffold
from partition_tweets import json_stream

def count_keys(files):
    print("Determining key distribution for files:")
    for f in files:
        print(f)
    tweets = json_stream(files)
    tdist = Counter()
    rtdist = Counter()
    udist = Counter()
    for tweet in tweets:
        for key in tweet:
            tdist[key] += 1
        if 'retweeted_status' in tweet:
            for key in tweet['retweeted_status']:
                rtdist[key] += 1
        if 'user' in tweet:
            for key in tweet['user']:
                udist[key] += 1
    print("The distribution of status keys in", len(files), "files is:")
    for key in sorted(list(tdist.keys())):
        print(" ", key, ":", tdist[key])
    print("The distribution of user keys in",
          len(files), "files is:")
    for key in sorted(list(udist.keys())):
        print(" ", key, ":", udist[key])
    print("The distribution of retweeted_status keys in",
          len(files), "files is:")
    for key in sorted(list(rtdist.keys())):
        print(" ", key, ":", rtdist[key])

for i in range(3):
    count_keys(datafiles[2 - i : 3 - i])

count_keys(datafiles)

