#! /opt/local/bin/python3.4

# test setup
import sys
sys.path.append("..")

# test data
sys.argv[1:] = [
    "data/20151122.215510/stream-results-2.json",
    "data/20151122.215510/stream-results-1.json",
    "data/20151122.215510/stream-results-0.json",
    ]

# test scaffold
from partition_tweets import (
    desired_fields,
    json_source,
    parse_command_line,
    prune_dict,
    )

sources = parse_command_line()

# one tweet (no retweeted_status)
for tweet in json_source([sources[0]]):
    prune_dict(tweet, desired_fields)
    
# one retweet (has retweeted_status)
for tweet in json_source([sources[1]]):
    prune_dict(tweet, desired_fields)

# three files, total 169 tweets
for tweet in json_source(sources):
    prune_dict(tweet, desired_fields)

# #### This test produces no output and no exceptions.
