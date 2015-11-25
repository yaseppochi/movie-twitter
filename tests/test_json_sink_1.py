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
    CLOSE_REQUEST,
    desired_fields,
    json_sink,
    json_source,
    parse_command_line,
    prune_dict,
    )

sources = parse_command_line()

# one tweet (no retweeted_status)
out = json_sink("test_json_sink_1.0.json")
next(out)
for tweet in json_source([sources[0]]):
    out.send(prune_dict(tweet, desired_fields))
    out.send('\n')
else:
    out.close()
    
# one retweet (has retweeted_status)
out = json_sink("test_json_sink_1.1.json")
next(out)
for tweet in json_source([sources[1]]):
    out.send(prune_dict(tweet, desired_fields))
    out.send('\n')
else:
    out.close()

# three files, total 169 tweets
out = json_sink("test_json_sink_1.all.json")
next(out)
for tweet in json_source(sources):
    out.send(prune_dict(tweet, desired_fields))
    out.send('\n')
else:
    out.close()


