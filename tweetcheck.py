#!/usr/bin/env python3.4

"""
Examine a tweet stream and compute distributions of attributes.
The tweet stream should be a sequence of tweets in JSON format.
"""

# You can ignore comments beginning with "TODO:".

from collections import Counter
import argparse
import json

parser = argparse.ArgumentParser(description="Examine a file of tweets (JSON)")
parser.add_argument('STREAM', type=str, help="File of JSON tweets")
args = parser.parse_args()

with open(args.STREAM) as stream:       # TODO: Need to handle Twitter API too.
    s = stream.read()                   # TODO: Need online algorithm.

decoder = json.JSONDecoder()
tweet, start = decoder.raw_decode(s)
start = start + 1                       # skip NL
end = len(s)

tweet_attributes = Counter(tweet.keys())
attribute_counts = {}
for k in tweet.keys():
    try:
        attribute_counts[k] = Counter(tweet[k].keys())
    except AttributeError:
        pass                            # TODO: Should get distributions here
                                        # But text, timestamps no point.

while True:
    try:
        tweet, offset = decoder.raw_decode(s[start:start+50000])
        start = start + offset + 1
    except Exception as e:
        print("Error:", e)
        print("|", s[start:start+5000], sep='')
        print("len(s) =", end, "start = ", start)
        if start >= len(s):
            break
        elif s[start] == '\n':
            start = start + 1
            continue
        else:
            break
    try:
        tweet_attributes += Counter(tweet.keys())
    except KeyError:
        tweet_attributes = Counter(tweet.keys())
    for k in tweet.keys():
        try:
            attribute_counts[k] += Counter(tweet[k].keys())
        except Exception:
            pass                  # See comment above.
    
    if start >= end: break

print(tweet_attributes)
for k in attribute_counts.keys():
    print("key:", k)
    print(attribute_counts[k])
