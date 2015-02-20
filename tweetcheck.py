#!/usr/bin/env python3.4

"""
Examine a tweet stream and compute distributions of attributes.
The tweet stream should be a sequence of tweets in JSON format.
"""

from collections import Counter
import json

# #### Implement command-line options!

STREAM = "results/stream.json"          # or "search.json"

with open(STREAM) as stream:            # #### Need to handle Twitter API too.
    s = stream.read()                   # #### Need online algorithm.

decoder = json.JSONDecoder()
tweet, start = decoder.raw_decode(s)
#tweets = [tweet]
start = start + 1                       # skip NL
end = len(s)

tweet_attributes = Counter(tweet.keys())
attribute_counts = {}
for k in tweet.keys():
    try:
        attribute_counts[k] = Counter(tweet[k].keys())
    except AttributeError:
        pass                            # #### Should get distributions here
                                        # But text, timestamps no point.

while True:
    try:
        tweet, offset = decoder.raw_decode(s[start:]) # #### Horrible consing?
        start = start + offset + 1
    except Exception as e:
        print("Error:", e)
        print("|", s[start:start+5000], sep='')
        print("len(s) =", end, "start = ", start, "len(tweets) =", len(tweets))
        print("len(tweets) should be 1000.")
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
    
    #tweets.append(tweet)
    if start >= end: break

print(tweet_attributes)
for k in attribute_counts.keys():
    print("key:", k)
    print(attribute_counts[k])
