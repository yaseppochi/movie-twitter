#!/usr/bin/env python3.4

"""
Examine a tweet stream and compute distributions of attributes.
The tweet stream should be a sequence of tweets in JSON format.
"""

import json

# #### Implement command-line options!

STREAM = "results/stream.json"          # or "search.json"

with open(STREAM) as stream:            # #### Need to handle Twitter API too.
    s = stream.read()                   # #### Need online algorithm.

decoder = json.JSONDecoder()
tweet, start = decoder.raw_decode(s)
tweets = [tweet]

while True:
    try:
        tweet, start = decoder.raw_decode(s[start:]) # #### Horrible consing?
    except Exception as e:
        print("Error:", e)
        break

print("len(s) =", len(s), "start = ", start, "len(tweets) =", len(tweets))
print(tweets[0])
