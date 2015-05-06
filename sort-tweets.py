#!/usr/bin/env python3.4

"""
Examine a tweet stream and sort by movie.
The tweet stream should be a sequence of tweets in JSON format.
"""

# You can ignore comments beginning with "TODO:".
# TODO: This program is based on tweetcheck.py.  Abstract the main loop.

from collections import Counter
from moviedata import MOVIES, PUNCT     # also STOPLIST when we get it
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

# Clean up movie names.
# TODO: Remove stoplist words.
movies = [m.translate(PUNCT) for m in MOVIES]
# Compute word->movie and movie->word maps.
movie_words = {}
word_movies = {}
for m in movies:
    movie_words[m] = [w.lower() for w in m.split()]
    for w in movie_words[m]:
        if w in word_movies:
            word_movies[w].append(m)
        else:
            word_movies[w] = [m]

tweet_movies = {}
count = 0
fail = 0
mismatch = 0
word_count = 0
movie_count = 0
while True:
    try:
        tweet, offset = decoder.raw_decode(s[start:start+50000])
        start = start + offset + 1
        count = count + 1
    except Exception as e:
        print("Error:", e)
        print("|", s[start:start+100])
        print("len(s) =", end, "start = ", start)
        # TODO: If the decoder raises, start doesn't get incremented.  So
        # there's nothing to do but bail out.
        break
    try:
        idno = tweet['id']
        text = tweet['text']
        # Get any other relevant items here (URLs, hashtags, other?)
    except KeyError:
        # We're missing essential data.  Try next tweet.
        fail = fail + 1
        continue

    tweet_movies[idno] = []
    if count < 10: print(idno)
    for m in movies:
        if count < 10: print(m)
        found = True
        for w in movie_words[m]:
            if count < 10: print(w)
            word_count += 1
            if not (w in text.lower()):
                mismatch += 1
                found = False
                break
        if found:
            movie_count += 1
            tweet_movies[idno].append(m)

print("{0:d} failures in {1:d} tweets.".format(fail, count))
print("{0:d} words matched.".format(word_count))
print("{0:d} movies matched.".format(movie_count))
print("{0:d} mismatched words tried.".format(mismatch))
idnos = sorted(tweet_movies.keys())
for idno in idnos:
    print("{0:d} ".format(idno), end='')
    for m in tweet_movies[idno]:
        print('"{0}"'.format(m), end=' ')
    print()
print(word_movies)
print(movie_words)
