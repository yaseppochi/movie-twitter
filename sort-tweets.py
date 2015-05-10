#!/usr/bin/env python3.4

"""
Examine a tweet stream and sort by movie.
The tweet stream should be a sequence of tweets in JSON format.
Output is a sorted list of tweet IDs with corresponding movies.
"""

# You can ignore comments beginning with "TODO:".
# TODO: This program is based on tweetcheck.py.  Abstract the main loop.

from moviedata import MOVIES, PUNCT     # also STOPLIST when we get it
import argparse
import json

# Set up the input file.  STREAM refers to future use with Twitter API.
parser = argparse.ArgumentParser(description="Examine a file of tweets (JSON)")
parser.add_argument('STREAM', type=str, help="File of JSON tweets")
args = parser.parse_args()
with open(args.STREAM) as stream:       # TODO: Need to handle Twitter API too.
    s = stream.read()                   # TODO: Need online algorithm.

# Set up the JSON decoder.
decoder = json.JSONDecoder()

# Get the first tweet, the start of the next tweet, and total size of file.
tweet, start = decoder.raw_decode(s)
start = start + 1                       # skip NL
end = len(s)

# Set parameters for data extraction.
optional_keys = ['urls', 'hashtags', 'retweet_count', 'favorite_count', 'lang']

# Clean up movie names.
# TODO: Remove stoplist words.
movies = [m.translate(PUNCT) for m in MOVIES]

# Compute word->movie and movie->word maps.
# Note that string "in" operator is case-sensitive, so we lowercase all words.
# Movie names are not searched for directly so they don't need lowercasing.
movie_words = {}
word_movies = {}
for m in movies:
    movie_words[m] = [w.lower() for w in m.split()]
    for w in movie_words[m]:
        if w in word_movies:
            word_movies[w].append(m)
        else:
            word_movies[w] = [m]

# Accumulate some statistics about the file.
object_count = 0                        # If there is an error in the JSON
                                        # parse, this may not be all of the
                                        # objects in the file.  Check for
                                        # start close to or greater than end
                                        # in output.
not_tweet_count = 0                     # Valid JSON object but not a tweet.
                                        # Eg, an "end connection" message.
word_count = 0
movie_count = 0

# #### Combine these.
tweet_movies = {}
tweet_data = {}
while True:
    try:
        tweet, offset = decoder.raw_decode(s[start:start+50000])
        start = start + offset + 1
        object_count = object_count + 1
    except Exception as e:
        # Currently there's no real point in printing the exception,
        # since we bail out in any case.
        # print("Error:", e)
        # print("|", s[start:start+100])
        print("next = ", start, "end =", end)
        # TODO: If the decoder raises, start doesn't get incremented.  So
        # there's nothing to do but bail out.
        break
    try:
        pruned = {}
        pruned['id'] = idno = tweet['id']
        pruned['text'] = text = tweet['text']
        pruned['created_at'] = tweet['created_at']
        # Get any other relevant items here (URLs, hashtags, other?)
        for k in optional_keys:
            # #### For dataset creation, probably should insert NULLs.
            if k in tweet:
                pruned[k] = tweet[k]
        tweet_data[idno] = pruned
    except KeyError:
        # We're missing essential data.  Try next tweet.
        not_tweet_count = not_tweet_count + 1
        continue

    tweet_movies[idno] = []
    for m in movies:
        found = True
        for w in movie_words[m]:
            word_count += 1
            if not (w in text.lower()):
                found = False
                break
        if found:
            movie_count += 1
            tweet_movies[idno].append(m)

print("{0:d} tweets and ".format(len(tweet_movies)), end='')
print("{0:d} non-tweets in ".format(not_tweet_count), end='')
print("{0:d} objects.".format(object_count))
print("{0:d} words and ".format(word_count), end='')
print("{0:d} movies matched.".format(movie_count))
idnos = sorted(tweet_movies.keys())
for idno in idnos:
    print("{0:d} ".format(idno), end='')
    for m in tweet_movies[idno]:
        print('"{0}"'.format(m), end=' ')
    print()
    print(json.dumps(tweet_data[idno], indent=4))
print(json.dumps(word_movies, indent=4))
print(json.dumps(movie_words, indent=4))
