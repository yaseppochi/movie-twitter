#!/usr/bin/env python3.4

"""
Examine a tweet stream and sort by movie.
The tweet stream should be a sequence of tweets in JSON format.
Output is a sorted list of tweet IDs with corresponding movies.
"""

# You can ignore comments beginning with "TODO:".
# TODO: This program is based on tweetcheck.py.  Abstract the main loop.

from collections import Counter, OrderedDict, defaultdict
from tweetdata import *
from tempfile import NamedTemporaryFile
import argparse
import json
import movie
import moviedata
import os
import os.path
import pytz
import re
import sys

# Set up the input file.  STREAM refers to future use with Twitter API.
parser = argparse.ArgumentParser(description="Examine a file of tweets (JSON)")
parser.add_argument('FILES', type=str, help="Files of JSON tweets", nargs='*')
parser.add_argument('--dataroot', type=str, help="Data root directory")
args = parser.parse_args()

def traverse_tweet_data(root):
    return (os.path.join(root, "results", stamp, json)
            for stamp in os.listdir(os.path.join(root, "results"))
            for json in os.listdir(os.path.join(root, "results", stamp))
            if json.endswith(".json"))

if not args.FILES:
    files = traverse_tweet_data(root)
else:
    files = args.FILES

# Set up the JSON decoder.
decoder = json.JSONDecoder()

# Set parameters for data extraction.
valence_terms = ['loved', 'fun', 'like', 'hated', "love", "liked", "good",
                 "best", 'entertainment', "must watch", "amazing", "thank",
                 "must see"]
filter_in_terms = ['movie', 'release', 'go to', 'see']
filter_out_terms = ['home run', 'get home', 'got home', 'home movie',
                    'homemovie', 'realtor', 'realty', 'real estate']
reportable_terms = valence_terms + filter_in_terms + filter_out_terms


# Accumulate some statistics about the file.
object_count = 0                        # If there is an error in the JSON
                                        # parse, this may not be all of the
                                        # objects in the file.  Check for
                                        # start close to or greater than end
                                        # in output.
not_tweet_count = 0                     # Valid JSON object but not a tweet.
                                        # Eg, an "end connection" message.
duplicate_count = 0                     # Duplicated tweets.
word_count = Counter()                  # Word distribution.
movie_count = Counter()                 # Movie distribution.
terms_count = Counter()
should_match = []
should_not_match = []

# Compute the distribution of words.
# This is not the number of times a word occurs in the corpus, but rather
# the number of tweets using the word.
# The tweet text is cleaned.  First, URLs (which are usually meaningless in
# the text) are replaced with the symbol <URL>.  Second, user mentions are
# replaced with the symbol @USER.  Third, hashtags are counted twice: once
# as the hashtag, and once as the word without the hash.

word_distribution = Counter()

# #### Combine these.
tweet_movies = {}
movie_tweets = defaultdict(list)
tweet_data = {}

print("TEXT OF TWEETS WITH lang ATTRIBUTE != en.\n")

def analyze_file(fileobject):
    global word_count, movie_count, tweet_movies, movie_tweets, \
        word_distribution, tweet_data, sampling_count, terms_count
    s = f.read()
    start = 0
    end = len(s)
    while True:
        try:
            status, offset = decoder.raw_decode(s[start:start+50000])
            start = start + offset + 1
            sampling_count["JSON objects"] += 1
        except Exception as e:
            # Currently there's no real point in printing the exception,
            # since we bail out in any case.
            # print("Error:", e)
            # print("|", s[start:start+100])
            print("\nnext = ", start, "end =", end, "\n")
            # TODO: If the decoder raises, start doesn't get incremented.  So
            # there's nothing to do but bail out.
            break
        try:
            tweet = TweetData(status)
            idno = status['id']
            if idno in tweet_data:
                print("{0:d} duplicate encountered, replacing.".format(idno))
                sampling_count["duplicate tweet"] += 1
            tweet_data[idno] = tweet
            # #### This really should be a regexp match.
            for term in reportable_terms:
                if term in tweet.text:
                    terms_count[term] += 1
        except (KeyError, SamplingException) as e:
            # We're missing essential data.
            sampling_count['not a tweet'] += 1
            # Maybe it's a limit notice?
            limit = status.get('limit')
            if limit:
                sampling_count['limit'] += 1
                for k, v in limit.items():
                    try:
                        sampling_count["limit." + k] += v
                    except Exception:
                        pass
            else:
                print(e)
            continue

        for w in tweet.words:
            word_distribution[w] += 1

        tweet_movies[idno] = []
        for m in movie.Movie.by_name.values():
            found = True
            for w in m.words:
                if w not in tweet.all_words:
                    found = False
                else:
                    word_count[w] += 1
            if found:
                movie_count[m] += 1
                movie_tweets[m].append(tweet)
                tweet_movies[idno].append(m)

COUNT_REPORTS = [
    ("tweet-word-distribution", word_distribution),
    # Need to refactor (or maybe define special json encoders).
    # ("movie-word-distribution", word_count),
    # ("movie-distribution", movie_count),
    ("filter-terms-distribution", terms_count),
    ("location-available-counts", location_count),
    ("sampling-counts", sampling_count),
    ]

if os.path.isdir("./reports"):
    pass
elif os.path.exists("./reports"):
    raise RuntimeError("reports exists and is non-directory")
else:
    os.mkdir("./reports")

for fn in args.FILES:
    with open (fn) as f:
        analyze_file(f)
        for name, dist in COUNT_REPORTS:
            with NamedTemporaryFile(mode="w", dir=".", delete=False) as tf:
                # For Windows portability.
                tname = tf.name
                print("{0:s} has {1:d} entries.".format(name, len(dist)),
                      file=tf)
                json.dump(OrderedDict(dist.most_common()), tf, indent=4)
            os.rename(tname, "./reports/" + name + ".out")

print("TWEET DATA SORTED BY id\n")
mcnt = ncnt = 0
idnos = sorted(tweet_movies.keys())
for idno in idnos:
    print("{0:d} ".format(idno), end='')
    if tweet_movies[idno]:
        mcnt = mcnt + 1
        for m in tweet_movies[idno]:
            print('"{0}"'.format(m), end=' ')
    else:
        ncnt = ncnt + 1
    print(json.dumps(tweet_data[idno].tweet, indent=4))

print("\nTWEET CONTENT BY MOVIE, SORTED FOR SOME SIMILARITY\n")

for m in movie_tweets.keys():
    tweets = [t for t in movie_tweets[m] if t.words]
    tweets.sort(key=lambda t: t.words)
    print("{0:s} ({1:d}, {2:d}):".format(m.name,
                                         len(movie_tweets[m]),
                                         len(tweets)))
    fmt = "{0:-18d} week={1:d} {2:s}"
    for t in tweets:
        print(fmt.format(t.tweet['id'],
                         m.timestamp_to_week(t.timestamp),
                         t.text))

# Need to define a special encoder.
# print(json.dumps(movie.Movie.word_movies, indent=4))
print("{")
for w, l in movie.Movie.word_movies.items():
    print("    ", w, " : [", sep="")
    for m in l:
        print("        ", m.name, ",", sep="")
    print("        ]")
print("}")
print("{")
for n, m in movie.Movie.by_name.items():
    print("   ", n, ": [ ", end="")
    print(*m.words, sep=", ", end=" ]\n")
print("}")
files = [f for f in traverse_tweet_data(".")]
#print(files)
print("There are", len(files), "output files in the database.")
print("{0:d} unique tweets, ".format(len(tweet_movies)))
print("len(idnos) = {0:d}.".format(len(idnos)))
print("{0:d} tweets with identified movie(s).".format(mcnt))
print("{0:d} tweets with no movie identified.".format(ncnt))  
print("should have matched while splitting: {}".format(should_match))
print("should not have matched while splitting: {}".format(should_not_match))
