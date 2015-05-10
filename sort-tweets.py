#!/usr/bin/env python3.4

"""
Examine a tweet stream and sort by movie.
The tweet stream should be a sequence of tweets in JSON format.
Output is a sorted list of tweet IDs with corresponding movies.
"""

# You can ignore comments beginning with "TODO:".
# TODO: This program is based on tweetcheck.py.  Abstract the main loop.

from moviedata import MOVIES, PUNCT     # also STOPLIST when we get it
from collections import Counter, OrderedDict
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
entity_keys = ['urls', 'hashtags']
retweet_keys = ['id', 'retweet_count']
other_keys = ['favorite_count', 'lang']

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
word_count = Counter()                  # Word distribution.
movie_count = Counter()                 # Movie distribution.
key_errors = 0                          # Count of missing entity lists.

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
        # Specifically:
        # The text of the Tweet and some entity fields are considered for
        # matches. Specifically, the text attribute of the Tweet, expanded_url
        # and display_url for links and media, text for hashtags, and
        # screen_name for user mentions are checked for matches.
        tweet_data[idno] = pruned
        entities = tweet['entities']
        for k in entity_keys:
            # #### For dataset creation, probably should insert NULLs.
            if k in entities:
                pruned[k] = entities[k]
        hash_text = " ".join(h['text']
                             for h in entities['hashtags']) \
                    if 'hashtags' in entities else " "
        media_text = " ".join(m['expanded_url'] + " " + m['display_url']
                              for m in entities['media']) \
                    if 'media' in entities else " "
        url_text = " ".join(u['expanded_url'] + " " + u['display_url']
                            for u in entities['urls']) \
                    if 'urls' in entities else " "
        user_text = " ".join(u['screen_name']
                             for u in entities['user_mentions']) \
                    if 'user_mentions' in entities else " "
        if 'retweeted_status' in tweet:
            retweet = tweet['retweeted_status']
            for k in retweet_keys:
                if k in retweet:
                    # This can overwrite the id from the tweet!
                    pruned[k] = retweet[k]
        for k in other_keys:
            if k in tweet:
                pruned[k] = tweet[k]
    except KeyError:
        # We're missing essential data.  Try next tweet.
        not_tweet_count = not_tweet_count + 1
        continue

    tweet_movies[idno] = []
    for m in movies:
        found = True
        for w in movie_words[m]:
            if not (w in " ".join([text, hash_text, media_text, url_text, user_text]).lower()):
                found = False
                break
            else:
                word_count[w] += 1
        if found:
            movie_count[m] += 1
            tweet_movies[idno].append(m)

mcnt = ncnt = 0
idnos = sorted(tweet_movies.keys())
for idno in idnos:
    print("{0:d} ".format(idno), end='')
    if tweet_movies[idno]:
        mcnt = mcnt + 1
        for m in tweet_movies[idno]:
            print('"{0}"'.format(m), end=' ')
            print()
    else:
        ncnt = ncnt + 1
    print(json.dumps(tweet_data[idno], indent=4))
print(json.dumps(word_movies, indent=4))
print(json.dumps(movie_words, indent=4))
print(json.dumps(OrderedDict(word_count.most_common()), indent=4))
print(json.dumps(OrderedDict(movie_count.most_common()), indent=4))
print("{0:d} tweets and ".format(len(tweet_movies)), end='')
print("{0:d} non-tweets in ".format(not_tweet_count), end='')
print("{0:d} objects.".format(object_count))
print("{0:d} missing entities were observed.".format(key_errors))
print("len(idnos) = {0:d}.".format(len(idnos)))
print("{0:d} tweets with identified movie(s).".format(mcnt))
print("{0:d} tweets with no movie identified.".format(ncnt))  
