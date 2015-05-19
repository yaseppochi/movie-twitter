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
valence_terms = ['loved', 'movie', 'home run', 'get home', 'got home',
                 'home movie', 'homemovie', 'liked', 'hated', "didn't like"]
filter_in_terms = ['movie']
filter_out_terms = ['home run', 'get home', 'got home', 'home movie',
                    'homemovie', 'realtor', 'realty', 'real estate']
reportable_terms = valence_terms + filter_in_terms + filter_out_terms

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
duplicate_count = 0                     # Duplicated tweets.
word_count = Counter()                  # Word distribution.
movie_count = Counter()                 # Movie distribution.
key_errors = 0                          # Count of missing entity lists.
location_count = Counter()
terms_count = Counter()

# #### Combine these.
tweet_movies = {}
tweet_data = {}

class SamplingException(Exception):
    pass

class MissingRequiredKeyException(SamplingException):
    pass

class BadLangException(SamplingException):
    pass

class TweetData(object):
    """
    Collect relevant data from a status and prepare it for analysis.
    """

    stoplist = ["a", "an", "the", "some", "to", "from", "for", "with"]
    required_keys = ['id','text', 'created_at']
    general_keys = ['timestamp_ms', 'lang', 'favorite_count'] \
                   .extend(required_keys)
    entity_keys = ['urls', 'hashtags']
    retweet_keys = ['retweet_count']
    reportable_keys = ['coordinates', 'geo', 'place',
                       'user.location', 'user.time_zone']
    def __init__(self, status):
        self.tweet = {}
        self._collect_attrs(self, self.tweet, status)
        self._filter()
        self._collect_entity_text(status)
        self._canonicalize_text()
        self.all_words = sorted(set(self.words + self.entity_words))

    def _filter(self):
        """
        Raise a SamplingException on a tweet that is out of sample.
        """
        # #### Are there 3-letter RFC 3166 codes starting with "en"?
        if 'lang' in self.tweet and not self.tweet['lang'].startswith('en'):
            print(self.tweet['text'])
            raise BadLangException(self.tweet['lang'])
        for k in self.required_keys:
            missing = []
            if self.tweet[k] is None:
                missing.append(k)
            if missing:
                raise MissingRequiredKeyException(missing)

    # The tweet argument allows recursion for retweets.
    def _collect_attrs(self, tweet, status):
        for k in self.general_keys:
            tweet[k] = status[k] if k in status else None
        entities = status['entities']
        for k in self.entity_keys:
            tweet[k] = entities[k] if k in entities else None
        if 'retweeted_status' in tweet:
            original = tweet['retweeted_status']
            retweeted = {}
            for k in self.retweet_keys:
                retweeted[k] = original[k] if k in original else None
            self._collect_attrs(retweeted, original)
            self.tweet['original'] = retweeted
        else:
            tweet['original'] = None

    def _canonicalize_text(self):
        """
        Canonicalize the text of the tweet.  Result in words attribute.
        Algorithm:
        1. Strip the text (probably unnecessary).
        2. Lowercase the text.
        3. Split into list of words (on whitespace).
        4. Remove stopwords and duplicates.
        5. Sort the list.
        """

        words = self.tweet['text'].strip().lower().split()
        words = { word for word in words if not word in TwitterData.stoplist }
        self.words = sorted(words)
        
    def _collect_entity_text(self, status):
        self.hash_text = " ".join(h['text']
                                  for h in entities['hashtags']) \
                         if 'hashtags' in entities else ""
        self.media_text = " ".join(m['expanded_url'] + " " + m['display_url']
                                   for m in entities['media']) \
                          if 'media' in entities else ""
        self.url_text = " ".join(u['expanded_url'] + " " + u['display_url']
                                 for u in entities['urls']) \
                        if 'urls' in entities else ""
        self.user_text = " ".join(u['screen_name']
                                  for u in entities['user_mentions']) \
                         if 'user_mentions' in entities else ""
        words = " ".join([self.hash_text, self.media_text, self.url_text,
                          self.user_text]).lower() \
                # Split the concatenation of texts on URL segment
                # boundaries as well as English word boundaries.
                .split(r"(\s|[/._-?#;,])+")
        # Eliminate duplicates and sort.
        self.entity_words = sorted(set(words))


while True:
    try:
        status, offset = decoder.raw_decode(s[start:start+50000])
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
        tweet = TweetData(status)
        if tweet['id'] in tweet_data:
            print("{0:d} duplicate encountered, replacing.".format(idno))
            duplicate_count = duplicate_count + 1
        tweet_data[tweet['id']] = tweet
        for term in reportable_terms:
            if term in tweet.all_words:
                terms_count[term] += 1
    # #### Maybe we should report details?
    except (KeyError, SamplingException):
        # We're missing essential data.  Try next tweet.
        not_tweet_count = not_tweet_count + 1
        continue

    tweet_movies[idno] = []
    for m in movies:
        found = True
        for w in movie_words[m]:
            if w not in tweet.all_words:
                found = False
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
    else:
        ncnt = ncnt + 1
    print(json.dumps(tweet_data[idno].tweet, indent=4))
print(json.dumps(word_movies, indent=4))
print(json.dumps(movie_words, indent=4))
print(json.dumps(OrderedDict(word_count.most_common()), indent=4))
print(json.dumps(OrderedDict(movie_count.most_common()), indent=4))
print(json.dumps(OrderedDict(location_count.most_common()), indent=4))
print(json.dumps(terms_count, indent=4))
print("{0:d} unique tweets, ".format(len(tweet_movies)), end='')
print("{0:d} duplicates, and ".format(duplicate_count), end='')
print("{0:d} non-tweets in ".format(not_tweet_count), end='')
print("{0:d} objects.".format(object_count))
print("{0:d} missing entities were observed.".format(key_errors))
print("len(idnos) = {0:d}.".format(len(idnos)))
print("{0:d} tweets with identified movie(s).".format(mcnt))
print("{0:d} tweets with no movie identified.".format(ncnt))  
