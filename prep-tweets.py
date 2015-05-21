#!/usr/bin/env python3.4

"""
Examine a tweet stream and sort by movie.
The tweet stream should be a sequence of tweets in JSON format.
Output is a sorted list of tweet IDs with corresponding movies.
"""

# You can ignore comments beginning with "TODO:".
# TODO: This program is based on tweetcheck.py.  Abstract the main loop.

from moviedata import MOVIES, PUNCT, STOPLIST
from collections import Counter, OrderedDict
import argparse
import json
import re

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
valence_terms = ['loved', 'fun', 'like', 'hated', "loved", "liked", "good",
                 "best", 'entertainment', "must watch", "amazing", "thank"]
filter_in_terms = ['movie', 'release', 'go to', 'see']
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
required_missing_count = 0              # Count of missing entity lists.
location_count = Counter()
terms_count = Counter()
badlang_count = 0
should_match = []
should_not_match = []

# Compute the distribution of words.
# This is not the number of times a word occurs in the corpus, but rather
# the number of tweets using the word.
# The tweet text is cleaned.  First, URLs (which are usually meaningless in
# the text) are replaced with the symbol <URL>.  Second, user mentions are
# replaced with the symbol @USER.  Third, hashtags are counted twice: once
# as the hashtag, and once as the word without the hash.

# #### word_distribution = Counter()

# #### DON'T FORGET THE RECURSIVE DESCENT INTO ALL DATA.
# #### ALSO NEED TO COMPUTE WEEK BOUNDARIES.

# #### Combine these.
tweet_movies = {}
movie_tweets = {}
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

    stoplist = STOPLIST
    required_keys = ['id','text', 'created_at']
    general_keys = ['timestamp_ms', 'lang', 'favorite_count']
    general_keys.extend(required_keys)
    entity_keys = ['urls', 'hashtags']
    retweet_keys = ['retweet_count']
    location_keys = ['coordinates', 'geo', 'place',
                       'user.location', 'user.time_zone']
    def __init__(self, status):
        self.tweet = {}
        self._collect_attrs(self.tweet, status)
        self._filter()
        self._collect_entity_text(status)
        self._collect_miscellaneous(status)
        self._canonicalize_text()
        self.all_words = sorted(set(self.words + self.entity_words))

    def _filter(self):
        """
        Raise a SamplingException on a tweet that is out of sample.
        """
        # #### These exceptions are probably mutually exclusive, but if not
        # we interpret BadLang as applying only to valid tweets.
        global required_missing_count, badlang_count
        missing = []
        for k in self.required_keys:
            if self.tweet[k] is None:
                missing.append(k)
        if missing:
            required_missing_count += 1
            raise MissingRequiredKeyException(missing)
        # #### Are there 3-letter RFC 3166 codes starting with "en"?
        if 'lang' in self.tweet and not self.tweet['lang'].startswith('en'):
            print(self.tweet['text'][:78])
            badlang_count += 1
            raise BadLangException(self.tweet['lang'])

    # The tweet argument allows recursion for retweets.
    def _collect_attrs(self, tweet, status):
        for k in TweetData.general_keys:
            tweet[k] = status[k] if k in status else None
        if 'entities' in status:
            entities = status['entities']
            for k in TweetData.entity_keys:
                tweet[k] = entities[k] if k in entities else None
        if 'retweeted_status' in status:
            original = status['retweeted_status']
            working = {}
            self._collect_attrs(working, original)
            for k in TweetData.retweet_keys:
                working[k] = original[k] if k in original else None
            self.tweet['original'] = working
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

        words = re.split(r"(\s|[-/._?#;:,']|http://)+",
                         self.tweet['text'].lower())
        for word in words[::2]:
            if re.match(r"(\s|[-/._?#;:,']|http://)+", word):
                should_not_match.append(word)
        for word in words[1::2]:
            if not re.match(r"(\s|[-/._?#;:,']|http://)+", word):
                should_match.append(word)
        words = { word.strip() for word in words[::2] }
        self.words = [ word for word in words
                       if word and word not in TweetData.stoplist ]
        self.words.sort()
        
    def _collect_entity_text(self, status):
        if 'entities' in status:
            entities = status['entities']
            self.hash_text = " ".join(h['text']
                                      for h in entities['hashtags']) \
                             if 'hashtags' in entities else ""
            self.media_text = " ".join(m['expanded_url'] + " "
                                       + m['display_url']
                                       for m in entities['media']) \
                              if 'media' in entities else ""
            self.url_text = " ".join(u['expanded_url'] + " " + u['display_url']
                                     for u in entities['urls']) \
                            if 'urls' in entities else ""
            self.user_text = " ".join(u['screen_name']
                                  for u in entities['user_mentions']) \
                             if 'user_mentions' in entities else ""
            # Split the concatenation of texts on URL segment boundaries
            # as well as English word boundaries.
            words = " ".join([self.hash_text, self.media_text, self.url_text,
                              self.user_text]).lower()
            # Split, eliminate duplicates, and sort.
            words = [ word.strip() for word
                      in re.split(r"(\s|[-:/._?#;,']|http://)+", words)][::2]
            self.entity_words = sorted(set(word for word in words if word))
        else:
            self.entity_words = []

    def _collect_miscellaneous(self, status):
        # #### Refactor this for efficiency!!
        # #### Use this technique for retweet fields.
        def extract_location(key, tweet):
            keys = key.split(".")
            result = tweet
            for k in keys:
                result = result.get(k)
                if not result: break
            return result
        for k in TweetData.location_keys:
            result = extract_location(k, status)
            if result:
                location_count[k] += 1
                self.tweet[k] = result
        # #### Probably should collect retweet operations.
        if 'retweeted_status' in status:
            retweet = status['retweeted_status']
            for k in TweetData.location_keys:
                result = extract_location(k, retweet)
                if result:
                    location_count["retweet." + k] += 1
                    # #### Flatten retweet attributes.
                    self.tweet['original'][k] = result


print("TEXT OF TWEETS WITH lang ATTRIBUTE != en.\n")

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
        print("\nnext = ", start, "end =", end, "\n")
        # TODO: If the decoder raises, start doesn't get incremented.  So
        # there's nothing to do but bail out.
        break
    try:
        tweet = TweetData(status)
        idno = tweet.tweet['id']
        if idno in tweet_data:
            print("{0:d} duplicate encountered, replacing.".format(idno))
            duplicate_count += 1
        tweet_data[idno] = tweet
        # #### This really should be a regexp match.
        for term in reportable_terms:
            if term in tweet.tweet['text']:
                terms_count[term] += 1
    except (KeyError, SamplingException):
        # We're missing essential data.  Try next tweet.
        not_tweet_count += 1
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
            # #### movie_tweets should be a DefaultDict
            if m in movie_tweets:
                movie_tweets[m].append(tweet)
            else:
                movie_tweets[m] = [tweet]
            tweet_movies[idno].append(m)


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

def clean_text(s):
    s = re.sub(r"(?i)(\bhttp://[-a-z0-9/?#,.]+[?/#]?\b|(^|\s)@\w+\b|\bRT\b):?",
               " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()

for m in movie_tweets.keys():
    tweets = [t for t in movie_tweets[m] if t.words]
    tweets.sort(key=lambda t: t.words)
    print("{0:s} ({1:d}, {2:d}):".format(m, len(movie_tweets[m]), len(tweets)))
    for t in tweets:
        print("{0:-18d} {1:s}".format(t.tweet['id'],
                                      clean_text(t.tweet['text'])))

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
print("{0:d} missing attributes found.".format(required_missing_count))
print("{0:d} tweets with non-English lang found".format(badlang_count))
print("len(idnos) = {0:d}.".format(len(idnos)))
print("{0:d} tweets with identified movie(s).".format(mcnt))
print("{0:d} tweets with no movie identified.".format(ncnt))  
print("should have matched while splitting: {}".format(should_match))
print("should not have matched while splitting: {}".format(should_not_match))
