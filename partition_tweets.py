# segregate_tweets -- organize tweets into movie-week files

import argparse
from collections import Counter
import json
import os.path
import re
from sys import stderr

def parse_command_line():
    """
    Return list of files based on command line arguments.
    """

    parser = argparse.ArgumentParser(
        description="Segregate tweets (JSON) according to movie and month.")
    parser.add_argument('--prefix', type=str,
                        help="""Directory prefix for resolving relative names.
Not useful with shell wildcards.""")
    parser.add_argument('sources', type=str, nargs="*",
                        help="""Sources of JSON tweets.
Can be a folder containing folders with names like "YYYYMMDD.HHMMSS",
or a space-separated list of folders with names like "YYYYMMDD.HHMMSS",
or a space-separated list of files with names like "stream-results-NNN.json".
NOTE: Mixing the three forms is OK, but repeats are not deleted.
NOTE: folder names should not have trailing slashes.
""")
    args = parser.parse_args()
    # Unlovely(?) onepass algorithm.
    if args.prefix:
        srclist = [os.path.join(args.prefix, x) for x in args.sources]
    else:
        srclist = args.sources
    stampre = r"(?:.*/)?2015[01][0-9][0-3][0-9]\.[0-2][0-9][0-6][0-9][0-6][0-9]$"
    stampre = re.compile(stampre)
    sources = []

    def add_one(item, sources):
        if item.endswith(".json"):
            if os.path.isfile(item):
                sources.append(item)
            else:
                print("Ignoring nonfile: %s" % (item,), file=stderr)
            return True
        return False

    def add_many(item, sources):
        if stampre.match(item):
            if os.path.isdir(item):
                for f in os.listdir(item):
                    add_one(os.path.join(item, f), sources)
            else:
                print("Ignoring nondirectory: %s" % (item,), file=stderr)
            return True
        else:
            return False

    def add_folder(item, sources):
        if os.path.isdir(item):
            for f in os.listdir(item):
                add_many(os.path.join(item, f), sources)
        else:
            print("Ignoring nondirectory: %s" % (item,), file=stderr)

    for x in srclist:
        add_one(x, sources) or add_many(x, sources) or add_folder(x, sources)
    return sources


def json_stream(file_list):
    decoder = json.JSONDecoder()
    for f in file_list:
        with open(f) as stream:         # TODO: Handle Twitter API too.
            s = stream.read()           # TODO: Online algorithm.
                                        # Do it with a JSON(Stream)?Decoder?
        start = 0
        end = len(s)

        while True:
            while start < end and s[start].isspace():
                start += 1
            if start >= end:
                break
            try:
                tweet, offset = decoder.raw_decode(s[start:start+20000])
                start = start + offset
                yield tweet
            except Exception as e:
                print("file =", f, file=stderr)
                print("start = ", start, "end =", start+20000, file=stderr)
                print("Error:", e, file=stderr)
                # Fall back to the rest of the file and hope this doesn't
                # happen often.
                try:
                    tweet, offset = decoder.raw_decode(s[start:])
                    start = start + offset
                    yield tweet
                except Exception as e:
                    print("file =", f, file=stderr)
                    print("start = ", start, "end =", end, file=stderr)
                    print("Error:", e, file=stderr)
                    # The file is hosed.  Bail out.
                    # We recognize this by the repeated error at same start.
                    break


def count_keys(tweets):
    tdist = Counter()
    rtdist = Counter()
    udist = Counter()
    for tweet in tweets:
        for key in tweet:
            tdist[key] += 1
        if 'retweeted_status' in tweet:
            for key in tweet['retweeted_status']:
                rtdist[key] += 1
        if 'user' in tweet:
            for key in tweet['user']:
                udist[key] += 1
    return tdist, udist, rtdist


desired_fields = (
    ('text'),
    ('id'),
    ('created_at'),
    ('timestamp_ms'),
    ('favorite_count'),
    ('url'),
    ('hashtags'),
    ('time_zone'),
    ('retweet_count'),
    ('user',
     ('id'),
     ('screen_name'),
     ('followers_count'),
     ('friends_count'),
     ('status_count'),
     ),
    ('retweeted_status',
     ('id'),
     ('favorite_count'),
     ('retweet_count'),
    #('entities',
    #    ('hashtags'),
    #    ('urls'),
    # ),
     ('time_zone'),
     ('user',
      ('id'),
      ('screen_name'),
      ('followers_count'),
      ('friends_count'),
      ('status_count'),
      ),
     ),
    )

# #### Doesn't handle lists.
def prune_dict (old, desired_fields):
    new = {}
    for item in desired_fields:
        # #### Optimize me!
        head = item[0]
        tail = item[1:]
        try:
            value = old[head]
            if tail:
                new[head] = prune_dict(value, tail)
            else:
                new[head] = value
        except KeyError:
            # #### We should log these.
            pass
    return new


# Assume that tweets are already in an iterable called "dataset".

def partition_tweets(dataset):

    file_map = {}                       # movie-week name -> open file object
    file_lru = []                       # File objects in LRU order.
    count = 0                           # Count of tweets (fully) processed.

    # Various statistics for checking on Twitter API.
    stats = {}
    stats['no lang'] = 0
    stats['lang english'] = 0
    stats['lang region'] = 0
    stats['out of period'] = 0
    stats['no movie for tweet'] = 0

    # main loop
    for tweet in dataset:

        # Short-circuit (ignore) non-English tweets.
        lang = tweet.get('lang')
        if not lang:
            stats['no lang'] += 1
            continue
        elif lang.lower() == "english":
            stats['lang english'] += 1
        elif lang.startswith("en_"):
            stats['lang region'] +=1
        elif lang != "en":
            continue
        else:
            pass

        # Determine applicable movies.
        # #### There may be a better algorithm based on Movie.word_movies.
        movies = []
        for m in Movie.by_name:
            for w in m.words:
                if w not in tweet['text']:
                    break
            else:
                movies.append(m)
        if not movies:
            stats['no movie for tweet'] += 1
            continue

        # Determine applicable dates and filenames.
        # #### It might be worthwhile to "unroll" a binary search?
        stamp = tweet['timestamp_ms']
        for m in movies:
            for i in range(len(m.week_bounds) - 1):
                # #### This range check can probably be improved.
                if m.week_bounds[i] <= stamp < m.week_bounds[i + 1]:
                    # This data is kept in memory, and written in batches.
                    filename = "/var/local/twitterdb/%s-%d.json" \
                               % (m.file_stem, i)
                    pending_writes.append((filename,
                                           prune_tweet(tweet, desired_fields)))
            else:
                stats['out of period'] += 1

        # Done processing this tweet.
        count += 1

        if count % 1000 == 0:
            # Sort pending_writes on filename to minimize file object churn.
            pending_writes.sort(key=lambda x: x[0])
            # Write them. #### IMPLEMENT ME!
            # Maybe flush them?
            # Clear pending_writes.
            pending_writes.clear()


def break_this_motherfucker_up():

# A bunch of code for handling the open files.

    f = file_map.get(filename)
    # .append() because .remove() deletes the first instance.
    file_lru.append(filename)
    # #### This should be a "while".
    if f is None:
        # #### Need to check for failure, as we should end
        # up with around 285*9 = 2565 files.
        # Mac OS X allows up to 256 open files.  Linux allows
        # 1024.
        f = open(filename, "a")
        file_map[filename] = f
        if length(file_lru) > 200:
            lru = file_lru.pop(0)
            file_map[lru].close()
            del file_map[lru]
    else:
        file_lru.remove(filename)
    
    for tweet in dataset:
        #     Create a dict to hold movie statistics and data.
        movie_data = {'title': "The DUFF",
                      'includes': {},
                      'excludes': {},
                      'tweets': [],
                     }
    
        #     Set up the selection criteria
        movie_includes = ["duff"]
        #     But what if one of the stars is named "hilary" or "anthony"?
        movie_excludes = ["hilary", "anthony", "jada"]
        movie_found = False                # We may have multiple includes,
                                           # but we want to check all.
        movie_ignore = False               # We may have multiple excludes,
                                           # but we want to check all.
    
        #     Eventually we must handle retweets and interesting metadata
        # (eg # of followers).  Not yet.
        #     Get the text and make it all lowercase.
        text = tweet['text'].lower()
    
        #     Test inclusion criteria.
        for phrase in movie_includes:
            if phrase in text:
                movie_found = True
                #     The r.h.s. uses .get() to avoid KeyError.  It is also
                # much more efficient than if ... then ... else.
                movie_data['includes'][phrase] = movie_data['includes'].get(phrase, 0) + 1
        if not movie_found:
            continue                # Don't record this tweet.
    
        #     Test exclusion criteria.
        for phrase in movie_excludes:
            if phrase in text:
                movie_ignore = True
                movie_data['excludes'][phrase] = movie_data['excludes'].get(phrase, 0) + 1
        if movie_ignore:
            continue                # Don't record this tweet.
    
        movie_data['tweets'].append(tweet)

if __name__ == "__main__":
    json_files = parse_command_line()
    if json_files:
        print("There are %d JSON files, estimated %.1fGB." % (
                len(json_files), len(json_files) / 20),
              file=stderr)
    else:
        print("You didn't provide any arguments.  Is that right?")

