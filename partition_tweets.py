# segregate_tweets -- organize tweets into movie-week files

import argparse
from collections import Counter
import json
import logging
from movie import Movie
import os.path
import re
from sys import stderr

# tuning parameters
MAX_FILES = 200
WRITES_PER_FLUSH = 1000
READS_PER_WRITE = 1000
TWEET_WRAP_COLUMNS = 72

# #### Use .ini file to configure this parameters and defaults for
# command arguments.

def parse_command_line():
    """
    Return list of files based on command line arguments.
    """

    parser = argparse.ArgumentParser(
        description="Segregate tweets (JSON) according to movie and month.")
    parser.add_argument('--prefix', type=str,
                        help="""Directory prefix for resolving relative names of sources.
Not useful with shell wildcards.""")
    parser.add_argument('--output', type=str, default="/var/local/twitterdb",
                        help="""Directory prefix for storing partition.""")
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
    args.sources = sources
    return args


def json_source(file_list):

    log = logging.getLogger("tweets")

    decoder = json.JSONDecoder()
    for f in file_list:
        log.start(f)
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
                # do this with log.error
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
                    # do this with log.error
                    print("file =", f, file=stderr)
                    print("start = ", start, "end =", end, file=stderr)
                    print("Error:", e, file=stderr)
                    # The file is hosed.  Bail out.
                    # We recognize this by the repeated error at same start.
                    break
        log.done(f)

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
    ('created_at',),
    ('favorite_count',),
    ('id',),
    ('retweet_count',),
    ('text',),
    ('timestamp_ms',),
    ('url',),
    ('user',
     ('favourites_count',),
     ('followers_count',),
     ('friends_count',),
     ('id',),
     ('location',),
     ('screen_name',),
     ('statuses_count',),
     ('time_zone',),
     ),
    ('retweeted_status',
     ('favorite_count',),
     ('id',),
     ('retweet_count',),
     ('text',),
     ('user',
      ('favourites_count',),
      ('followers_count',),
      ('friends_count',),
      ('id',),
      ('location',),
      ('screen_name',),
      ('statuses_count',),
      ('time_zone',),
      ),
     ('entities',
      ('hashtags',
       ('text',),
       ),
      ('urls',
       ('expanded_url',),
       ),
      ),
     ),
    ('entities',
     ('hashtags',
      ('text',),
      ),
     ('urls',
      ('expanded_url',),
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
            if isinstance(value, list):
                new[head] = [prune_dict(x, tail) for x in value]
            elif tail:
                new[head] = prune_dict(value, tail)
            else:
                new[head] = value
        except KeyError:
            # #### We should log these.
            pass
    return new


def json_sink(filename):                # Should use asyncio.
    count = 0
    with open(filename, 'a') as s:      # TODO: Handle Twitter API too.
        while True:
            obj = (yield)
            if isinstance(obj, dict):
                json.dump(obj, s)
                count = (count + 1) % WRITES_PER_FLUSH
                if count == 0:
                    s.flush()
            else:
                # #### This should only be used for whitespace.
                s.write(obj)


def get_writer(filename):

    f = file_map.get(filename)
    while f is None:
        try:
            f = json_sink(filename)
            next(f)
            file_map[filename] = f
            if filename in file_lru:    # #### Maybe using .index is faster?
                file_lru.remove(filename)
            file_lru.append(filename)
        except IOError:
            if not file_lru:
                raise
            lru = file_lru.pop(0)
            file_map[lru].close()
            del file_map[lru]
    else:
        if len(file_lru) > MAX_FILES:
            lru = file_lru.pop(0)
            file_map[lru].close()
            del file_map[lru]
    return f


def wrap_tweet_text(tweet):
    text = (tweet.get('retweeted_status') or tweet)['text']
    words = text.split()
    i = 0
    lines = []
    end = len(words)
    line = ["*"]                        # First line of tweet marker.
    length = 1
    while i < end:
        word = words[i]
        length += (1 + len(word))
        if length > TWEET_WRAP_COLUMNS:
            # Alternative algorithm:
            # don't join, insert newline in words here ...
            lines.append(" ".join(line))
            line = [word]
            length = len(word)
        else:
            # ... and space here ...
            line.append(word)
        i += 1
    #lines.append("")
    # ... and join words on "" here.
    return "\n".join(lines)


file_map = {}                           # movie-week name -> open file object
file_lru = []                           # File objects in LRU order.
stats = {}

def partition_tweets(dataset, output):

    count = 0                           # Count of tweets (fully) processed.

    # Various statistics for checking on Twitter API.
    stats['no lang'] = 0
    stats['lang english'] = 0
    stats['lang region'] = 0
    stats['out of period'] = 0
    stats['no movie for tweet'] = 0

    # main loop
    pending_writes = []
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
        text = tweet['text'].lower()
        movies = []
        for m in Movie.by_name.values():
            for w in m.words:
                if w not in text:
                    break
            else:
                movies.append(m)
        if not movies:
            stats['no movie for tweet'] += 1
            continue

        # Determine applicable dates and filenames.
        stamp = int(tweet['timestamp_ms'])
        for m in movies:
            # #### It might be worthwhile to "unroll" a binary search?
            for i in range(len(m.week_bounds) - 1):
                # #### This range check can probably be improved.
                if m.week_bounds[i] <= stamp < m.week_bounds[i + 1]:
                    # This data is kept in memory, and written in batches.
                    filestem = "%s/%s" % (output, m.file_stem)
                    pending_writes.append(("%s-%d.json" % (filestem, i),
                                           prune_dict(tweet, desired_fields)))
                    pending_writes.append(("%s.text" % (filestem,),
                                           "* " + tweet['text'].strip().translate({10 : 32})))
                                           
            else:
                stats['out of period'] += 1

        # Done processing this tweet.
        count += 1

        if count % READS_PER_WRITE == 0:
            # Sort pending_writes on filename to minimize file object churn.
            pending_writes.sort(key=lambda x: x[0])
            # Write them.
            for filename, obj in pending_writes:
                f = get_writer(filename)
                f.send(obj)
                f.send('\n')
            # Maybe flush them?
            # Clear pending_writes.
            pending_writes.clear()

    pending_writes.sort(key=lambda x: x[0])
    file_map = {}                       # #### UGH!!
    file_lru = []
    for filename, obj in pending_writes:
        f = get_writer(filename)
        f.send(obj)
        f.send('\n')


# #### UNIMPLEMENTED
def select_movie_tweets():
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
                movie_data['includes'][phrase] = \
                    movie_data['includes'].get(phrase, 0) + 1
        if not movie_found:
            continue                # Don't record this tweet.
    
        #     Test exclusion criteria.
        for phrase in movie_excludes:
            if phrase in text:
                movie_ignore = True
                movie_data['excludes'][phrase] = \
                    movie_data['excludes'].get(phrase, 0) + 1
        if movie_ignore:
            continue                # Don't record this tweet.
    
        movie_data['tweets'].append(tweet)

if __name__ == "__main__":
    args = parse_command_line()
    json_files = args.sources
    dataset = json_source(json_files)
    
    logging.addLevelName(logging.INFO, "START")
    logging.addLevelName(logging.WARNING, "DONE")
    format = logging.Formatter("%(levelname)-6s %(message)s %(asctime)s")
    handler = logging.FileHandler("%s/source.log" % args.output)
    handler.setFormatter(format)
    source_log = logging.getLogger("tweets")
    source_log.setLevel(logging.INFO)
    source_log.addHandler(handler)
    source_log.start = source_log.info
    source_log.done = source_log.warning

    if json_files:
        print("There are %d JSON files, estimated %.1fGB." % (
                len(json_files), len(json_files) / 20),
              file=stderr)
        partition_tweets(dataset, args.output)
    else:
        print("You didn't provide any arguments.  Is that right?")

