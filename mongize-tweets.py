#!/usr/bin/env python3.4

"""
Examine a tweet stream and store it to a MongoDB database.
The tweet stream should be a sequence of tweets in JSON format.
"""

# #### This program is based on prep-tweets.py.  The common parts should be
# abstracted into a separate module.  prep-tweets.py should be updated to
# take its input from the Mongo-ized database.
# See also TODOs in prep-tweets.py.

#from collections import Counter, OrderedDict, defaultdict
from pymongo import MongoClient
from tweetdata import *
#from tempfile import NamedTemporaryFile
import argparse
import json
import movie
import moviedata
import os
import os.path
#import pytz
#import re
import signal
import sys

track_movie_tweets = False
print_tweets_as_json = False
WORLD_READABLE = 0o644
GB = 1024 * 1024 * 1024

signal_names = [name for name in dir(signal)
                if name.startswith("SIG") and not name.startswith("SIG_")]
signal_dict = {}
for name in signal_names:
    signal_dict[getattr(signal,name)] = name

def handle_signal(signum, frame):
    """Handle signal by raising OSError."""
    raise OSError(signum, signal_dict[signum], "<OS signal>")

# I don't think we need to handle SIGHUP.
# signal.signal(signal.SIGHUP, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

class MongizeParser(argparse.ArgumentParser):
    def __init__(self):
        super().__init__(description="Examine a stream of JSON tweets")
        self.add_argument('FILES', type=str,
                          help="Files of JSON tweets", nargs='*')
        self.add_argument('--dataroot', type=str,
                          help="Data root directory (ignored in favor of FILES)")
        self.add_argument('--reserve', type=int, default=4,
                          help="Space in GB to reserve in MongoDB filesystem")

def handle_file(fileobject, collection):
    """
    Read a stream of tweets from fileobject and add them to collection.
    fileobject is an open file object.
    collection is a collection in an open MongoDB.
    #### Probably fileobject should be opened inside this function from a
    file name.
    """

    s = fileobject.read()
    start = 0
    end = len(s)
    count = 0
    while True:
        try:
            status, offset = decoder.raw_decode(s[start:start+50000])
            start = start + offset + 1
            # Give status a serial number.  This will be used to estimate
            # skip intervals.
            status['anna:serial'] = count
            if "id" in status:
                if "retweeted_status" in status:
                    original = collection.find_one({"id" : status["retweeted_status"]["id"]})
                    if original:
                        status['anna:original'] = original["_id"]
                        # #### Should update original's retweet count.
                    else:
                        status["retweeted_status"]["anna:inferred"] = True
                        result = collection.insert_one(status["retweeted_status"])
                        status['anna:original'] = result.inserted_id
                        # #### Should replace retweeted status with
                        # locally different fields.
                original = collection.find_one({"id" : status["id"]})
                if original is not None:
                    # #### By checking anna:inferred attribute can determine
                    # which is actual original.  Need to do post-processing
                    # anyway for various reasons (e.g., checking duplicates
                    # for equality and cleaning retweeted_status attributes),
                    # so this is good enough.
                    status["anna:duplicate"] = original["_id"]
            collection.insert_one(status)
            count += 1
        except Exception as e:
            if start < end:
                print("Error:", e)
            # print("|", s[start:start+100])
            print("Records read =", count, "next =", start, "end =", end, "\n")
            # TODO: If the decoder raises, start doesn't get incremented.  So
            # there's nothing to do but bail out.
            break

def traverse_tweet_data(root):
    return (os.path.join(root, "results", stamp, json)
            for stamp in os.listdir(os.path.join(root, "results"))
            for json in os.listdir(os.path.join(root, "results", stamp))
            if json.startswith("stream-") and json.endswith(".json"))

def check_available(fspath, requested_reserve):
    """
    Return space available relative to requested_reserve on fspath.
    Returns the difference in bytes.
    """

    stat = os.statvfs(fspath)
    return stat.f_bfree * stat.f_bsize - requested_reserve

def analyze_tweet(status, Session):
    global word_count, movie_count, sampling_count, terms_count, tweet_data
    # tweet_movies, movie_tweets
    try:
        tweet = TweetData(status)
        idno = status['id']
        if idno in tweet_data:
            print("{0:d} duplicate encountered, replacing.".format(idno))
            sampling_count["duplicate tweet"] += 1
        #tweet_data[idno] = tweet
        tweet_data.add(idno)
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
        #else:
        #    print(e)
    # #### This could maybe be optimized?
    # #### After refactoring, Session isn't visible in this suite.
    session = Session()
    for w in tweet.words:
        try:
            word = session.query(Word).filter_by(word=w).one()
        except NoResultFound:
            word = Word(word=w, count=0)
            session.add(word)
        word.count += 1
    session.commit()
    session.close()

    #tweet_movies[idno] = []
    for m in movie.Movie.by_name.values():
        found = True
        for w in m.words:
            if w not in tweet.all_words:
                found = False
            else:
                word_count[w] += 1
        if found:
            week = m.timestamp_to_week(tweet.timestamp)
            if week >= FIRST_WEEK and week <= LAST_WEEK:
                movie_count[m][week] += 1
                #movie_tweets[m].append(tweet)
                #tweet_movies[idno].append(m)
            else:
                untimely_count[m.name] += 1

# #### This could be used at the end of the __main__ script.
def old_main():
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
    terms_count = Counter()
    should_match = []
    should_not_match = []

    # Movie distribution.
    movie_count = defaultdict(Counter)
    untimely_count = Counter()
    FIRST_WEEK = -1
    LAST_WEEK = 10

    # Compute the distribution of words.
    # This is not the number of times a word occurs in the corpus, but rather
    # the number of tweets using the word.
    # The tweet text is cleaned.  First, URLs (which are usually meaningless in
    # the text) are replaced with the symbol <URL>.  Second, user mentions are
    # replaced with the symbol @USER.  Third, hashtags are counted twice: once
    # as the hashtag, and once as the word without the hash.

    # #### Eventually we should use a database for everything (maybe MongoDB
    # for the tweets).  But for now we'll just start with what was the
    # word_distribution.

    from sqlalchemy import create_engine, Column, Integer, String, Sequence
    from sqlalchemy.ext.declarative import declarative_base
    from sqlalchemy.orm import sessionmaker
    from sqlalchemy.orm.exc import NoResultFound

    engine = create_engine("sqlite:///word-distribution.db")
    Base = declarative_base()

    if True:                                # #### For restartability.
        Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)

    # #### Combine these.
    #tweet_movies = {}
    #movie_tweets = defaultdict(list)
    #tweet_data = {}
    tweet_data = set()

    print("TEXT OF TWEETS WITH lang ATTRIBUTE != en.\n")

    if os.path.isdir("./reports"):
        pass
    elif os.path.exists("./reports"):
        raise RuntimeError("reports exists and is non-directory")
    else:
        os.mkdir("./reports")

    for fn in files:
        sys.stdout.flush()
        print("Analyzing", fn)
        with open (fn) as f:
            analyze_file(f)
            # the main datafile
            # It should be optional to get JSON output or both JSON and CSV.
            # Code is at tag json-report.
            # #### The use of FIRST_WEEK and LAST_WEEK may be buggy!
            with NamedTemporaryFile(mode="w", dir=".", delete=False) as tf:
                # For Windows portability.
                tname = tf.name
                print("{", file=tf)
                session = Session()
                for word in session.query(Word).order_by(-Word.count):
                    print("   ", word.word, ":", word.count, file=tf)
                session.close()
                print("}", file=tf)
            newname = "./reports/tweet-word-distribution.out"
            os.rename(tname, newname)
            os.chmod(newname, WORLD_READABLE)
            with NamedTemporaryFile(mode="w", dir=".", delete=False) as tf:
                # For Windows portability.
                tname = tf.name
                print('"TWEET COUNTS BY MOVIE AND WEEK",,,,,,,,,,,,', file=tf)
                print('"If a movie does not appear here, all weeks are 0.",,,,,,,,,,,,',
                      file=tf)
                print('"Week 0 is the week BEFORE opening.",,,,,,,,,,,,',
                      file=tf)
                print('"MovieName","OpeningFriday"', end='', file=tf)
                for i in range(11):
                    print(',"Week{0:d}"'.format(i), end='', file=tf)
                print(file=tf)
                for m in movie_count:
                    print('"', m.name, '","', m.opening_date.strftime("%Y-%m-%d"),
                          '"', sep='', end='', file=tf)
                    for w in range(FIRST_WEEK, LAST_WEEK):
                        print(",{0:d}".format(movie_count[m][w]), end='', file=tf)
                    print(file=tf)
            newname = "./reports/movie-count-by-week.csv"
            os.rename(tname, newname)
            os.chmod(newname, WORLD_READABLE)
            # #### It would be nice if this code could be told to append.
            for header, name, dist in COUNT_REPORTS:
                with NamedTemporaryFile(mode="w", dir=".", delete=False) as tf:
                    print(header, file=tf)
                    # For Windows portability.
                    tname = tf.name
                    print("{0:s} has {1:d} entries.".format(name, len(dist)),
                          file=tf)
                    TRIES = 3
                    for i in range(TRIES):
                        try:
                            # I'm guessing this fails on the word distribution.
                            json.dump(OrderedDict(dist.most_common()), tf,
                                      indent=4)
                            break
                        except Exception:
                            print("Writing", name, "failed.  {}.".format("Giving up" if i == TRIES - 1 else "Retrying"))
                            continue
                newname = "./reports/" + name + ".out"
                os.rename(tname, newname)
                os.chmod(newname, WORLD_READABLE)
            with NamedTemporaryFile(mode="w", dir=".", delete=False) as tf:
                # For Windows portability.
                tname = tf.name
                fmt = "There were {0:d} objects, containing {1:d} unique in-period tweets."
                print(fmt.format(sampling_count['JSON objects'],
                                 len(tweet_data)),
                      file=tf)
                print("Tweets are counted only once, but may apply to multiple movies.", file=tf)
            newname = "./reports/summary.out"
            os.rename(tname, newname)
            os.chmod(newname, WORLD_READABLE)

    # #### This code doesn't work with tweet_data as list!
    if print_tweets_as_json:
        idnos = sorted(tweet_movies.keys())
        with open("./reports/tweet-json-by-id.out", "w") as f:
            print("TWEET DATA SORTED BY id\n", file=f)
            mcnt = ncnt = 0
            for idno in idnos:
                print("{0:d} ".format(idno), end='', file=f)
                if tweet_movies[idno]:
                    mcnt = mcnt + 1
                    for m in tweet_movies[idno]:
                        print('"{0}"'.format(m), end=' ', file=f)
                else:
                    ncnt = ncnt + 1
                print(json.dumps(tweet_data[idno].tweet, indent=4), file=f)


    if track_movie_tweets:
        with open("./reports/tweets-by-movie.out", "w") as f:
            print("\nTWEET CONTENT BY MOVIE, SORTED FOR SOME SIMILARITY\n",
                  file=f)
            for m in movie_tweets.keys():
                tweets = [t for t in movie_tweets[m] if t.words]
                tweets.sort(key=lambda t: t.words)
                print("{0:s} ({1:d}, {2:d}):".format(m.name,
                                                     len(movie_tweets[m]),
                                                     len(tweets)),
                      file=f)
                fmt = "{0:-18d} week={1:d} {2:s}"
                for t in tweets:
                    print(fmt.format(t.tweet['id'],
                                     m.timestamp_to_week(t.timestamp),
                                     t.text),
                          file=f)

    with open("./reports/movie-maps.out", "w") as of:
        # Need to define a special encoder.
        # print(json.dumps(movie.Movie.word_movies, indent=4))
        print("{", file=of)
        for w, l in movie.Movie.word_movies.items():
            print("    ", w, " : [", sep="", file=of)
            for m in l:
                print("        ", m.name, ",", sep="", file=of)
                print("        ]", file=of)
                print("}", file=of)
                print("{", file=of)
        for n, m in movie.Movie.by_name.items():
            print("   ", n, ": [ ", end="", file=of)
            print(*m.words, sep=", ", end=" ]\n", file=of)
        print("}", file=of)

    print("There are", len(files), "output files in the database.")
    print("There were {0:d} objects, ".format(sampling_count['JSON objects']),
          end='')
    print("containing {0:d} unique tweets.".format(len(tweet_data)))
    #print("{0:d} tweets with identified movie(s).".format(mcnt))
    #print("{0:d} tweets with no movie identified.".format(ncnt))  
    #print("should have matched while splitting: {}".format(should_match))
    #print("should not have matched while splitting: {}".format(should_not_match))
    session = Session()
    print("There are {0:d} words in the word distribution.".format(
        session.query(Word).count()))
    session.close()

if __name__ == "__main__":

    args = MongizeParser().parse_args()
    if not args.FILES:
        files = list(traverse_tweet_data(args.dataroot))
    else:
        files = args.FILES

    # Set up the JSON decoder.
    # #### I wonder if it would be worth fixing the Twitter module to store
    # directly into MongoDB.
    decoder = json.JSONDecoder()

    # Set up MongoDB client.
    # Linux version doesn't like the "mongodb://localhost/" URL, but the
    # host[/dbname] style works on Mac too.
    mongo = MongoClient("localhost")
    db = mongo.anna
    collection = db.status_stream
    collection.create_index("id", sparse=True)
    # #### Probably should do this with a timestamp instead.  That would
    # be more accurate and even if it involves running over the whole
    # database, this only needs to be done offline and occasionally.
    collection.create_index("anna:serial", sparse=True)
    
    # Read statuses.
    try:
        for fn in files:
            available = check_available("/home/steve/lv/mongodb",
                                        args.reserve * GB)
            if available < 0:
                break
            print("Available space for {} is {}, about {}GB.".format(fn,
                                                                     available,
                                                                     available//GB))
            sys.stdout.flush()
            with open(fn) as fo:
                handle_file(fo, collection)

    finally:
        with open("remaining.files.list", "w") as rfl:
            print("Current file =", fn, file=rfl)
            # Files is a partially exhausted generator object!
            # Yay, Python!
            # We could also iterate print, making prettier output.
            print("File list =\n{}\n".format(list(files)), file=rfl)
        # Report on the collection.
        print("Records in collection =", collection.count())
        print("Non-tweet records in collection =",
              collection.count({"id" : {"$exists" : False}}))
        print("Duplicates in collection =",
              collection.count({"anna:duplicate" : {"$exists" : True}}))
        print("Retweets in collection =",
              collection.count({"retweeted_status" : {"$exists" : True}}))
        print("Retweet has original tweet in collection =",
              collection.count({"anna:original" : {"$exists" : True}}))
        for status in collection.find({"id" : {"$exists" : False}}):
            print("Non-tweet =", status)
            serial = status["anna:serial"]
            try:
                print("    Previous =",
                      collection.find_one({"anna:serial" : serial - 1})["created_at"])
                print("    Next     =",
                      collection.find_one({"anna:serial" : serial + 1})["created_at"])
            except:
                print("    Couldn't get Previous or Next.  Multiple limits?")

        mongo.close()

