#! /opt/local/bin/python3.4

# Set up the imports and other stuff needed to access the tweet
# database.  Assume that tweets are already in an iterable called
# "dataset".
import argparse
import json
import os.path
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
    parser.add_argument('sources', type=str, nargs="+",
                        help="""Sources of JSON tweets.
Can be a folder containing folders with names like "YYYYMMDD.HHMMSS",
or a space-separated list of folders with names like "YYYYMMDD.HHMMSS",
or a space-separated list of files with names like "stream-results-NNN.json".
NOTE: Mixing the three forms is unimplemented.
""")
    args = parser.parse_args()
    # Unlovely(?) onepass algorithm.
    if args.prefix is not None:
        srclist = [os.path.join(args.prefix, x) for x in args.sources]
    stampre = r"2015[01][0-9][0-3][0-9]\.[0-2][0-9][0-6][0-9][0-6][0-9]"
    stampre = re.compile(stampre)
    sources = []

    def add_one(item, sources):
        if item.endswith(".json"):
            if os.path.isfile(item):
                sources.append(item)
            else:
                print("Ignoring nonfile: %s" % item, file=stderr)
            return True
        return False

    def add_many(item, sources):
        if stampre.match(item):
            if os.path.isdir(item):
                for f in os.listdir(item):
                    addone(item, sources)
            else:
                print("Ignoring nondirectory: %s" % item, file=stderr)
            return True
        else:
            return False

    def add_folder(item, sources):
        if os.path.isdir(item):
            for f in os.listdir(item):
                addmany(item, sources)
        else:
            print("Ignoring nondirectory: %s" % item, file=stderr)

    for x in srclist:
        addfile(x, sources) or addmany(x, sources) or add_folder(x, sources)


def break_this_motherfucker_up():
    with open(args.STREAM) as stream:       # TODO: Need to handle Twitter API too.
        s = stream.read()                   # TODO: Need online algorithm.
                                            # Do it with a JSON(Stream)?Decoder
    decoder = json.JSONDecoder()
    tweet, start = decoder.raw_decode(s)
    start = start + 1                       # skip NL
    end = len(s)
    
    
    while True:
        try:
            tweet, offset = decoder.raw_decode(s[start:start+50000])
            start = start + offset + 1
        except Exception as e:
            print("Error:", e, file=stderr)
            print("|", s[start:start+5000], sep='', file=stderr)
            print("len(s) =", end, "start = ", start, file=stderr)
            if start >= len(s):
                break
            elif s[start] == '\n':
                start = start + 1
                continue
            else:
                break
    
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
    for f in json_files:
        print(f)
