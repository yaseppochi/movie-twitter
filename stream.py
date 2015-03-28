#!/usr/bin/env python3.4

# Stream tweets for a particular filter from the API.
# A sample of 1000 tweets with the filter "movie" collected from
# Thu Feb 19 02:50:15 +0000 2015 to Thu Feb 19 02:52:51 +0000 2015
# resulted in 4,376,897 bytes of formatted JSON or 4377 bytes/tweet
# in 156 seconds, a rate of 2424 MB/day and 553,846 tweets/day for
# an estimated 255 GB and 58,153,830 tweets in the data set.

from myauth import get_my_api
import twitter
import json
import os
import os.path
import signal
import sys

# #### Use command line options!!
MOVIES = [ # "movie",
    # Released on Feb. 2
    "The DUFF",
    "Hot Tub Time Machine 2",
    "McFarland, USA",
    "Badlapur",
    "Digging Up the Marrow",
    "Queen and Country",
    "Wild Tales",
    # Released on Feb. 27
    "Focus",                            #  (2015)
    "The Lazarus Effect",
    "'71",
    "Deli Man",
    "Eastern Boys",
    "Everly",
    "Farewell to Hollywood",
    "Futuro Beach",
    "The Hunting Ground",
    "A La Mala",
    "Maps to the Stars",
    "My Life Directed",
    "The Salvation",
    "Snow Girl and the Dark Crystal",
    "Wild Canaries",
    # Released on Mar. 6
    "Chappie",
    "Unfinished Business",
    "Bad Asses on the Bayou",
    "Compared to What? The Improbable Journey of Barney Frank",
    "Hayride 2",
    "The Life and Mind of Mark DeFriest",
    "The Mafia Only Kills in Summer",
    "Merchants of Doubt",
    "The Second Best Exotic Marigold Hotel",
    "These Final Hours",
    # Released on Mar. 13
    "Cinderella", # (2015)
    "Run All Night",
    "Eva",
    "Home Sweet Hell",
    "It Follows",
    "Seymour: An Introduction",
    "The Tales of Hoffmann", # (2015 re-issue)
    "The Wrecking Crew",
    # Released on Mar. 20
    "The Divergent Series: Insurgent",
    "Do You Believe?",
    "The Gunman",
    "Can't Stand Losing You: Surviving the Police",
    "Danny Collins",
    "Kumiko, The Treasure Hunter",      # (on Wed.)
    "Love and Lost",                    # (Shi Gu)
    "Shi Gu",
    "Spring",                           # (2015)
    "Zombeavers",
    # Released on Mar. 27
    "Get Hard",
    "Home",                             # (2015)
    "Cupcakes",
    "A Girl Like Her",
    "Man From Reno",
    "The Riot Club",
    "The Salt of the Earth",
    "Serena",
    "Welcome to New York",
    "While We're Young",
    "White God",
    # Released on Apr. 3
    "Furious 7",
    "5 to 7",
    "Cheatin'",
    "Effie Gray",
    "Lambert & Stamp",
    "The Living",
    "SBK The-Movie",
    "Woman in Gold",
    ]
COUNT = 10000
INDENT = 1                              # INDENT=0 doesn't help.  None?
PUNCT = { ord(',') : None,
          ord('?') : None,
          ord(':') : None,
          ord(';') : None,
          }
def track_join(ks):
    return ','.join(k.translate(PUNCT) for k in ks)

movies = track_join(MOVIES)
print(movies)
sys.stdout.flush()

api = get_my_api()

def handle_signal(signum, frame):
    """Handle signal by raising IOError."""
    print("Caught signal", signum)
    raise IOError(signum)

i = 0
vol = 0
working = True

signal.signal(signal.SIGHUP, handle_hup)

def interate_tweets(api):
    stream = twitter.TwitterStream(auth=api.auth)
    tweets = stream.statuses.filter(track=movies, stall_warnings=True)
    for tweet in tweets:
        yield tweet

while working:
    try:
        # #### results/20150325.091639/stream-results-13.json was left open and
        # stream.py restarted.  It appears to have skipped over that file?
        # (It's empty in the next series, too.)  What happened here?
        tweets = generate_tweets()
        next(tweets)
        with open("stream-results-%d.json" % vol, "w") as f:
            for tweet in tweets:
                print(json.dumps(tweet, indent=INDENT), file=f)
                i = i + 1
                if i % 100 == 0:
                    f.flush()
                # normal file rotation
                elif i % COUNT == 0:
                    vol = vol + 1
                    break
    except IOError as e:
        print(e)
        # Exit somewhat gracefully on signals by default.
        working = False
        if e.errno == signal.SIGHUP:
            print("Caught SIGHUP, reinitializing stream and file %d." % (vol,))
            vol = vol + 1
            # I don't know how to close a Twitter API connection,
            # so just drop it on the floor.
            working = True
        # Normal exit (#### maybe this could be SIGTERM?)
        elif e.errno == signal.SIGUSR1:
            print("Caught SIGUSR1, exiting.")
        else:
            print("Caught signal %d, exiting.\n%s\n" % (e.errno, e.errstring))
    except StopIteration as e:
        print(e)
        need_connect = True
    except ConnectionResetError as e:
        print(e)
        need_connect = True
    sys.stdout.flush()

print(i, "tweets done.")
sys.stdout.flush()
