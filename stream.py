#!/usr/bin/env python3.4

# Stream tweets for a particular filter from the API.
# A sample of 1000 tweets with the filter "movie" collected from
# Thu Feb 19 02:50:15 +0000 2015 to Thu Feb 19 02:52:51 +0000 2015
# resulted in 4,376,897 bytes of formatted JSON or 4377 bytes/tweet
# in 156 seconds, a rate of 2424 MB/day and 553,846 tweets/day for
# an estimated 255 GB and 58,153,830 tweets in the data set.

from myauth import get_my_api
import json
import os
import os.path
import signal
import sys
import time
import twitter
import urllib

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
    # Released on Apr. 10
    "The Longest Ride",
    "Clouds of Sils Maria",
    "Desert Dancer",
    "Ex Machina",
    "Freetown",
    "Kill Me Three Times",
    "Rebels of the Neon God",           # (2015 re-release) 
    "The Sisterhood of Night",
    # Released on April 17th (Friday)
    "Child 44",
    "Monkey Kingdom",
    "Paul Blart: Mall Cop 2",
    "Unfriended",
    "Alex of Venice",
    "Beyond the Reach",
    "The Dead Lands",
    "Felix and Meira",
    "Monsters: Dark Continent",
    "The Road Within",
    "True Story",
    # Released on April 24th (Friday) 
    "The Age of Adaline",
    "Little Boy",
    "Adult Beginners",
    "After The Ball",
    "Brotherly Love",
    "Kung Fu Killer",
    "Misery Loves Comedy",
    "WARx2",
    "The Water Diviner",
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

signal_names = [name for name in dir(signal)
                if name.startswith("SIG") and not name.startswith("SIG_")]
signal_dict = {}
for name in signal_names:
    signal_dict[getattr(signal,name)] = name

def handle_signal(signum, frame):
    """Handle signal by raising OSError."""
    raise OSError(signum, signal_dict[signum], "<OS signal>")

i = 0
vol = 0
working = True
delay = 1                               # delay == 1 is ignored.

signal.signal(signal.SIGHUP, handle_signal)
signal.signal(signal.SIGTERM, handle_signal)

def generate_tweets(api):
    stream = twitter.TwitterStream(auth=api.auth)
    tweets = stream.statuses.filter(track=movies, stall_warnings=True)
    for tweet in tweets:
        yield tweet

while working:
    try:
        # #### results/20150325.091639/stream-results-13.json was left open and
        # stream.py restarted.  It appears to have skipped over that file?
        # (It's empty in the next series, too.)  What happened here?
        if delay > 1:
            time.sleep(delay)
            if delay < 900:
                delay = delay * 2
        tweets = generate_tweets(api)
        next(tweets)
        with open("stream-results-%d.json" % vol, "w") as f:
            delay = 1
            # #### When things break, we see
            # 1. stream-results-###.json in the directory list
            # 2. it does not show up as open in lsof
            # 3. a TCP connection to Twitter does not show up in lsof
            for tweet in tweets:
                print(json.dumps(tweet, indent=INDENT), file=f)
                i = i + 1
                if i % 100 == 0:
                    f.flush()
                # normal file rotation
                if i % COUNT == 0:
                    break
    # Handle signals, exiting somewhat gracefully by default.
    # SIGHUP breaks out of iteration and starts new volume.
    except OSError as e:
        print(e)
        if e.errno != signal.SIGHUP:
                working = False
        print("%s caught signal %d%s\n%s." \
              % (time.ctime(), e.errno, ", exiting" if not working else "",
                 e.strerror))
    except StopIteration as e:
        print(e)
    except (twitter.api.TwitterHTTPError,
            urllib.error.HTTPError,
            ConnectionResetError
            ) as e:
        print(dir(e))
        print(e)
        # AFAIK most of these errors indicate we should stop.
        #
        # 406 stops.  Normally it's a program error:
        #   Twitter sent status 406 for URL:
        #   1.1/statuses/filter.json using parameters: (...)
        #   details: b'Parameter track item index 69 too long: \
        #   The Longest Ride Clouds o\r\n'
        #
        # 420 resets delay to at least 900 seconds:
        #   Twitter sent status 420 for URL:
        #   1.1/statuses/filter.json using parameters: (...)
        #   details: b'Easy there, Turbo. Too many requests recently. \
        #   Enhance your calm.\r\n'
        working = False
        if str(e).startswith("Twitter sent status 503"):
            working = True
        elif str(e).startswith("Twitter sent status 420"):
            working = True
            delay = 1000                # approximately 16 minutes
    sys.stdout.flush()
    vol = vol + 1

print(i, "tweets done.")
sys.stdout.flush()
