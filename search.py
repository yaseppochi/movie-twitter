#!/usr/bin/env python3.4

# Perform a single Twitter search via the API.
# A sample of 200 tweets for "Hobbit movie" resulted in a file of 754,606 bytes
# of formatted JSON, for an average of 3773 bytes/tweet.  The period covered
# was Wed Feb 18 10:16:57 +0000 2015 to Tue Feb 17 12:39:42 +0000 2015, or
# 77835 seconds = 222 tweets/day, implying 17,904 tweets/movie in 65MB.

from myauth import get_my_api
from twitter.api import TwitterHTTPError
from urllib.error import HTTPError
import json

# #### Use command line options!!
MOVIES = [ # "movie",
#     # Released on Feb. 2
#     "The DUFF",
#     "Hot Tub Time Machine 2",
#     "McFarland, USA",
#     "Badlapur",
#     "Digging Up the Marrow",
#     "Queen and Country",
#     "Wild Tales",
#     # Released on Feb. 27
#     "Focus (2015)",
#     "The Lazarus Effect",
#     "'71",
#     "Deli Man",
#     "Eastern Boys",
#     "Everly",
#     "Farewell to Hollywood",
#     "Futuro Beach",
#     "The Hunting Ground",
#     "A La Mala",
#     "Maps to the Stars",
#     "My Life Directed",
#     "The Salvation",
#     "Snow Girl and the Dark Crystal",
#     "Wild Canaries",
#     # Released on Mar. 6
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
    ]

QUERY = "Hobbit movie"
COUNT = 500
INDENT = 1

api = get_my_api()

for QUERY in MOVIES:
    # #### This should be initialized "null".
    results = api.search.tweets(q=QUERY, count=COUNT)
    print(QUERY)
    print(dir(results))

    # #### This too.
    statuses = results['statuses']
    for _ in range(5):
        print(len(statuses), "received so far.")
        try:
            next_query = results['search_metadata']['next_results']
        except KeyError as e:
            print("Not a key:", e)
            break
        except HTTPError as e:
            print(e)
            print("You have probably exceeded a rate limit!!")
            break
        except TwitterHTTPError as e:
            print(e)
            print("You have probably exceeded a rate limit!!")
            break
        kw = dict([kv.split('=') for kv in next_query[1:].split('&')])
        results = api.search.tweets(**kw)
        statuses += results['statuses']
print("Done.")

with open("search-results.json", "w") as f:
    for i in range(len(statuses)):
        print(json.dumps(statuses[i], indent=INDENT), file=f)

print("limit", results.rate_limit_limit,
      "remaining", results.rate_limit_remaining,
      "reset", results.rate_limit_reset)
