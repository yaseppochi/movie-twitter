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
QUERY = "Hobbit movie"
COUNT = 100

api = get_my_api()

# #### This should be initialized "null".
results = api.search.tweets(q=QUERY, count=COUNT)
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
        print(json.dumps(statuses[i], indent=1), file=f)

print("limit", results.rate_limit_limit,
      "remaining", results.rate_limit_remaining,
      "reset", results.rate_limit_reset)
