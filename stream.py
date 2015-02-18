#!/usr/bin/env python3.4

# Stream tweets for a particular filter the API.

from myauth import get_my_api
from twitter.api import TwitterHTTPError
from urllib.error import HTTPError
import json

# #### Use command line options!!
QUERY = "Hobbit movie"
COUNT = 100

api = get_my_api()

# #### Everything below this line is bogus!

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

with open("twitter-search-results.json", "w") as f:
    for i in range(len(statuses)):
        print(json.dumps(statuses[i], indent=1), file=f)

print("limit", results.rate_limit_limit,
      "remaining", results.rate_limit_remaining,
      "reset", results.rate_limit_reset)
