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

# #### Use command line options!!
QUERY = "movie"
COUNT = 1000
INDENT = 1

api = get_my_api()

stream = twitter.TwitterStream(auth=api.auth)
tweets = stream.statuses.filter(track=QUERY)

with open("stream-results.json", "w") as f:
    for i in range(COUNT):
        print(json.dumps(next(tweets), indent=INDENT), file=f)
        print(".", end='' if (i+1)%65 else '\n')
    
print(COUNT, "tweets done.")

