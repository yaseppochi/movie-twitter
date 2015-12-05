This folder contains data and Python programs related to Anna Chen's
movie revenue prediction using Twitter stream data project.

Studying programs here may be of use in other projects using Twitter
stream data, and perhaps other streaming SNS APIs.  However, little
effort has been made to factor out generically useful modules yet.
Don't expect to be able to "import tweet_stream" and do useful
analysis on a data set created with two or three statements.

This directory was created on Dec. 5, 2015.  Expect inaccuracies.

Directory /Users/steve/src/Twitter/
    26 Dec  4 22:18 00README.txt              # this file
   476 Dec  3 01:18 AnnaProgs                 # subfolder for Anna's code
 25864 Dec  4 22:14 Database for thesis.csv   # by-movie data Anna collected
   340 Dec  4 20:10 __pycache__               # used by the Python interpreter
  6444 Jul 25 23:35 *.pem                     # .pem files are personal ID
 34975 Sep 18 22:00 annamoviessept
  2771 Jul 10 21:28 case-protect.py
  1201 Apr 19  2015 cleandir.py
   544 Jul 27 14:28 demoCA                    # authentication authority
 19506 Jul 11 03:51 mongize-tweets.py         # mongo* files not used
  8184 Jul 25 18:41 mongo.cnf
  3852 Jul 30 22:37 mongoauth.py
  4753 Jul 27 20:17 mongodb-ssl-setup.txt
  3830 Nov 26 00:21 movie.py                  # Python module for movie objects
 16495 Nov 29 22:48 moviedata.py              # Movie and scheduled release DB
  1072 Feb 18  2015 myauth.py
  2705 Dec  4 20:09 names.txt
 15125 Nov 30 12:48 partition_tweets.py       # partition tweets by movie & week
 14210 Jul  5 23:32 prep-tweets.py
   374 May 31  2015 reports
   136 May 24  2015 results
    26 Mar 27  2015 robots.txt                # used to reduce SE activity
  2736 Mar 12  2015 search.py
 10911 Sep  9 03:43 stream.py
   850 Nov 30 23:58 tests
  1948 Apr 28  2015 tweetcheck.py
  6613 May 25  2015 tweetdata.py
  3966 Dec  5 01:55 twitter_build_db.py       # build the SQL database
  1095 Dec  5 14:18 twitter_csv_to_sql.py     # used by twitter_build_db.py
 46080 May 31  2015 word-distribution.db      # words used in tweets

Changes to original data not documented in programs

- Repair variable name 'TSentiScoreW7' in "Database for thesis.csv".

