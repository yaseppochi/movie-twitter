# movie-twitter

This is a collection of scripts and modules I developed to help a student collect data from the Twitter API on conversations about movies.

The most interesting file is `stream.py`, which continuously pulls out tweets from Twitter's streaming API that refer to a list of current movies over an 11 week lifecycle.  Of course movies frequently have longer runs that that, but most movies get most of their revenue from the initial 2.5 months.  We also tracked tweets for the week before scheduled release.  The script ran mostly continuously for about 6 months, and collected about 1TB of tweet data (note that although the text itself is limited to 140 bytes at that time, a full status is typically about 4kB, and statuses of 20kb are not unheard of.

`stream.py` and its imports `movie.py` and `moviedata.py` are intended to be production-quality code, since it ran unattended for a period of months.

Other code is auxiliary, often one-off scripts for fixing the data, or testing.
