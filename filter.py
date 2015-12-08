"""
Collect natural language information about tweet texts.
"""

from build_snw_db import (
    create_tables,
    populate_from_sentiwordnet,
    term_extract_command,
    )
from math import log1p
import moviedata
from partition_tweets import json_source
import os
import os.path
import re
import sqlite3 as sql

url_re = re.compile(r"(?:ht|f)tps?://\S*")

stopword_re = re.compile(
    r"\b(?:"
    r"a|an|and|as|but|for|of|on|or|some|the|to"
    r")\b"
    )

movie_word_re = re.compile(
    r"actor|actress|cinema|director|film|go|movie|review|saw|screen|see"
    r"|theater|watch|went|view"
    )

STOPPUNCT = {
    ord('.'): ' ',
    ord(','): ' ',
    ord(';'): ' ',
    ord(':'): ' ',
    ord('?'): ' ',
    ord('!'): ' ',
    ord('/'): ' ',
    ord('-'): ' ',
    ord('^'): ' ',
    ord('~'): ' ',
    ord('|'): ' ',
    ord('&'): ' ',
    ord('"'): None,
    ord("'"): None,
    ord('['): None,
    ord(']'): None,
    ord('('): None,
    ord(')'): None,
    ord('<'): None,
    ord('>'): None,
    }

def prep_text(s):
    """
    1.  Lowercase the string.
    2.  Remove URLs.  #### Other Twitter syntax?
    3.  Replace punctuation with space, except delete quotes and apostrophe.
        Leave hashtags and user references.
    4.  Remove stopwords.
    5.  Split into words.
    """

    s = url_re.sub("", s.lower()).translate(STOPPUNCT)
    return stopword_re.sub("", s).split()

class NGram(list):
    """
    Initialize with a string and a minimum length of match.
    If minimum length is None or not provided, use n.
    1.  Lowercase the string.
    2.  Replace punctuation with space, except delete quotes and apostrophe.
    Leave hashtags and user references.
    3.  Remove stopwords.
    4.  Split into words.
    """

    all_ngrams = []

    def __init__(self, s, minwds=None):
        # Could use prep_text, but speed matters.
        # #### Maybe it doesn't since it's only a few times per movie.
        self.extend(stopword_re.sub("", s.lower().translate(STOPPUNCT)).split())
        self.minwds = minwds or len(self)
        # Register self.
        self.id = len(self.all_ngrams)
        self.all_ngrams.append(self)
        # Initialize distribution.
        # #### Distribution of what?  Not of matching tweets.  (Multi-match.)
        self.distribution = [0] * (len(self) * (len(self) + 1) // 2)
        
    def match(self, slist, minwds=None):
        """
        Match an n-gram and its sub-m-grams to a list of strings.
        m must be at least minwds to count as a match.
        May not find the longest match if a word appears multiple times.
        """
        n = len(self)
        minwds = (minwds or self.minwds)
        result = []
        for i in range(len(slist) - minwds):
            for j in range(n - minwds):
                for k in range(j, n):
                    if self[k] == slist[i]:
                        i += 1
                    else:
                        break
                if k - j >= minwds:
                    # #### Need to update match distribution
                    result.append((i, j, k))
                    break
        return result

class Movie(object):
    """
    Construct lists of include and exclude NGrams.
    """

    minwds = 2
    prefix = "/var/local/twitterdb"

    def __init__(self, name, minwds=None):
        self.name = name
        self.minwds = minwds or self.minwds
        self.ngram = NGram(name, self.minwds)
        self.includes = [self.ngram] if len(self.ngram) >= self.minwds else []
        self.excludes = []
        c = sql.connect("twitter.sql").cursor()
        c.execute("select Includes,MustInclude,Excludes,Director,Actors,"
                  "ReleaseMonth,ScheduledRelease "
                  "from movies where name=?", (name,))
        inc, must, exc, dirs, stars, actual, scheduled = c.fetchone()
        c.close()
        self.must_match_include = must
        # #### Refactor to make includes empty on not must?
        if dirs is not None:
            dirs = dirs.split("[,/]")
            for x in dirs:
                self.includes.append(NGram(x,1))
        if stars is not None:
            stars = stars.split("[,/]")
            for x in stars:
                self.includes.append(NGram(x,1))
        if inc is not None:
            inc = inc.split("[,/]")
            for x in inc:
                self.includes.append(NGram(x))
        if exc is not None:
            exc = exc.split("[,/]")
            for x in exc:
                self.excludes.append(NGram(x))
        # #### Duplicated algorithm from movie.Movie.__init__.
        self.stem = "-".join(
            [w for w
             in self.name.translate(moviedata.PUNCT).lower().split()
             if w not in moviedata.STOPSET]
            )

    def process_week(self, week):
        """
        1.  Get the JSON source.
        2.  Read tweets.
        3.  Get text, preferably from retweeted_status.
        4.  Preprocess text into token sequence.
        5.  Check exclude criteria (log exclusion).
        6.  If required, check include criteria (log exclusion).
        7.  Extract valences, per tweet and per word.
        8.  Extract URL and media features (I don't think we have media?)
        9.  Count and average.
        10. Record.
        """

        # Initialize counters.
        total_tweets = 0
        exclude_matches = 0
        must_include_mismatches = 0
        tids_seen = {}

        # Initialize word database.
        db = sql.connect(":memory:")
        create_tables(db)
        populate_from_sentiwordnet(db)
        cur = db.cursor()

        filename = "%s/%s-%d.json" % (self.prefix, self.stem, week)
        for tweet in json_source([filename]): # These should all be tweets.
            # If we were going to do anything with user info from
            # individual tweets, we'd have to get it here.
            # Prefer the retweeted_status for longer text.
            total_tweets += 1

            tid = tweet['id']
            if tid in tids_seen:
                tids_seen[tid][0] += 1
                continue
            tids_seen[tid] = [1, 0]
            tweet = tweet.get('retweeted_status', tweet)
            tid = tweet['id']
            rcnt = tweet['retweet_count']
            if tid in tids_seen:
                # Keep track of maximum retweet counts.
                tids_seen[tid][1] = max(rcnt, tids_seen[tid][1])
                continue
            tids_seen[tid] = [0, rcnt]

            tokens = prep_text(tweet['text'])

            for ngram in self.excludes:
                if ngram.match(tokens):
                    exclude_matches += 1
                    continue

            if self.must_match_include:
                results = []
                for ngram in self.includes:
                    results.extend(ngram.match(tokens))
                if not (results or any(movie_words.match(t) for t in tokens)):
                    must_include_mismatches += 1
                    continue

            tid_values = tids_seen[tid]
            # 7.  Extract valences, per tweet, per word, per valent word
            n = len(tokens)
            m = 0
            pos = neg = 0.0
            for token in tokens:
                cur.execute(term_extract_command, (token,))
                res = cur.fetchone()
                if res is not None:
                    m += 1
                    if res[1] is None or res[2] is None:
                        print("broken valence:", token, res[1], res[2])
                    pos += res[1] or 0.0
                    neg += res[2] or 0.0
            tid_values.append(n)        # token_count_index = 2
            tid_values.append(m)        # valent_count_index = 3
            tid_values.append(pos)      # tweet_pos_index = 4
            tid_values.append(neg)      # tweet_neg_index = 5
            # 8.  Extract URL and media features (I don't think we have media?)
            tid_values.append(len(tweet['entities']['hashtags']))
                                        # hashtag_count_index = 6
            tid_values.append(len(tweet['entities']['urls']))
                                        # url_count_index = 7
            # #### Collect user stats here.
            # Done with this tweet.
            print(tid_values)

        # 9a.  Count ...
        results = [
            self.name,                  # 0 Name of movie
            total_tweets,               # 1 Total tweets in file
            exclude_matches,            # 2 Count of tweets excluded by excludes
            must_include_mismatches,    # 3 Count excluded by no includes
            total_tweets - exclude_matches - must_include_mismatches,
                                        # 4 Count of tweets included
            len(tids_seen),             # 5 Count of unique tweets by ID
            0,                          # 6 Count of duplicates in database
            0,                          # 7 Estimate of valid retweet count
            0,                          # 8 Count of tokens processed
            0,                          # 9 Count of valent tokens found
            0.0,                        # 10 Average positive per tweet
            0.0,                        # 11 Average positive per tweet
                                        #    (weighted by log1p(retweets))
            0.0,                        # 12 Average positive per token
            0.0,                        # 13 Average positive per valent token
            0.0,                        # 14 Average negative per tweet
            0.0,                        # 15 Average negative per tweet
                                        #    (weighted by log1p(retweets))
            0.0,                        # 16 Average negative per token
            0.0,                        # 17 Average negative per valent token
            0.0,                        # 18 Average hashtags per tweet
            0.0,                        # 19 Average urls per tweet
            ]
        for r in tids_seen.values():
            results[6] += r[0] - 1
            if len(r) < 3:
                continue
            results[7] += r[1]
            results[8] += r[2]
            results[9] += r[3]
            results[10] += r[4]
            results[11] += r[4] * log1p(1 + r[7])  # include original tweet
            results[14] += r[5]
            results[15] += r[5] * log1p(1 + r[7])  # include original tweet
            results[18] += r[6]
            results[19] += r[7]
        # 9b. ... and average.
        results[13] = results[10] / results[9]
        results[12] = results[10] / results[8]
        results[11] = results[11] / results[5]  # #### per unique tweet?
        results[10] = results[10] / results[5]
        results[17] = results[14] / results[9]
        results[16] = results[14] / results[8]
        results[15] = results[15] / results[5]  # #### per unique tweet?
        results[14] = results[14] / results[5]
        results[18] = results[18] / results[5]
        results[19] = results[19] / results[5]
        # 10. Record.
        return results


if __name__ == "__main__":
    s = prep_text("1 2 3 4 5 & 6'")
    n1 = NGram("3 4 1", 3)
    n2 = NGram("3 4 1", 2)
    print("slist =", s)
    print("n1 match is", n1.match(s))
    print("n2 match is", n2.match(s))
