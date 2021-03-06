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
import sys

PREFIX = ".."                           # #### Horrible hack!
EXEC_PREFIX = "."                       # #### Horrible hack two!
START = END = None                      # #### Horrible hack three!

# #### Duplicated work, need to fix in build_snw_db.
alias = {
    #"'71", "71"),
    #"Can't Stand Losing You: Surviving the Police",
    # "Can't Stand Losing You Surviving the Police"),
    #'Do You Believe?', 'Do You Believe'),
    #'Grandma', 'Grandma (2015)'),
    "Kahlil Gibran's The Prophet" : 'kahlil-gibran',
    #'Kumiko, The Treasure Hunter', 'Kumiko The Treasure Hunter'),
    'Love and Lost (Shi Gu)' : 'love-lost',
    #'Max', 'Max (2015)'),
    #'McFarland, USA', 'McFarland USA'),
    #'Mission: Impossible - Rogue Nation', 'Mission Impossible Rogue Nation'),
    #'Paul Blart: Mall Cop 2', 'Paul Blart Mall Cop 2'),
    #'Phoenix', 'Phoenix (2015)'),
    #'Poltergeist', 'Poltergeist (2015)'),
    #'Rififi', 'Rififi (2015 re-release)'),
    #'Seymour: An Introduction', 'Seymour An Introduction'),
    'Shaun the Sheep Movie' : 'shaun-sheep',
    'Gemma Bovery' : 'gemma-bovary',
    #'Tangerines', 'Tangerine'),
    #'The Divergent Series: Insurgent', 'The Divergent Series Insurgent'),
    #'The Gift', 'The Gift (2015)'),
    'The Young & Prodigious T.S. Spivet' : 'young-&-prodigious-t-s-spivet',
    }

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
    return tuple(stopword_re.sub("", s).split())

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
        p = len(slist)
        minwds = (minwds or self.minwds)
        result = []
        for i in range(p - minwds):
            for j in range(n - minwds):
                for k in range(j, n):
                    if i < p and self[k] == slist[i]:
                        i += 1
                    else:
                        break
                if k - j >= minwds:
                    # #### Need to update match distribution
                    result.append((i, j, k))
                    break
        return result

def init_word_db():
    db = sql.connect(":memory:")
    create_tables(db)
    populate_from_sentiwordnet(db, PREFIX)
    return db

class Movie(object):
    """
    Construct lists of include and exclude NGrams.
    """

    minwds = 2                          # #### Unused in practice?
    wdb = init_word_db()
    hashes_seen = set()

    def __init__(self, name, minwds=None):
        self.name = name
        self.minwds = minwds or self.minwds
        self.ngram = NGram(name, self.minwds)
        self.includes = [self.ngram] if len(self.ngram) >= self.minwds else []
        self.excludes = []
        db = sql.connect(os.path.join(PREFIX, "twitter.sql"))
        c = db.cursor()
        c.execute("select Includes,MustInclude,Excludes,Director,Actors,"
                  "ReleaseMonth,ScheduledRelease "
                  "from movies where name=?", (name,))
        inc, must, exc, dirs, stars, actual, scheduled = c.fetchone()
        c.close()
        db.close()
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
        # ... and from below.  What a f--king mess!
        self.stem = alias.get(self.name, "-".join(
            [w for w
             in self.name.translate(moviedata.PUNCT).lower().split()
             if w not in moviedata.STOPSET]
            ))

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
        hash_repeat_count = 0

        # Connect to word database.
        cur = self.wdb.cursor()

        filename = "%s/%s-%d.json" % (PREFIX, self.stem, week)
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
            if 'retweeted_status' in tweet:
                tweet = tweet['retweeted_status']
                tid = tweet['id']
                rcnt = tweet['retweet_count']
                if tid in tids_seen:
                    # Keep track of repeats and maximum retweet counts.
                    seen = tids_seen[tid]
                    seen[0] += 1
                    seen[1] = max(rcnt, seen[1])
                    continue
                tids_seen[tid] = [1, rcnt]

            tokens = prep_text(tweet['text'])
            tokhash = hash(tokens)
            if tokhash in self.hashes_seen:
                hash_repeat_count += 1
            else:
                self.hashes_seen.add(tokhash)

            for ngram in self.excludes:
                if ngram.match(tokens):
                    exclude_matches += 1
                    continue

            if self.must_match_include:
                results = []
                for ngram in self.includes:
                    results.extend(ngram.match(tokens))
                if not (results or any(movie_word_re.match(t) for t in tokens)):
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
                if res is not None and res[1] is not None:
                    m += 1
                    pos += res[1]
                    neg += res[2]
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
            hash_repeat_count,          # 20 Hash repeat count
            week,                       # 21 Week
            ]
        for r in tids_seen.values():
            results[6] += r[0]
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
        # #### Can numerators be non-zero if denominators are zero?
        if results[9]:
            results[13] = results[10] / results[9]
            results[17] = results[14] / results[9]
        if results[8]:
            results[12] = results[10] / results[8]
            results[16] = results[14] / results[8]
        if results[4]:
            results[11] = results[11] / results[4]  # #### per unique tweet?
            results[10] = results[10] / results[4]
            results[15] = results[15] / results[4]  # #### per unique tweet?
            results[14] = results[14] / results[4]
            results[18] = results[18] / results[4]
            results[19] = results[19] / results[4]
        # 10. Record.
        return tuple(results)

def resample(sample, prefix, start, end):
    """
    Take a list of movies in sample, determine sizes related files in prefix,
    sort in order of size, and analyze the set from start to end, beginning
    with the smallest movie.
    """
    if start is None:
        return sample
    result = []
    for m in sample:
        stem = alias.get(m, "-".join(
            [w for w
             in m.translate(moviedata.PUNCT).lower().split()
             if w not in moviedata.STOPSET]
            ))
        n = len(stem) + 1
        sz = 0
        files = os.listdir(prefix)
        for fn in files:
            if fn.startswith(stem) and fn[n + 1] != "0" \
               and fn[n] not in "89":
                sz += os.path.getsize(os.path.join(prefix, fn))
        result.append((m, sz))
    result.sort(key=lambda x: x[1])
    result = result[start : end]
    try:
        result.remove("The DUFF")
    except ValueError:
        pass
    print(*(y[1] // 1000 for y in result))
    return [y[0] for y in result]
    

if __name__ == "__main__":
    if 0:                               # #### Move this to a unit test.
        s = prep_text("1 2 3 4 5 & 6'")
        n1 = NGram("3 4 1", 3)
        n2 = NGram("3 4 1", 2)
        print("slist =", s)
        print("n1 match is", n1.match(s))
        print("n2 match is", n2.match(s))

    # print headers to CSV
    csvfile = os.path.join(EXEC_PREFIX, "movie-week-sentiment-1.csv")
    printheader = not os.path.exists(csvfile)
    with open(csvfile, "a") as f:
        if printheader:
            print(",".join([
                "Name of movie",
                "Total tweets in file",
                "Count of tweets excluded by excludes",
                "Count excluded by no includes",
                "Count of tweets included",
                "Count of unique tweets by ID",
                "Count of duplicates in database",
                "Estimate of valid retweet count",
                "Count of tokens processed",
                "Count of valent tokens found",
                "Average positive per tweet",
                "Average positive per tweet (weighted by log1p(retweets))",
                "Average positive per token",
                "Average positive per valent token",
                "Average negative per tweet",
                "Average negative per tweet (weighted by log1p(retweets))",
                "Average negative per token",
                "Average negative per valent token",
                "Average hashtags per tweet",
                "Average urls per tweet",
                "Hash repeat count",
                "Week",
                ]),
                  file=f)
        # get movie list
        db = sql.connect(os.path.join(PREFIX, "twitter.sql"))
        c = db.cursor()
        c.execute("select Name from movies where InSample=1")
        sample = [x[0] for x in c]
        c.close()
        db.close()

        sample = resample(sample, PREFIX, 75, 125)
        print(sample, sys.stderr)
    
        for movie in sample:
            m = Movie(movie)
            for week in range(8):
                try:
                    print('"%s",%d,%d,%d,%d,%d,%d,%d,%d,%d,'
                          '%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,'
                          '%.3f,%.3f,%d,%d' % m.process_week(week),
                          file=f)
                    f.flush()
                except FileNotFoundError:
                    print("File not found:", week, "of", movie, file=sys.stderr)
    # close up shop.
    Movie.wdb.close()
