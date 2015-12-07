"""
Collect natural language information about tweet texts.
"""

import re

url_re = re.compile(r"(?:ht|f)tps?://\S*")

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

stopword_re = re.compile(r"\b(?:"
                         r"a|an|and|as|but|for|of|on|or|some|the|to"
                         r")\b")

MOVIE_WORDS = ["movie", "movies", "film", "films", "theater", "theaters",
               "go", "went", "going", "see", "saw", "seeing", "watch",
               "watched", "actor", "actress", "director", "screen"]

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

    all_ngrams = []

    def __init__(self, s, minwds):
        """
        Initialize with a string and a minimum length of match.
        1.  Lowercase the string.
        2.  Replace punctuation with space, except delete quotes and apostrophe.
        Leave hashtags and user references.
        3.  Remove stopwords.
        4.  Split into words.
        """

        # Could use prep_text, but speed matters.
        # #### Maybe it doesn't since it's only a few times per movie.
        self.extend(stopword_re.sub("", s.lower().translate(STOPPUNCT)).split())
        self.minwds = minwds
        # Register self.
        self.id = len(self.all_ngrams)
        self.all_ngrams.append(self)
        # Initialize distribution.
        # #### Distribution of what?  Not of matching tweets.  (Multi-match.)
        self.distribution = [0] * (len(self) * (len(self) + 1) // 2)
        
    def match(self, slist, minwds=None):
        """
        Match an n-gram and its sub-m-grams to a list of strings.
        m must be at least min to count as a match.
        May not find the longest match if a word appears multiple times.
        """
        n = len(self)
        minwds = minwds or self.minwds
        minwds = n - 1 if minwds > n else minwds - 1
        result = []
        for i in range(len(slist) - minwds):
            for j in range(n - minwds):
                for k in range(j, n):
                    if self[k] == slist[i]:
                        i += 1
                    else:
                        break
                if k - j > minwds:
                    result.append((i, j, k))
                    break
        return result

if __name__ == "__main__":
    s = prep_text("1 2 3 4 5 & 6'")
    n1 = NGram("3 4 1", 3)
    n2 = NGram("3 4 1", 2)
    print("slist =", s)
    print("n1 match is", n1.match(s))
    print("n2 match is", n2.match(s))
