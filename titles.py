"""
titles.py

Read movie titles from a file, and set up the list of filters.
"""

import argparse
import re

PUNCT = { ord(',') : None,
          ord('?') : None,
          ord(':') : None,
          ord(';') : None,
          }

def clear_parens(s):
    """
    Remove patenthesized content (including parentheses).
    Assumes proper nesting of parentheses.
    """
    s, n = re.subn(r"\([^(]*¥)", "", s)
    while n:
        s, n = re.subn(r"\([^(]*¥)", "", s)
    return s
    
def track_join(ks):
    """
    Romove punctuation (especially commas) from keys, then join with commas.
    Unwanted punctuation is defined by the global variable PUNCT, which is
    a dict appropriate for use with str.translate.
    """
    return ','.join(k.translate(PUNCT) for k in ks)

parser = argparse.ArgumentParser(description="Read the name of a file of movie titles")
parser.add_argument('TITLES', type=str, help="File of movie titles")
args = parser.parse_args()

with open(args.TITLES) as titles:       # #### Need to handle Twitter API too.
    s = titles.read()                   # #### Need online algorithm.

