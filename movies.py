"""
Module to read movie titles from a file, and set up the list of filters.
"""

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
    s, n = s.subst(r"\([^(]*¥)","")
    while n:
        s, n = s.subst(r"\([^(]*¥)","")
    return s
    
def track_join(ks):
    return ','.join(k.translate(PUNCT) for k in ks)
