from moviedata import STOPSET

class SamplingException(Exception):
    pass

class MissingRequiredKeyException(SamplingException):
    pass

class BadLangException(SamplingException):
    pass

class TweetData(object):
    """
    Collect relevant data from a status and prepare it for analysis.
    """

    stopset = STOPSET
    str_int_keys = ['timestamp_ms']
    required_keys = ['id','text', 'created_at']
    general_keys = ['lang', 'favorite_count']
    general_keys.extend(required_keys)
    entity_keys = ['urls', 'hashtags']
    retweet_keys = ['retweet_count']
    location_keys = ['coordinates', 'geo', 'place',
                     'user.location', 'user.time_zone']
    def __init__(self, status):
        self.tweet = {}
        self._collect_attrs(self.tweet, status)
        self._filter()
        self._collect_entity_text(status)
        self._collect_miscellaneous(status)
        self._clean_text()
        self._canonicalize_text()
        self.all_words = sorted(set(self.words + self.entity_words))

    def _filter(self):
        """
        Raise a SamplingException on a tweet that is out of sample.
        """
        # #### These exceptions are probably mutually exclusive, but if not
        # we interpret BadLang as applying only to valid tweets.
        global required_missing_count, badlang_count
        missing = []
        for k in self.required_keys:
            if self.tweet[k] is None:
                missing.append(k)
        if missing:
            required_missing_count += 1
            raise MissingRequiredKeyException(missing)
        # #### Are there 3-letter RFC 3166 codes starting with "en"?
        if 'lang' in self.tweet and not self.tweet['lang'].startswith('en'):
            print(self.tweet['text'][:78])
            badlang_count += 1
            raise BadLangException(self.tweet['lang'])

    # The tweet argument allows recursion for retweets.
    def _collect_attrs(self, tweet, status):
        for k in TweetData.general_keys:
            # #### Use status.get(k) here.
            tweet[k] = status[k] if k in status else None
        for k in TweetData.str_int_keys:
            # Can't use .get() here!
            tweet[k] = int(status[k]) if k in status else None
        if 'entities' in status:
            entities = status['entities']
            for k in TweetData.entity_keys:
                tweet[k] = entities[k] if k in entities else None
        if 'retweeted_status' in status:
            original = status['retweeted_status']
            working = {}
            self._collect_attrs(working, original)
            for k in TweetData.retweet_keys:
                working[k] = original[k] if k in original else None
            self.tweet['original'] = working
        else:
            self.tweet['original'] = None
        self.timestamp = int(status['timestamp_ms'])/1000

    def _clean_text(self):
        """
        Canonicalize the text of the tweet into text.
        Algorithm:
        1. Lowercase the text.
        2. Replace certain patterns with uppercase symbols.
        3. Split into list of words (on whitespace).
        4. Strip the text (probably unnecessary).
        """

        if hasattr(self, 'text',
                   # Don't use these arguments.
                   url_re = re.compile(r"\bhttps?://[-a-z0-9/?#,.]+"),
                   astral_re = re.compile(r"[^\u0000-\u00ff]+"),
                   retweet_re = re.compile(r"\bRT\b:?"),
                   user_re = re.compile(r"@\w+\b"),
                   space_re = re.compile(r"\s+")):
            return
        s = self.tweet['text'].lower()
        s = url_re.sub(" URL ", s)
        s = astral_re.sub(" \0 ", s)
        s = user_re.sub(" @USER ", s)
        # #### This could mean "retweet", but it could be "round trip", too.
        # s = retweet_re.sub(" ", s)
        s = space_re.sub(" ", s)
        self.text = s.strip()

    def _canonicalize_text(self):
        """
        Produce a canonical word list in words.
        Algorithm:
        1. Remove stopwords and duplicates.
        2. Sort the list, rare words (in corpus) first.
        """

        self.words = list(set(self.text.split()) - TweetData.stopset)
        self.words.sort()
        
    def _collect_entity_text(self, status):
        if 'entities' in status:
            entities = status['entities']
            self.hash_text = " ".join(h['text']
                                      for h in entities['hashtags']) \
                             if 'hashtags' in entities else ""
            self.media_text = " ".join(m['expanded_url'] + " "
                                       + m['display_url']
                                       for m in entities['media']) \
                              if 'media' in entities else ""
            self.url_text = " ".join(u['expanded_url'] + " " + u['display_url']
                                     for u in entities['urls']) \
                            if 'urls' in entities else ""
            self.user_text = " ".join(u['screen_name']
                                  for u in entities['user_mentions']) \
                             if 'user_mentions' in entities else ""
            # Split the concatenation of texts on URL segment boundaries
            # as well as English word boundaries.
            words = " ".join([self.hash_text, self.media_text, self.url_text,
                              self.user_text]).lower()
            # Split, eliminate duplicates, and sort.
            words = [ word.strip() for word
                      in re.split(r"(\s|[-:/._?#;,']|http://)+", words)][::2]
            self.entity_words = sorted(set(word for word in words if word))
        else:
            self.entity_words = []

    def _collect_miscellaneous(self, status):
        # #### Refactor this for efficiency!!
        # #### Use this technique for retweet fields.
        def extract_location(key, tweet):
            keys = key.split(".")
            result = tweet
            for k in keys:
                result = result.get(k)
                if not result: break
            return result
        for k in TweetData.location_keys:
            result = extract_location(k, status)
            if result:
                location_count[k] += 1
                self.tweet[k] = result
        # #### Probably should collect retweet operations.
        if 'retweeted_status' in status:
            retweet = status['retweeted_status']
            for k in TweetData.location_keys:
                result = extract_location(k, retweet)
                if result:
                    location_count["retweet." + k] += 1
                    # #### Flatten retweet attributes.
                    self.tweet['original'][k] = result


