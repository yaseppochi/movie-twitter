import datetime
import moviedata

WEEKMILLISECONDS = 1000 * int(datetime.timedelta(7).total_seconds())

class MovieException(Exception):
    pass

class DuplicateNameError(MovieException):
    pass

class ReinitializationError(MovieException):
    pass

class Movie(object):
    """
    Collect names, opening dates, and stars of movies.

    Fields that are None are uninitialized.  name is required (may not be
    None), others may be optional or lazily initialized.
    """

    by_name = {}
    word_movies = {}

    # #### The optional arguments arguably are useless.
    def __init__(self, name, opening_date=None, star_list=None):
        self.name = name
        self.opening_date = opening_date
        self.star_list = star_list
        if name in Movie.by_name:
             raise DuplicateNameError(name)
        Movie.by_name[name] = self
        self.words = [w for w
                      in self.name.translate(moviedata.PUNCT).lower().split()
                      if w not in moviedata.STOPSET]
        assert(self.words)
        self.file_stem = "-".join(self.words)
        self._compute_word_movie_map()

    @classmethod
    def _get_by_name(cls, name):
        m = Movie.by_name.get(name)
        if m is None:
            m = Movie(name)
        return m

    def _add_opening_date(self, date):
        if isinstance(date, str):
            # #### Need to handle setting time and timezone.
            raise NotImplementedError(opening_date)
        if self.opening_date is not None:
            raise ReinitializationError(self.name, 'date', date)
        self.opening_date = date

    def _add_star_list(self, stars):
        if self.star_list is not None:
            raise ReinitializationError(self, 'stars', stars)
        self.star_list = stars

    def _compute_timestamp_list(self):
        self.week_bounds = []
        origin = 1000 * int(self.opening_date.timestamp())
        for i in range(-1, 11):
            self.week_bounds.append(origin + i * WEEKMILLISECONDS)

    def __str__(self):
        if self.star_list:
            s = ",".join(" " + star for star in self.star_list)
        elif self.star_list is None:
            s = " N/A"
        else:
            s = " no stars"
        return "{0} Opened: {1} Stars:{2}".format(self.name,
                                                  self.opening_date,
                                                  s)

    # Compute word->movie map.
    # Note that string "in" operator is case-sensitive, so we lowercase all words.
    # Movie names are not searched for directly so they don't need lowercasing.
    def _compute_word_movie_map(self):
        for w in self.words:
            # #### Use DefaultDict.
            wm = Movie.word_movies
            if w in wm:
                wm[w].append(self)
            else:
                wm[w] = [self]

    # #### Obsolete with the advent of week_bounds.
    def timestamp_to_week(self, time, _week=datetime.timedelta(7)):
        return (datetime.datetime.fromtimestamp(time, moviedata.nytime)
                - self.opening_date) // _week


def populate_movie_list(dates, stars):
    stems = {}
    for elt in dates:
        date = elt[0]
        for name in elt[1:]:
            m = Movie._get_by_name(name)
            m._add_opening_date(date)
            m._compute_timestamp_list()
            assert(m.file_stem not in stems)
            stems.add(m.file_stem)
    for elt in stars:
        m = Movie._get_by_name(elt[0])
        m._add_star_list(elt[1:])

def track_join(ks):
    return ','.join(k.translate(moviedata.PUNCT) for k in ks)

populate_movie_list(moviedata.DATES_MOVIES,
                    moviedata.MOVIES_STARS)


if __name__ == "__main__":
    for m in Movie.by_name.values():
        print(m.name, m.words, m.opening_date)
        print(m.week_bounds[:4])
