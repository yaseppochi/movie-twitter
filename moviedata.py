import itertools
import datetime
import pytz

nytime = pytz.timezone('US/Eastern')

DATES_MOVIES = [ # "movie",
    [datetime.datetime(2015, 2, 20, 9, tzinfo=nytime),
     # Released on Feb. 27
     "The DUFF",
     "Hot Tub Time Machine 2",
     "McFarland, USA",
     "Badlapur",
     "Digging Up the Marrow",
     "Queen and Country",
     "Wild Tales"],
    [datetime.datetime(2015, 2, 27, 9, tzinfo=nytime),
     # Released on Feb. 27
     "Focus",                            #  (2015)
     "The Lazarus Effect",
     "'71",
     "Deli Man",
     "Eastern Boys",
     "Everly",
     "Farewell to Hollywood",
     "Futuro Beach",
     "The Hunting Ground",
     "A La Mala",
     "Maps to the Stars",
     "My Life Directed",
     "The Salvation",
     "Snow Girl and the Dark Crystal",
     "Wild Canaries"],
    [datetime.datetime(2015, 3, 6, 9, tzinfo=nytime),
     # Released on Mar. 6
     "Chappie",
     "Unfinished Business",
     "Bad Asses on the Bayou",
     "Compared to What? The Improbable Journey of Barney Frank",
     "Hayride 2",
     "The Life and Mind of Mark DeFriest",
     "The Mafia Only Kills in Summer",
     "Merchants of Doubt",
     "The Second Best Exotic Marigold Hotel",
     "These Final Hours"],
    [datetime.datetime(2015, 3, 13, 9, tzinfo=nytime),
     # Released on Mar. 13
     "Cinderella", # (2015)
     "Run All Night",
     "Eva",
     "Home Sweet Hell",
     "It Follows",
     "Seymour: An Introduction",
     "The Tales of Hoffmann", # (2015 re-issue)
     "The Wrecking Crew"],
    [datetime.datetime(2015, 3, 20, 9, tzinfo=nytime),
     # Released on Mar. 20
     "The Divergent Series: Insurgent",
     "Do You Believe?",
     "The Gunman",
     "Can't Stand Losing You: Surviving the Police",
     "Danny Collins",
     "Kumiko, The Treasure Hunter",      # (on Wed.)
     "Love and Lost",                    # (Shi Gu)
     "Shi Gu",
     "Spring",                           # (2015)
     "Zombeavers"],
    [datetime.datetime(2015, 3, 27, 9, tzinfo=nytime),
     # Released on Mar. 27
     "Get Hard",
     "Home",                             # (2015)
     "Cupcakes",
     "A Girl Like Her",
     "Man From Reno",
     "The Riot Club",
     "The Salt of the Earth",
     "Serena",
     "Welcome to New York",
     "While We're Young",
     "White God"],
    [datetime.datetime(2015, 4, 3, 9, tzinfo=nytime),
     # Released on Apr. 3
     "Furious 7",
     "5 to 7",
     "Cheatin'",
     "Effie Gray",
     "Lambert & Stamp",
     "The Living",
     "SBK The-Movie",
     "Woman in Gold"],
    [datetime.datetime(2015, 4, 10, 9, tzinfo=nytime),
     # Released on Apr. 10
     "The Longest Ride",
     "Clouds of Sils Maria",
     "Desert Dancer",
     "Ex Machina",
     "Freetown",
     "Kill Me Three Times",
     "Rebels of the Neon God",           # (2015 re-release) 
     "The Sisterhood of Night"],
    [datetime.datetime(2015, 4, 17, 9, tzinfo=nytime),
     # Released on April 17th (Friday)
     "Child 44",
     "Monkey Kingdom",
     "Paul Blart: Mall Cop 2",
     "Unfriended",
     "Alex of Venice",
     "Beyond the Reach",
     "The Dead Lands",
     "Felix and Meira",
     "Monsters: Dark Continent",
     "The Road Within",
     "True Story"],
    [datetime.datetime(2015, 4, 24, 9, tzinfo=nytime),
     # Released on April 24th (Friday) 
     "The Age of Adaline",
     "Little Boy",
     "Adult Beginners",
     "After The Ball",
     "Brotherly Love",
     "Kung Fu Killer",
     "Misery Loves Comedy",
     "WARx2",
     "The Water Diviner"],
    [datetime.datetime(2015, 5, 1, 9, tzinfo=nytime),
     # Released on May 1st Friday
     "Avengers: Age of Ultron",
     # The 100-Year Old Man Who Climbed Out the Window and Disappeared
     "100-Year Old Man Who Climbed Out Window Disappeared",
     "Far from Men",
     "Far from the Madding Crowd",
     "Hyena",
     "Iris",                             # (2015)
     "Ride",                             # (2015)
     "Tangerines",
     "Welcome to Me"],
    [datetime.datetime(2015, 5, 8, 9, tzinfo=nytime),
     # Released on May 8th (Friday)
     "Hot Pursuit",
     "5 Flights Up",
     "The D Train",
     "I Am Big Bird",                    # (on Wed.)
     "In the Name of my Daughter",
     "Maggie",
     "Noble",
     "Saint Laurent",
     "The Seven Five",                   # (on Thu.)
     "Skin Trade",
     # "WARx2",                          # Not released on 4/24?
     "Sister Code"],
    [datetime.datetime(2015, 5, 15, 9, tzinfo=nytime),
     # Released on May 15th (Friday)
     "Mad Max: Fury Road",
     "Pitch Perfect 2",
     "Animals",                          # (2015)
     "The Connection",
     "Every Secret Thing",
     "The Film Critic",
     "Good Kill",
     "I'll See You In My Dreams",
     "Set Fire to the Stars",
     "Where Hope Grows",
     "Forbidden Films",                  # (on Wed.)
     "Know How",
     "One Cut, One Life",                # (on Wed.)
     "Our Man in Tehran"],
    [datetime.datetime(2015, 5, 22, 9, tzinfo=nytime),
     # Released on May 22th (Friday)
     "Poltergeist",                      # (2015)
     "Tomorrowland",
     "Aloft",
     "Chocolate City",
     "The Farewell Party",
     "Love At First Sight",
     "Sunshine Superman",
     "When Marnie Was There"],
    [datetime.datetime(2015, 5, 29, 9, tzinfo=nytime),
     # Released on May 29th (Friday)
     "Aloha",
     "San Andreas",
     "Club Life",
     "Gemma Bovary",
     "Heaven Knows What",
     "Results"],
    ]

STOPLIST = ["a", "an", "the", "some", "to", "from", "for", "with"]

PUNCT = { ord(',') : None,
          ord('?') : None,
          ord(':') : None,
          ord(';') : None,
          }

def track_join(ks):
    return ','.join(k.translate(PUNCT) for k in ks)

MOVIES_STARS = [
    ["The DUFF", "Mae Whitman", "Bella Thorne", "Robbie Amell"],
    ["Hot Tub Time Machine 2", "Rob Corddry", "Craig Robinson", "Clark Duke"],
    ["McFarland, USA", "Kevin Costner", "Maria Bello", "Ramiro Rodriguez"],
    ["Badlapur", "Nawazuddin Siddiqui", "Varun Dhawan", "Radhika Apte"],
    ["Queen and Country", "Callum Turner", "Caleb Jones", "David Thewlis,"],
    ["Wild Tales", "Ricardo Dar\u00EDn", "Leonardo Sbaraglia"],
    ["Focus", "Will Smith", "Margot Robbie", "Rodrigo Santoro"],
    ["The Lazarus Effect", "Olivia Wilde", "Mark Duplass", "Evan Peters"],
    ["'71", "Jack O'Connell", "Sam Reid", "Sean Harris"],
    ["Deli Man", "Ziggy Gruber", "Larry King", "Jerry Stiller"],
    ["Eastern Boys", "Olivier Rabourdin", "Kirill Emelyanov", "Daniil Vorobyov"],
    ["Farewell to Hollywood", "Henry Corra", "Regina Nicholson"],
    ["Futuro Beach", "Wagner Moura", "Clemens Schick", "Jesu\u00EDta Barbosa"],
    ["The Hunting Ground", "Kirby Dick", "Amy Ziering", "Amy Herdy"],
    ["A La Mala", "Aislinn Derbez", "Mauricio Ochmann", "Papile Aurora"],
    ["Maps to the Stars", "Julianne Moore", "Mia Wasikowska", "Robert Pattinson"],
    ["The Salvation", "Mads Mikkelsen", "Eva Green", "Eric Cantona"],
    ["Snow Girl and the Dark Crystal", "Bei-Er Bao", "Winston Chao", "Kun Chen"],
    ["Wild Canaries", "Sophia Takal", "Lawrence Michael Levine", "Alia Shawkat"],
    ["Chappie", "Sharlto Copley", "Dev Patel", "Hugh Jackman"],
    ["Unfinished Business", "Vince Vaughn", "Dave Franco", "Tom Wilkinson"],
    ["The Mafia Only Kills in Summer", "Cristiana Capotondi", "Pif", "Alex Bisconti"],
    ["Merchants of Doubt", "Frederick Singer", "Naomi Oreskes", "Jamy Ian Swiss"],
    ["The Second Best Exotic Marigold Hotel", "Judi Dench", "Maggie Smith", "Bill Nighy"],
    ["Cinderella", "Lily James", "Cate Blanchett", "Richard Madden"],
    ["Run All Night", "Liam Neeson", "Ed Harris", "Joel Kinnaman"],
    ["It Follows", "Maika Monroe", "Keir Gilchrist", "Olivia Luccardi"],
    ["Seymour: An Introduction", "Seymour Bernstein", "Jiyang Chen", "Ethan Hawke"],
    ["The Tales of Hoffmann", "Moira Shearer", "Robert Rounseville", "Ludmilla Tch\u00E9rina"],
    ["The Wrecking Crew", "Lou Adler", "Herb Alpert", "The Association"],
    ["The Divergent Series: Insurgent", "Patrick Cronen", "Mekhi Phifer", "Miles Teller"],
    ["Do You Believe?", "Mira Sorvino", "Sean Astin", "Alexa PenaVega"],
    ["The Gunman", "Sean Penn", "Idris Elba", "Jasmine Trinca"],
    ["Can't Stand Losing You: Surviving the Police", "Sting", "Andy Summers", "Stewart Copeland"],
    ["Danny Collins", "Al Pacino", "Annette Bening", "Jennifer Garner"],
    ["Kumiko, The Treasure Hunter", "Rinko Kikuchi", "Nobuyuki Katsube", "Shirley Venard"],
    ["Love and Lost", "Andy Lau", "Boran Jing", "Tony Leung Ka-Fai"],
    ["Shi Gu", "Andy Lau", "Boran Jing", "Tony Leung Ka-Fai"],
    ["Spring", "Lou Taylor Pucci", "Nadia Hilker", "Vanessa Bednar"],
    ["Get Hard", "Will Ferrell", "Kevin Hart", "Alison Brie"],
    ["Home", "Jim Parsons", "Rihanna", "Steve Martin"],
    ["Cupcakes", "Dana Ivgy", "Keren Berger", "Yael Bar-Zohar"],
    ["Man From Reno", "Ayako Fujitani", "Pepe Serna", "Kazuki Kitamura"],
    ["The Riot Club", "Sam Claflin", "Max Irons", "Douglas Booth"],
    ["The Salt of the Earth", "Sebasti\u00E3o Salgado", "Wim Wenders", "Juliano Ribeiro Salgado"],
    ["Serena", "Bradley Cooper", "Jennifer Lawrence", "Rhys Ifans"],
    ["While We're Young", "Ben Stiller", "Naomi Watts", "Adam Driver"],
    ["White God", "Zs\u00F3fia Psotta", "S\u00E1ndor Zs\u00F3t\u00E9r", "Lili Horv\u00E1th"],
    ["Furious 7", "Vin Diesel", "Paul Walker", "Dwayne Johnson"],
    ["5 to 7", "Anton Yelchin", "B\u00E9r\u00E9nice Marlohe", "Olivia Thirlby"],
    ["Cheatin'"],
    ["The Living", "Fran Kranz", "Jocelin Donahue", "Kenny Wormald"],
    ["Woman in Gold", "Helen Mirren", "Ryan Reynolds", "Daniel Br\u00FChl"],
    ["The Longest Ride", "Scott Eastwood", "Britt Robertson", "Alan Alda"],
    ["Clouds of Sils Maria", "Juliette Binoche", "Kristen Stewart", "Chlo\u00EB Grace Moretz"],
    ["Desert Dancer", "Nazanin Boniadi", "Freida Pinto", "Tom Cullen"],
    ["Ex Machina", "Alicia Vikander", "Domhnall Gleeson", "Oscar Isaac"],
    ["Freetown", "Henry Adofo", "Michael Attram", "Alphonse Menyo"],
    ["Kill Me Three Times", "Simon Pegg", "Teresa Palmer", "Alice Braga"],
    ["Child 44", "Tom Hardy", "Gary Oldman", "Noomi Rapace"],
    ["Monkey Kingdom", "Tina Fey"],
    ["The Dead Lands", "James Rolleston", "Lawrence Makoare", "Te Kohe Tuhaka"],
    ["Paul Blart: Mall Cop 2", "Kevin James", "Raini Rodriguez", "Eduardo Ver\u00E1stegui"],
    ["Adult Beginners", "Nick Kroll", "Rose Byrne", "Bobby Cannavale"],
    ["The Age of Adaline", "Blake Lively", "Michiel Huisman", "Harrison Ford"],
    ["Kung Fu Killer", "Donnie Yen", "Charlie Yeung", "Baoqiang Wang"],
    ["Misery Loves Comedy", "Amy Schumer", "James L. Brooks", "Judd Apatow"],
    ["Avengers: Age of Ultron", "Robert Downey Jr.", "Chris Evans", "Mark Ruffalo"],
    ["Far from the Madding Crowd", "Carey Mulligan", "Matthias Schoenaerts", "Michael Sheen"],
    ]

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
        self._compute_word_movie_maps()

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
            raise ReinitializationError(self, 'date', date)
        self.opening_date = date

    def _add_star_list(self, stars):
        if self.star_list is not None:
            raise ReinitializationError(self, 'stars', stars)
        self.star_list = stars

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

    # Compute word->movie and movie->word maps.
    # Note that string "in" operator is case-sensitive, so we lowercase all words.
    # Movie names are not searched for directly so they don't need lowercasing.
    def _compute_word_movie_maps(self):
        self.words = [w.lower() for w in self.name.translate(PUNCT).split()]
        for w in self.words:
            # #### Use DefaultDict.
            wm = Movie.word_movies
            if w in wm:
                wm[w].append(self)
            else:
                wm[w] = [self]


def populate_movie_list(dates, stars):
    for elt in dates:
        date = elt[0]
        for name in elt[1:]:
            m = Movie._get_by_name(name)
            m._add_opening_date(date)
    for elt in stars:
        m = Movie._get_by_name(elt[0])
        m._add_star_list(elt[1:])

populate_movie_list(DATES_MOVIES, MOVIES_STARS)


if __name__ == "__main__":
    for m in Movie.by_name.values():
        print(m)
