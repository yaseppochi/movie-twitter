MOVIES = [ # "movie",
    # Released on Feb. 2
    "The DUFF",
    "Hot Tub Time Machine 2",
    "McFarland, USA",
    "Badlapur",
    "Digging Up the Marrow",
    "Queen and Country",
    "Wild Tales",
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
    "Wild Canaries",
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
    "These Final Hours",
    # Released on Mar. 13
    "Cinderella", # (2015)
    "Run All Night",
    "Eva",
    "Home Sweet Hell",
    "It Follows",
    "Seymour: An Introduction",
    "The Tales of Hoffmann", # (2015 re-issue)
    "The Wrecking Crew",
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
    "Zombeavers",
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
    "White God",
    # Released on Apr. 3
    "Furious 7",
    "5 to 7",
    "Cheatin'",
    "Effie Gray",
    "Lambert & Stamp",
    "The Living",
    "SBK The-Movie",
    "Woman in Gold",
    # Released on Apr. 10
    "The Longest Ride",
    "Clouds of Sils Maria",
    "Desert Dancer",
    "Ex Machina",
    "Freetown",
    "Kill Me Three Times",
    "Rebels of the Neon God",           # (2015 re-release) 
    "The Sisterhood of Night",
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
    "True Story",
    # Released on April 24th (Friday) 
    "The Age of Adaline",
    "Little Boy",
    "Adult Beginners",
    "After The Ball",
    "Brotherly Love",
    "Kung Fu Killer",
    "Misery Loves Comedy",
    "WARx2",
    "The Water Diviner",
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
    "Welcome to Me",
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
    "Sister Code",
    # "WARx2",                          # Not released on 4/24?
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
    "Our Man in Tehran",
    # Released on May 22th (Friday)
    "Poltergeist",                      # (2015)
    "Tomorrowland",
    "Aloft",
    "Chocolate City",
    "The Farewell Party",
    "Love At First Sight",
    "Sunshine Superman",
    "When Marnie Was There",
    # Released on May 29th (Friday)
    "Aloha",
    "San Andreas",
    "Club Life",
    "Gemma Bovary",
    "Heaven Knows What",
    "Results",
    ]

STOPLIST = ["a", "an", "the", "some", "to", "from", "for", "with"]

PUNCT = { ord(',') : None,
          ord('?') : None,
          ord(':') : None,
          ord(';') : None,
          }

def track_join(ks):
    return ','.join(k.translate(PUNCT) for k in ks)
