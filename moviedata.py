import datetime
import itertools
import pytz

nyzone = pytz.timezone('US/Eastern')

DATES_MOVIES = [ # "movie",
    [datetime.datetime(2015, 2, 20, 9, tzinfo=nyzone),
     # Released on Feb. 27
     "The DUFF",
     "Hot Tub Time Machine 2",
     "McFarland, USA",
     "Badlapur",
     "Digging Up the Marrow",
     "Queen and Country",
     "Wild Tales"],
    [datetime.datetime(2015, 2, 27, 9, tzinfo=nyzone),
     # Released on Feb. 27
     "Focus",                           # (2015)
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
    [datetime.datetime(2015, 3, 6, 9, tzinfo=nyzone),
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
    [datetime.datetime(2015, 3, 13, 9, tzinfo=nyzone),
     # Released on Mar. 13
     "Cinderella",                      # (2015)
     "Run All Night",
     "Eva",
     "Home Sweet Hell",
     "It Follows",
     "Seymour: An Introduction",
     "The Tales of Hoffmann",           # (2015 re-issue)
     "The Wrecking Crew"],
    [datetime.datetime(2015, 3, 20, 9, tzinfo=nyzone),
     # Released on Mar. 20
     "The Divergent Series: Insurgent",
     "Do You Believe?",
     "The Gunman",
     "Can't Stand Losing You: Surviving the Police",
     "Danny Collins",
     "Kumiko, The Treasure Hunter",     # (on Wed.)
     "Love and Lost",                   # (Shi Gu)
     "Shi Gu",
     "Spring",                          # (2015)
     "Zombeavers"],
    [datetime.datetime(2015, 3, 27, 9, tzinfo=nyzone),
     # Released on Mar. 27
     "Get Hard",
     "Home",                            # (2015)
     "Cupcakes",
     "A Girl Like Her",
     "Man From Reno",
     "The Riot Club",
     "The Salt of the Earth",
     "Serena",
     "Welcome to New York",
     "While We're Young",
     "White God"],
    [datetime.datetime(2015, 4, 3, 9, tzinfo=nyzone),
     # Released on Apr. 3
     "Furious 7",
     "5 to 7",
     "Cheatin'",
     "Effie Gray",
     "Lambert & Stamp",
     "The Living",
     "SBK The-Movie",
     "Woman in Gold"],
    [datetime.datetime(2015, 4, 10, 9, tzinfo=nyzone),
     # Released on Apr. 10
     "The Longest Ride",
     "Clouds of Sils Maria",
     "Desert Dancer",
     "Ex Machina",
     "Freetown",
     "Kill Me Three Times",
     "Rebels of the Neon God",          # (2015 re-release) 
     "The Sisterhood of Night"],
    [datetime.datetime(2015, 4, 17, 9, tzinfo=nyzone),
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
    [datetime.datetime(2015, 4, 24, 9, tzinfo=nyzone),
     # Released on April 24th (Friday) 
     "The Age of Adaline",
     "Little Boy",
     "Adult Beginners",
     "After The Ball",
     "Brotherly Love",
     "Kung Fu Killer",
     "Misery Loves Comedy",
     # "WARx2",
     "The Water Diviner"],
    [datetime.datetime(2015, 5, 1, 9, tzinfo=nyzone),
     # Released on May 1st Friday
     "Avengers: Age of Ultron",
     # The 100-Year Old Man Who Climbed Out the Window and Disappeared
     "100-Year Old Man Who Climbed Out Window Disappeared",
     "Far from Men",
     "Far from the Madding Crowd",
     "Hyena",
     "Iris",                            # (2015)
     "Ride",                            # (2015)
     "Tangerines",
     "Welcome to Me"],
    [datetime.datetime(2015, 5, 8, 9, tzinfo=nyzone),
     # Released on May 8th (Friday)
     "Hot Pursuit",
     "5 Flights Up",
     "The D Train",
     "I Am Big Bird",                   # (on Wed.)
     "In the Name of my Daughter",
     "Maggie",
     "Noble",
     "Saint Laurent",
     "The Seven Five",                  # (on Thu.)
     "Skin Trade",
     # "WARx2",                         # Not released on 4/24?
     "Sister Code"],
    [datetime.datetime(2015, 5, 15, 9, tzinfo=nyzone),
     # Released on May 15th (Friday)
     "Mad Max: Fury Road",
     "Pitch Perfect 2",
     "Animals",                         # (2015)
     "The Connection",
     "Every Secret Thing",
     "The Film Critic",
     "Good Kill",
     "I'll See You In My Dreams",
     "Set Fire to the Stars",
     "Where Hope Grows",
     "Forbidden Films",                 # (on Wed.)
     "Know How",
     "One Cut, One Life",               # (on Wed.)
     "Our Man in Tehran"],
    [datetime.datetime(2015, 5, 22, 9, tzinfo=nyzone),
     # Released on May 22th (Friday)
     "Poltergeist",                     # (2015)
     "Tomorrowland",
     "Aloft",
     "Chocolate City",
     "The Farewell Party",
     "Love At First Sight",
     "Sunshine Superman",
     "When Marnie Was There"],
    [datetime.datetime(2015, 5, 29, 9, tzinfo=nyzone),
     # Released on May 29th (Friday)
     "Aloha",
     "San Andreas",
     "Club Life",
     "Gemma Bovary",
     "Heaven Knows What",
     "Results"],
    # *Released on June 5th, Friday: *
    [datetime.datetime(2015, 6, 5, 9, tzinfo=nyzone),
     "Entourage",
     "Insidious Chapter 3",
     "Spy",
     "Every Last Child",
     "How to Save Us",
     "Hungry Hearts",
     "Love & Mercy",
     "Pigeon",
     "Police Story: Lockdown",
     "Testament of Youth"],
    # *Released on June 12 Friday: *
    [datetime.datetime(2015, 6, 12, 9, tzinfo=nyzone),
     "Jurassic World",
     "11th Hour",
     "Me and Earl and the Dying Girl",
     #"Set Fire to the Stars",
     "Soaked in Bleach",
     #"WARx2",
     "The Wolfpack"],
    #*Released on June 19 Friday: *
    [datetime.datetime(2015, 6, 19, 9, tzinfo=nyzone),
     "Dope",
     "Inside Out",
     "Dark Awakening",
     "Eden",
     "Gabriel",
     "Infinitely Polar Bear",
     "Manglehorn",
     "The Overnight",
     "The Tribe"],
    # *Released on June 26 Friday: *
    [datetime.datetime(2015, 6, 26, 9, tzinfo=nyzone),
     "Max",                             # (2015)
     "Ted 2",
     "7 Minutes",
     "The Algerian",
     "Batkid Begins",
     "Big Game",
     "Felt",
     "The Little Death",
     "Murder in the Park",
     "The Pardon",
     "Runoff"],
    [datetime.datetime(2015, 7, 3, 9, tzinfo=nyzone),
    # *Released on July 3rd, Friday: *
    "Magic Mike XXL",                   # (on Wed.)
    "Terminator: Genisys",              # (on Wed.)
    "Amy",
    "Cartel Land",
    "Jackie & Ryan",
    "Jimmy's Hall",
    "Mala Mala"],                        # (on Wed.)
    [datetime.datetime(2015, 7, 10, 9, tzinfo=nyzone),
    # *Released on July 10th, Friday: *",
    "The Gallows",
    "Minions",
    "Self/Less",
    "Do I Sound Gay?",
    "Nowitzki",
    "The Suicide Theory",
    "Tangerine"],
    [datetime.datetime(2015, 7, 17, 9, tzinfo=nyzone),
    # *Released on July 17th, Friday: *
    "Ant-Man",
    "Trainwreck",
    "Bonobos: Back to the Wild",
    "Catch Me Daddy",
    "Irrational Man",
    "The Look of Silence",
    "Mr. Holmes",
    "The Stanford Prison Experiment"],
    [datetime.datetime(2015, 7, 24, 9, tzinfo=nyzone),
    # *Released on July 24th, Friday: *
    "Paper Towns",
    "Pixels",
    "Southpaw",
    "Big Significant Things",
    "Phoenix",                          # (2015)
    "Unexpected",
    "The Vatican Tapes",
    #"WARx2"
     ],
    [datetime.datetime(2015, 7, 31, 9, tzinfo=nyzone),
    # *Released on July 31rd, Friday: *
    "Mission: Impossible - Rogue Nation",
    "Vacation",                         # (on Wed.)
    "Best of Enemies",
    "The End of the Tour",
    "A LEGO Brickumentary",
    "Listen to Me Marlon",              # (on Wed.)
    "The Young & Prodigious T S Spivet",  # change periods to space
     ],
    [datetime.datetime(2015, 8, 7, 9, tzinfo=nyzone),
    # August 7th (Friday)
    "Fantastic Four",
    "The Gift",                         # (2015)
    "Ricki and the Flash",
    "Shaun the Sheep",                  # deleted "movie"
    "Cop Car",
    "Dark Places",
    "The Diary of a Teenage Girl",
    "Dragon Ball Z: Resurrection of 'F'",  # (on Tue.)
    "The Falling",
    "Kahlil Gibran", "The Prophet",     # split into two phrases
    "Metropolitan",],                   # (2015 re-release)
    [datetime.datetime(2015, 8, 14, 9, tzinfo=nyzone),
    # August 14th (Friday)
    "The Man From U.N.C.L.E.",
    "Straight Outta Compton",
    "Underdogs",                        # (2014)
    "Mistress America",
    "WARx2",
    "We Come As Friends",],
    [datetime.datetime(2015, 8, 21, 9, tzinfo=nyzone),
    # August 21rd (Friday)
    "American Ultra",
    "Hitman: Agent 47",
    "6 Years",                          # (on Tue.)
    "Digging for Fire",
    "Grandma",                          # (2015)
    "Learning to Drive",
    "She's Funny That Way",],
    [datetime.datetime(2015, 8, 28, 9, tzinfo=nyzone),
    # August 28rd (Friday)
    "Regression",
    "Sinister 2",
    "We Are Your Friends",
    "The Second Mother",
    "War Room",
    "Z for Zachariah",],
    [datetime.datetime(2015, 9, 4, 9, tzinfo=nyzone),
    # Released on Sep. 4th, Friday: 
    "The Transporter Refueled",
    "A Walk in the Woods",              # (on Wed.)
    "Before We Go",
    "Bloodsucking Bastards",
    "Number One Fan",
    "Rififi",                           # (2015 re-release) (on Wed.)
    "Un Gallo con Muchos Huevos",],
    [datetime.datetime(2015, 9, 11, 9, tzinfo=nyzone),
    # Released on Sep. 11st, Friday:
    "90 Minutes in Heaven",
    "Perfect Guy",
    "The Visit",
    "Breathe",
    "Coming Home",                      # (2015) (on Wed.)
    "Listening",
    "Paul Taylor: Creative Domain",
    "Sleeping with Other People",
    "Time Out of Mind",
    "Wolf Totem",],
    [datetime.datetime(2015, 9, 18, 9, tzinfo=nyzone),
    # Released on Sep. 18th, Friday:
    "Black Mass",
    "Captive",                          # (2015)
    "Maze Runner: The Scorch Trials",
    "About Ray",
    "Everest",                          # (2015)
    "Katti Batti",
    "Pawn Sacrifice",                   # (on Wed.)
    "Prophet's Prey",
    "Sicario",
    "War Pigs",],
    [datetime.datetime(2015, 9, 25, 9, tzinfo=nyzone),
    #Released on Sep. 25th, Friday:
    "Before I Wake",
    "The Green Inferno",
    "Hotel Transylvania 2",
    "The Intern",
    "10 Days in a Madhouse",
    "99 Homes",
    "A Brave Heart: The Lizzie Velasquez Story",
    "The Keeping Room",
    "Labyrinth of Lies",
    "Mississippi Grind",
    "The Reflektor Tapes",              # (on Thu.)
    "Stonewall",],                      # (2015)
]

STOPSET = {"a", "an", "the", "some", "to", "from", "for", "with"}

PUNCT = { ord(',') : None,
          ord('?') : None,
          ord(':') : None,
          ord(';') : None,
          ord('.') : None,
          #ord('-') : None,
          }

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

