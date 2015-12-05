import csv
import os.path
import sqlite3 as sql
from twitter_csv_to_sql import row_csv_to_sql

DEBUG = 0                               # 1 = print current development
                                        # 2 = print tested results
                                        # 3 = print command strings

movie_table_create_command = "create table movies ( " \
    "ReleaseMonth date, " \
    "Items integer, " \
    "Name text, " \
    "TotalGross integer, " \
    "Sum4 integer, " \
    "Sum7 integer, " \
    "Budget integer, " \
    "ScreensWeek1 integer, " \
    "MarketShare real, " \
    "Genre integer, " \
    "GenreCheck integer, " \
    "Sources integer, " \
    "ScheduledRelease date, " \
    "InSample " \
    ")"

genre_table_create_command = "create table genres ( " \
    "id integer, " \
    "genre text" \
    ")"

week_table_create_command = "create table weeks ( " \
    "movie text, " \
    "week integer, " \
    "revenue integer, " \
    "sentiment real, " \
    "volume real" \
    ")"

csv_fields = [
    ("ReleaseMonth", "movie"),          # [0:6]
    ("Items", "movie"),
    ("Name", "movie"),
    ("TotalGross", "movie"),
    ("Sum4", "movie"),
    ("Sum7", "movie"),
    ("Week0", "week", "revenue"),       # [6:14]
    ("Week1", "week", "revenue"),
    ("Week2", "week", "revenue"),
    ("Week3", "week", "revenue"),
    ("Week4", "week", "revenue"),
    ("Week5", "week", "revenue"),
    ("Week6", "week", "revenue"),
    ("Week7", "week", "revenue"),
    ("Budget", "movie"),                # [14:17]
    ("ScreensWeek1", "movie"),
    ("MarketShare", "movie"),
    ("HorrorThriller", "genus"),        # [17:26]
    ("Drama", "genus"),
    ("Sci-Fi", "genus"),
    ("AnimationComputer", "genus"),
    ("Documentary", "genus"),
    ("Action", "genus"),
    ("Romance", "genus"),
    ("Comedy", "genus"),
    ("Foreign", "genus"),
    ("GenreCheck", "movie"),            # [26:27]
    ("TSentiScoreW0", "week", "sentiment"),  # [27:35]
    ("TSentiScoreW1", "week", "sentiment"),
    ("TSentiScoreW2", "week", "sentiment"),
    ("TSentiScoreW3", "week", "sentiment"),
    ("TSentiScoreW4", "week", "sentiment"),
    ("TSentiScoreW5", "week", "sentiment"),
    ("TSentiScoreW6", "week", "sentiment"),
    ("TSentiScoreW7", "week", "sentiment"),
    ("TVolumeW0", "week", "volume"),    # [35:43]
    ("TVolumeW1", "week", "volume"),
    ("TVolumeW2", "week", "volume"),
    ("TVolumeW3", "week", "volume"),
    ("TVolumeW4", "week", "volume"),
    ("TVolumeW5", "week", "volume"),
    ("TVolumeW6", "week", "volume"),
    ("TVolumeW7", "week", "volume"),
    ]

movie_aliases = [
    ("'71", "71"),
    ("Can't Stand Losing You: Surviving the Police",
     "Can't Stand Losing You Surviving the Police"),
    ('Do You Believe?', 'Do You Believe'),
    ('Grandma', 'Grandma (2015)'),
    ("Kahlil Gibran's The Prophet", 'Kahlil Gibran'),
    ('Kumiko, The Treasure Hunter', 'Kumiko The Treasure Hunter'),
    ('Love and Lost (Shi Gu)', 'Love and Lost', 'Shi Gu'),
    ('Max', 'Max (2015)'),
    ('McFarland, USA', 'McFarland USA'),
    ('Mission: Impossible - Rogue Nation', 'Mission Impossible Rogue Nation'),
    ('Paul Blart: Mall Cop 2', 'Paul Blart Mall Cop 2'),
    ('Phoenix', 'Phoenix (2015)'),
    ('Poltergeist', 'Poltergeist (2015)'),
    ('Rififi', 'Rififi (2015 re-release)'),
    ('Seymour: An Introduction', 'Seymour An Introduction'),
    ('Shaun the Sheep Movie', 'Shaun the Sheep'),
    ('Tangerines', 'Tangerine'),
    ('The Divergent Series: Insurgent', 'The Divergent Series Insurgent'),
    ('The Gift', 'The Gift (2015)'),
    ('The Young & Prodigious T.S. Spivet', 'The Young & Prodigious T S Spivet'),
    ]

canonical_name = {n[1]: n[0] for n in movie_aliases}
canonical_name['Shi Gu'] = 'Love and Lost (Shi Gu)'  # multiple aliases

def create_tables():
    db = sql.connect("twitter.sql")
    c = db.cursor()
    c.execute(movie_table_create_command)
    c.execute(genre_table_create_command)
    c.execute(week_table_create_command)
    c.close()
    db.commit()
    db.close()


def populate_tables_from_csv():
    db = sql.connect("twitter.sql")
    c = db.cursor()
    # populate genres
    bit = 0
    for f in csv_fields:
        if f[1] == "genus":
            c.execute("insert into genres values (?,?)", (bit, f[0]))
            if DEBUG > 1:
                print("Inserting genre", f[0], "at bit", bit)
            bit += 1
    # populate movies and weeks
    def calculate_genre(row):
        "Genre is a set represented as a bitvector, computed from dummies."
        bit = 1
        result = 0
        for g in row[17:26]:
            if g:
                result += bit
            bit <<= 1
        return [result]
    r = csv.reader(open("Database for thesis.csv"))
    next(r)                             # skip header row
    for row in r:
        row = row_csv_to_sql(row)
        c.execute("insert into movies values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  row[0:2] + [canonical_name.get(row[2], row[2])] + row[3:6]
                  + row[14:17] + calculate_genre(row) + row[26:27]
                  + [0, None, True])    # 0 = From anna's CSV
        for w in range(8):
            c.execute("insert into weeks values (?,?,?,?,?)",
                      (row[2], w, row[6 + w], row[27 + w], row[35 + w]))
        if DEBUG > 1:
            print("inserting record for", row[2])
    c.close()
    db.commit()
    db.close()


def populate_tables_from_moviedata():
    # retrieve list of names, if present, update
    db = sql.connect("twitter.sql")
    c = db.cursor()
    namelist = [canonical_name.get(x[0], x[0])
                for x in c.execute("select Name from movies")]
    if DEBUG > 1:
        print(namelist)
    # populate movies
    import moviedata as mdb
    mdb_movies = []
    for releasedate in mdb.DATES_MOVIES:
        rd = releasedate[0]
        for m in releasedate[1:]:
            mdb_movies.append((canonical_name.get(m, m), rd))
    if DEBUG > 1:
        print(mdb_movies)
    itemno = len(namelist)
    if DEBUG > 1:
        print("last existing itemno is", itemno, "... ", end='')
    for movie, date in mdb_movies:
        if movie in namelist:
            # update existing record
            c.execute("update movies set " \
                      "Sources=?," \
                      "ScheduledRelease=?," \
                      "InSample=? " \
                      "where Name=?",
                      (2, date, True, movie))
        else:
            # create new record
            itemno += 1
            c.execute("insert into " \
                      "movies (Name,Items,Sources,ScheduledRelease,InSample) " \
                      "values (?,?,?,?,?)",
                      (movie, itemno, 1, date, False))
    db.commit()
    c.close()
    db.close()


if __name__ == "__main__":
    if DEBUG > 2:
        print(movie_table_create_command)
    if DEBUG > 2:
        print(genre_table_create_command)
    if DEBUG > 2:
        print(week_table_create_command)
    if os.path.exists("twitter.sql"):
        print("***** DELETING SQL DB FILE!! *****")
        os.remove("twitter.sql")
    if not os.path.isfile("twitter.sql"):
        if os.path.exists("twitter.sql"):
            raise RuntimeError("twitter.sql exists but is not a file.")
        print("create tables ... ", end='')
        create_tables()
        print("done.\npopulate tables from CSV ... ", end='')
        populate_tables_from_csv()
        print("done.\npopulate tables from moviedata ... ", end='')
        populate_tables_from_moviedata()
        print("done.")
    db = sql.connect("twitter.sql")
    c = db.cursor()
    #for fields in c.execute("select items,ReleaseMonth,Sources,ScheduledRelease,InSample,name from movies"):
    #    print(*fields)
    for rec in sorted(c.execute("select name,sources from movies where InSample=1")):
        print(rec)
    c.close()
    db.commit()
    db.close()

