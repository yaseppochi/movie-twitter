import csv
import os.path
import sqlite3 as sql
from twitter_csv_to_sql import row_csv_to_sql

movie_table_create_command = "create table movies (" \
    "ReleaseMonth date, " \
    "Items integer, " \
    "Name text, " \
    "TotalGross integer, " \
    "Sum4 integer, " \
    "Sum7 integer, " \
    "Budget integer, " \
    "ScreensWeek1 integer, " \
    "MarketShare real, " \
    "Genre text, " \
    "GenreCheck integer" \
    ")"

genre_table_create_command = "create table genres (" \
    "id integer, " \
    "genre text" \
    ")"

week_table_create_command = "create table weeks (" \
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
            print("Inserting genre", f[0], "at bit", bit)
            bit += 1
    # populate movies and weeks
    def calculate_genre(row):
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
        c.execute("insert into movies values (?,?,?,?,?,?,?,?,?,?,?)",
                  row[0:6] + row[14:17] + calculate_genre(row) + row[26:27])
        for w in range(8):
            c.execute("insert into weeks values (?,?,?,?,?)",
                      (row[2], w, row[6 + w], row[27 + w], row[35 + w]))
        print("inserting record for", row[2])
    c.close()
    db.commit()
    db.close()


if __name__ == "__main__":
    #print(movie_table_create_command)
    #print(genre_table_create_command)
    #print(week_table_create_command)
    if os.path.exists("twitter.sql"):
        print("***** DELETING SQL DB FILE!! *****")
        os.remove("twitter.sql")
    if not os.path.isfile("twitter.sql"):
        if os.path.exists("twitter.sql"):
            raise RuntimeError("twitter.sql exists but is not a file.")
        print("create tables ... ", end='')
        create_tables()
        print("done.\npopulate tables ... ", end='')
        populate_tables_from_csv()
        print("done.")
    db = sql.connect("twitter.sql")
    c = db.cursor()
    for fields in c.execute("select name,ReleaseMonth,genre,GenreCheck from movies"):
        print(*fields)
    c.close()
    db.commit()
    db.close()
