import sqlite3 as sql
import os
import os.path

term_extract_command = \
    "select term_map.term,avg(synsets.positive),avg(synsets.negative) " \
    "from term_map inner join synsets " \
    "on term_map.synset=synsets.id " \
    "where term_map.term=? "

synset_table_create_command = "create table synsets ( " \
    "id integer, " \
    "speech_part text, " \
    "positive float, " \
    "negative float, " \
    "gloss text " \
    ")"

term_table_create_command = "create table term_map ( " \
    "term text, " \
    "synset integer, " \
    "sense integer " \
    ")"

def create_tables(db):
    c = db.cursor()
    c.execute(synset_table_create_command)
    c.execute(term_table_create_command)
    c.close()
    db.commit()

def populate_from_sentiwordnet(db):
    with open("sentiwordnet-valent.txt") as f:
        c = db.cursor()
        for line in f:
            line = line.strip()
            if line.startswith("#"):
                continue
            fields = line.split('\t')
            terms = [term.split('#') for term in fields[4].split()]
            c.execute("insert into "
                      "synsets (id,speech_part,positive,negative,gloss) "
                      "values (?,?,?,?,?)",
                      (fields[1], fields[0], fields[2], fields[3], fields[5]))
            for term in terms:
                c.execute("insert into "
                          "term_map (term,synset,sense) "
                          "values (?,?,?)",
                          (term[0], fields[1], term[1]))
        c.close()
        db.commit()


if __name__ == "__main__":
    db = sql.connect("twitter.sql")
    #create_tables(db)
    #populate_from_sentiwordnet(db)
    c = db.cursor()
    c.execute("select * from term_map")
    for i in range(10):
        print(*next(c))
    c.execute("select id,speech_part,positive,negative from synsets")
    for i in range(10):
        print(*next(c))
    c.execute("select term_map.term,synsets.positive,synsets.negative "
              "from term_map inner join synsets "
              "on term_map.synset=synsets.id")
    for i in range(10):
        print(*next(c))
    for word in ["full-length", "absolute", "direct", "unquestioning",
                 "implicit", "movie", "moving", "good", "recommend"]:
        c.execute(term_extract_command, (word,))
        rows = c.fetchall()
        if rows:
            for rec in rows:
                print(*rec)
        else:
            print("no matches for", word)

    c.close()
    db.close()
