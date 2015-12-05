import csv
import sqlite3 as sql

MONTHS = [None, 'jan', 'feb', 'mar', 'apr', 'may', 'jun',
          'jul', 'aug', 'sep', 'oct', 'nov', 'dec']
STRIPPUNCT = {
    ord(','): None,
    ord('/'): None,                     # #### what about fractions?
    }
STRIPNL = {
    ord('\n'): ' ',
    ord('\r'): None,
    }

def row_csv_to_sql(row):
    """
    CSV data is parsed as strings; convert them to "natural" types.
    Handle defects in data (eg, embedded commas in numbers).
    
    This probably could be done with the CSV module itself but I'm not
    familiar with it and was in a hurry.
    """

    def date_csv_to_sql(d):
        "Convert string date in the form DD-Mon-YY to sql.Date."
        year = int(d[7:]) + 2000
        month = MONTHS.index(d[3:6].lower())
        day = int(d[:2])
        return sql.Date(year, month, day)

    def typify(s, typ):
        "Convert numeric string to numeric type after removing defects."
        item = s.strip().lower().translate(STRIPPUNCT)
        if item in ["na"]:
            return None
        else:
            return typ(item) if item else None

    result = [date_csv_to_sql(row[0]),
              int(row[1]),
              row[2].strip().translate(STRIPNL)]
    for i in range(3, 16):
        result.append(typify(row[i], int))
    result.append(typify(row[16], float))
    for i in range(17, 27):
        result.append(typify(row[i], int))
    for i in range(27, 35):
        result.append(typify(row[i], float))
    for i in range(35, 43):
        result.append(typify(row[i], int))
    return result


if __name__ == "__main__":
    r = csv.reader(open("Database for thesis.csv"))
    r0 = next(r)
    print(*(repr(x) for x in r0))
    r1 = next(r)
    print(*(repr(x) for x in row_csv_to_sql(r1)))

