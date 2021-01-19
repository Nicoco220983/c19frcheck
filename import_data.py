#!/usr/bin/env  python3
import os
import sys
import click
import sqlite3
from collections import defaultdict
from glob import glob

HERE = os.path.dirname(__file__)

@click.command()
def main():
    _init_db()
    _import_data()


def _import_data():
    with _db_connect() as conn:
        for _path in glob(os.path.join(HERE, "data/*.txt")):
            with open(_path) as _file:
                num_line = 1
                nb_inserted = 0
                nb_errors = defaultdict(int)
                for line in _file.readlines():
                    try:
                        parsed = {
                            "sex": _parse_sex(line[80]),
                            "birth_date": _parse_date(line[81:89], def_month="01", def_day="01"),
                            "death_date": _parse_date(line[154:162])
                        }
                        _insert_death_in_db(conn, parsed)
                        nb_inserted += 1
                    except ParseError as exc:
                        if isinstance(exc, ParseSexError):
                            nb_errors["sex"] += 1
                        if isinstance(exc, DateParseError):
                            nb_errors["date"] += 1
                    num_line += 1
            # print res
            print(f"{nb_inserted} line inserted")
            for err in nb_errors:
                if nb_errors[err] > 0:
                    print(f"{nb_errors[err]} errors of type {err}")

# parsing

class ParseError(Exception):
    pass

class ParseSexError(ParseError):
    pass

def _parse_sex(val):
    if val == "1": return "M"
    if val == "2": return "F"
    raise ParseSexError("Bad value")

class DateParseError(ParseError):
    pass

def _parse_date(val, def_month=None, def_day=None):
    try:
        year = val[0:4]
        month = val[4:6]
        day = val[6:8]
        if year=="0000":
            raise DateParseError("Bad Value")
        if month=="00":
            if def_month:
                month = def_month
            else:
                raise DateParseError("Bad Value")
        if day=="00":
            if def_day:
                day = def_day
            else:
                raise DateParseError("Bad Value")
        return f"{year}-{month}-{day}"
    except Exception as exc:
        raise DateParseError(exc)

# db

def _db_connect():
    return sqlite3.connect(os.path.join(HERE, "data.sqlite"))

def _init_db():
    with _db_connect() as conn:
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS deaths(sex text, birth_date text, death_date text)''')
        cur.execute('''DELETE FROM deaths''')

def _insert_death_in_db(conn, row):
    cur = conn.cursor()
    cur.execute('''INSERT INTO deaths (sex, birth_date, death_date) VALUES (?, ?, ?)''',
        [row["sex"], row["birth_date"], row["death_date"]])

if __name__ == "__main__":
    main()
