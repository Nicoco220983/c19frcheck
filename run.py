#!/usr/bin/env  python3
import os
import sys
import re
import click
import sqlite3
import urllib.request
from datetime import datetime, timedelta
from collections import defaultdict
from glob import glob
import xlrd
import matplotlib.pyplot as plt

HERE = os.path.dirname(__file__)

DATE_RANGES = {
    "grippe 2016/2017": ("2017-01-01", "2017-02-01"),
    "covid 2019/2020": ("2020-03-20", "2020-04-20")
}

DATA_FILES_CONFS = [
    {
        "src": "https://www.data.gouv.fr/fr/datasets/r/fd61ff96-1e4e-450f-8648-3e3016edbe34",
        "type": "deces",
        "name": "deces-2017.txt"
    },
    {
        "src": "https://www.data.gouv.fr/fr/datasets/r/a1f09595-0e79-4300-be1a-c97964e55f05",
        "type": "deces",
        "name": "deces-2020.txt"
    },
    {
        "src": "https://www.insee.fr/fr/statistiques/fichier/1913143/pyramide-des-ages-2017.xls",
        "type": "pyramide-des-ages",
        "annee": 2017,
        "name": "pyramide-des-ages-2017.xls",
        "cols": {
            "age": 2,
            "nb": 5
        },
        "rows": (7, 107)
    },
    {
        "src": "https://www.insee.fr/fr/statistiques/fichier/1913143/pyramide-des-ages-2020.xls",
        "type": "pyramide-des-ages",
        "annee": 2020,
        "name": "pyramide-des-ages-2020.xls",
        "cols": {
            "age": 2,
            "nb": 5
        },
        "rows": (7, 107)
    },
]


@click.group()
def main():
    pass


@main.command("all")
def all():
    _init_db()
    _download_data()
    _import_data()
    _compute_taux_mortalite_par_age()
    _compute_deces_par_date()
    _compute_population_par_age()
    _compute_deces_par_age()


@main.command("init_db")
def init_db_cmd():
    _init_db()


def _db_connect():
    return sqlite3.connect(os.path.join(HERE, "data.sqlite"))


def _init_db():
    with _db_connect() as conn:
        cur = conn.cursor()
        cur.execute('''CREATE TABLE IF NOT EXISTS deces(sex text, date_naissance text, date_deces text, age integer)''')
        cur.execute('''DELETE FROM deces''')
        cur.execute('''CREATE TABLE IF NOT EXISTS ages(annee integer, age integer, nb integer)''')
        cur.execute('''DELETE FROM ages''')


@main.command("download_data")
def download_data_cmd():
    _download_data()


def _download_data():
    data_path = os.path.join(HERE, "data")
    if not os.path.exists(data_path):
        os.makedirs(data_path)
    for conf in DATA_FILES_CONFS:
        _download_data_file(conf)


def _download_data_file(conf):
    path = os.path.join(HERE, "data", conf["name"])
    if not os.path.exists(path):
        print(f'Download {conf["name"]}... ', end='')
        urllib.request.urlretrieve(conf["src"], path)
        print(f'DONE')


@main.command("import_data")
def import_data_cmd():
    _import_data()


def _import_data():
    with _db_connect() as conn:
        for conf in DATA_FILES_CONFS:
            print(f"import {conf['name']}")
            if conf["type"] == "deces":
                datas = _parse_deces_file(conf)
                _insert_deces_in_db(conn, datas)
            if conf["type"] == "pyramide-des-ages":
                datas = _parse_pda_file(conf)
                _insert_ages_in_db(conn, datas)




def _parse_deces_file(conf):
    path = os.path.join(HERE, "data", conf["name"])
    with open(path) as file:
        res, errors = [], []
        num_line = 1
        nb_inserted = 0
        for line in file.readlines():
            try:
                date_naissance = _parse_date(line[81:89], def_month="06", def_day="15")
                date_deces = _parse_date(line[154:162])
                naissance_dt = _to_dt(date_naissance)
                deces_dt = _to_dt(date_deces)
                age = _dt_to_annees(deces_dt - naissance_dt)
                parsed = {
                    "sex": _parse_sex(line[80]),
                    "date_naissance": date_naissance,
                    "date_deces": date_deces,
                    "age": age
                }
                res.append(parsed)
                nb_inserted += 1
            except ParseError as exc:
                if isinstance(exc, ParseError):
                    errors.append(exc)
            num_line += 1
        print(f"Nb errors for {conf['name']}: {len(errors)} / {num_line-1} ({'{:.5f}'.format(100*len(errors)/(num_line-1))}%)")
        for e in errors[:10]: print(e)
        return res


def _insert_deces_in_db(conn, rows):
    cur = conn.cursor()
    cur.executemany('''INSERT INTO deces (sex, date_naissance, date_deces, age) VALUES (?, ?, ?, ?)''',
        [(row["sex"], row["date_naissance"], row["date_deces"], row["age"]) for row in rows])


def _parse_pda_file(conf):
    path = os.path.join(HERE, "data", conf["name"])
    book = xlrd.open_workbook(path)
    sheet = book.sheet_by_index(0)
    res = []
    # loop on rows
    # (with xlrd rows and columns start with 0)
    first_row, last_row = conf["rows"]
    age_col = conf["cols"]["age"]
    nb_col = conf["cols"]["nb"]
    for i in range(first_row-1, last_row):
        # parse age
        age = sheet.cell(i, age_col-1).value
        assert age != ''
        if type(age) is str:
            # traitement specifique pour "100 et +"
            age = int(re.sub("[^0-9]", "", age))
        else:
            age = int(age)
        # parse nb
        nb = sheet.cell(i, nb_col-1).value
        assert nb != ''
        nb = int(nb)
        res.append({
            "annee": conf["annee"],
            "age": age,
            "nb": nb
        })
    return res


def _insert_ages_in_db(conn, rows):
    cur = conn.cursor()
    cur.executemany('''INSERT INTO ages (annee, age, nb) VALUES (?, ?, ?)''',
        [(row["annee"], row["age"], row["nb"]) for row in rows])


@main.command("compute_taux_mortalite_par_age")
def compute_taux_mortalite_par_age():
    _compute_taux_mortalite_par_age()


def _compute_taux_mortalite_par_age():
    print(f"compute taux_mortalite_par_age")
    _assert_all_date_ranges_have_same_duration()
    with _db_connect() as conn:
        age_range = list(range(1, 101))
        plt.clf()
        plt.title("Taux de mortalité par âge")
        def _compute_taux_mortalite_par_age(deces, pop):
            _div = lambda a, b: a/b if b else 0
            return {age: _div(nb, pop.get(age)) for age, nb in deces.items()}
        def _plot(title, date_range, annee):
            pop_par_age = _select_pop_par_age(conn, annee)
            nb_deces_par_age = _select_deces_par_age(conn, date_range)
            taux_mortalite_par_age = _compute_taux_mortalite_par_age(nb_deces_par_age, pop_par_age)
            plt.plot(age_range, [taux_mortalite_par_age.get(i, 0) for i in age_range], label=f"{title} (de {date_range[0]} à {date_range[1]})")
        _plot("Grippe", DATE_RANGES["grippe 2016/2017"], 2017)
        _plot("Covid19", DATE_RANGES["covid 2019/2020"], 2020)
        plt.legend()
        plt.savefig(os.path.join(HERE, 'results/taux_mortalite_par_age.png'))


def _select_pop_par_age(conn, annee):
    rows = conn.cursor().execute(
        '''SELECT age, SUM(nb) FROM ages WHERE annee = ? GROUP BY age''',
        [annee]
    )
    return {age:nb for age, nb in rows}


def _select_deces_par_age(conn, date_range):
    rows = conn.cursor().execute(
        '''SELECT age, count(*) FROM deces WHERE date_deces BETWEEN ? AND ? GROUP BY age''',
        [*date_range]
    )
    return {age: nb for age, nb in rows}


@main.command("compute_deces_par_date")
def compute_deces_par_date():
    _compute_deces_par_date()


def _compute_deces_par_date():
    print(f"compute deces_par_date")
    _assert_all_date_ranges_have_same_duration()
    with _db_connect() as conn:
        date_range_2017 = DATE_RANGES["grippe 2016/2017"]
        date_range_2020 = DATE_RANGES["covid 2019/2020"]
        deces_par_date_2017 = _select_deces_par_date(conn, date_range_2017)
        deces_par_date_2020 = _select_deces_par_date(conn, date_range_2020)
        dates_2017 = _date_range_to_dates(date_range_2017)
        dates_2020 = _date_range_to_dates(date_range_2020)
        plt.clf()
        plt.plot(range(len(dates_2017)), [deces_par_date_2017.get(d, 0) for d in dates_2017], label=f"Grippe (de {date_range_2017[0]} à {date_range_2017[1]})")
        plt.plot(range(len(dates_2020)), [deces_par_date_2020.get(d, 0) for d in dates_2020], label=f"Covid19 (de {date_range_2020[0]} à {date_range_2020[1]})")
        plt.title("Décès par date")
        plt.legend()
        plt.savefig(os.path.join(HERE, 'results/deces_par_date.png'))


def _select_deces_par_date(conn, date_range):
    rows = conn.cursor().execute(
        '''SELECT date_deces, count(*) FROM deces WHERE date_deces BETWEEN ? AND ? GROUP BY date_deces''',
        [*date_range]
    )
    return {_to_dt(date_deces): nb for date_deces, nb in rows}


@main.command("compute_population_par_age")
def compute_population_par_age():
    _compute_population_par_age()


def _compute_population_par_age():
    print(f"compute population_par_age")
    _assert_all_date_ranges_have_same_duration()
    with _db_connect() as conn:
        plt.clf()
        plt.title("Population par âge")
        age_range = list(range(1, 101))
        def _plot(annee):
            pop_par_age = _select_pop_par_age(conn, annee)
            plt.plot(age_range, [pop_par_age.get(i, 0) for i in age_range], label=annee)
        _plot(2017)
        _plot(2020)
        plt.legend()
        plt.savefig(os.path.join(HERE, 'results/population_par_age.png'))


@main.command("compute_deces_par_age")
def compute_deces_par_age():
    _compute_deces_par_age()


def _compute_deces_par_age():
    print("compute deces_par_age")
    _assert_all_date_ranges_have_same_duration()
    with _db_connect() as conn:
        age_range = list(range(1, 101))
        plt.clf()
        plt.title("Décès par âge")
        def _plot(title, date_range):
            nb_deces_par_age = _select_deces_par_age(conn, date_range)
            plt.plot(age_range, [nb_deces_par_age.get(i, 0) for i in age_range], label=f"{title} (de {date_range[0]} à {date_range[1]})")
        _plot("Grippe", DATE_RANGES["grippe 2016/2017"])
        _plot("Covid19", DATE_RANGES["covid 2019/2020"])
        plt.legend()
        plt.savefig(os.path.join(HERE, 'results/deces_par_age.png'))


# parsing

class ParseError(Exception):
    pass

class ParseSexError(ParseError):
    pass

def _parse_sex(val):
    if val == "1": return "M"
    if val == "2": return "F"
    raise ParseSexError(f"Bad sex value: {val}")

class DateParseError(ParseError):
    pass

def _parse_date(val, def_month=None, def_day=None):
    try:
        year = val[0:4]
        month = val[4:6]
        day = val[6:8]
        if year=="0000":
            raise DateParseError(f"Bad year value: {year}")
        if month=="00":
            if def_month:
                month = def_month
            else:
                raise DateParseError(f"Bad month value: {month}")
        if day=="00":
            if def_day:
                day = def_day
            else:
                raise DateParseError(f"Bad day value: {day}")
        return f"{year}-{month}-{day}"
    except Exception as exc:
        raise DateParseError(exc)


def _assert_all_date_ranges_have_same_duration():
    duration = None
    for _key, (start_date, end_date) in DATE_RANGES.items():
        dur = _to_dt(end_date) - _to_dt(start_date)
        if duration is None:
            duration = dur
        else:
            assert duration == dur


def _date_range_to_dates(date_range):
    res = []
    start, end = _to_dt(date_range[0]), _to_dt(date_range[1])
    day = start
    while day <= end:
        res.append(day)
        day += timedelta(days=1)
    return res


# utils

def _to_dt(date):
    return datetime.strptime(date, '%Y-%m-%d')

def _dt_to_annees(dt):
    return int(dt.days / 365.25)

if __name__ == "__main__":
    main()