from pprint import pprint
import arrow

from server.sqlite import (get_all_tables, get_jobs,
                           get_table_details, set_table_for_update,
                           propagate_force_flag, get_overview_stats)

from server.formatters import format_job


def test_get_all_tables(db_cursor):
    tables = [dict(t) for t in get_all_tables(db_cursor)]
    assert len(tables) == 4
    first_cities = [t for t in tables if t['table_name'] == 'first.cities'][0]
    assert first_cities['interval'] == 1440


def test_get_jobs(db_cursor):
    current = get_jobs(0, arrow.utcnow().timestamp, db_cursor)
    assert len(current) == 1
    assert current[0]['table'] == 'first.cities'
    pprint(format_job(current[0]))

    empty = get_jobs(0, 1522151000, db_cursor)
    assert len(empty) == 0


def test_get_table_details(db_cursor):
    details = get_table_details(db_cursor, 'first.cities')
    assert len(details) == 1
