import arrow

from scheduler.graph import build_graph
from server.sqlite import (
    get_all_tables,
    get_jobs,
    get_table_details,
    set_table_for_update,
    propagate_force_flag,
    get_overview_stats,
)
from utils.file_utils import load_tables_in_path


def test_get_all_tables(db_cursor):
    tables = [dict(t) for t in get_all_tables(db_cursor)]
    assert len(tables) == 4
    first_cities = [t for t in tables if t["table_name"] == "first.cities"][0]
    assert first_cities["interval"] == 1440


def test_get_jobs(db_cursor):
    current = get_jobs(0, arrow.utcnow().timestamp, db_cursor)
    assert len(current) == 3
    assert current[0]["table"] == "first.cities"

    empty = get_jobs(0, 1522151000, db_cursor)
    assert len(empty) == 0


def test_get_table_details(db_cursor):
    details = get_table_details(db_cursor, "first.cities")
    assert len(details) == 3
    assert details[0]["interval"] == 1440
    assert details[0]["start"] == 1522151835
    assert details[0]["table_name"] == "first.cities"

    empty_details = get_table_details(db_cursor, "non-existent")
    assert empty_details == []


def test_get_overview_stats(db_connection):
    assert get_overview_stats(db_connection, 1_000_000) == {
        "load": 0,
        "tables": 1,
        "updates": 2,
    }

    assert get_overview_stats(db_connection, 1) == {
        "load": None,
        "tables": 0,
        "updates": 0,
    }


def test_propagate_force_flag(db_connection, views_path):
    tables = load_tables_in_path(views_path)
    graph = build_graph(tables)

    propagate_force_flag(db_connection, "first.cities", graph)

    cursor = db_connection.cursor()
    cursor.execute("select count(*) from tables where force = 1")
    assert cursor.fetchone()[0] == 1
    cursor.execute("select table_name from tables where force = 1")
    assert cursor.fetchone()[0] == "first.cities"

    cursor.execute("update tables set force = null")

    propagate_force_flag(db_connection, "second.parent", graph)
    cursor.execute("select count(*) from tables where force = 1")
    assert cursor.fetchone()[0] == 3
    cursor.execute("select table_name from tables where force = 1")
    assert cursor.fetchone()[0] == "first.cities"
    assert cursor.fetchone()[0] == "second.child"
    assert cursor.fetchone()[0] == "second.parent"

    cursor.execute("update tables set force = null")
    propagate_force_flag(db_connection, "non-existent", graph)


def test_set_table_for_update(db_connection):
    config = "test_config.conf"
    set_table_for_update(db_connection, "first.cities", 0, config)
    cursor = db_connection.cursor()
    cursor.execute("select count(*) from tables where force = 1")
    assert cursor.fetchone()[0] == 1
    cursor.execute("select table_name from tables where force = 1")
    assert cursor.fetchone()[0] == "first.cities"

    cursor.execute("update tables set force = null")

    set_table_for_update(db_connection, "second.child", 0, config)
    cursor.execute("select count(*) from tables where force = 1")
    assert cursor.fetchone()[0] == 1
    cursor.execute("select table_name from tables where force = 1")
    assert cursor.fetchone()[0] == "second.child"

    cursor.execute("update tables set force = null")

    set_table_for_update(db_connection, "second.child", 1, config)
    cursor.execute("select count(*) from tables where force = 1")
    assert cursor.fetchone()[0] == 2
    cursor.execute("select table_name from tables where force = 1")
    assert cursor.fetchone()[0] == "first.cities"
    assert cursor.fetchone()[0] == "second.child"
