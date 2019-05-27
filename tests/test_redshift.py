from unittest.mock import MagicMock

import pytest

from duro.create.redshift import (
    create_temp_table,
    drop_table,
    drop_old_table,
    drop_temp_table,
    drop_view,
    replace_old_table,
)


def redshift_execute(query: str):
    redshift_execute.last_query = query


def connection():
    def cursor_enter(cls):
        cursor = MagicMock()
        cursor.execute = redshift_execute
        redshift_execute.last_query = None
        return cursor

    conn = MagicMock()
    conn.cursor.return_value.__enter__ = cursor_enter
    return conn


def test_create_temp_table(table, table_without_config):
    create_temp_table(table, connection())
    assert pytest.similar(
        redshift_execute.last_query,
        """
            DROP TABLE IF EXISTS first.cities_duro_temp;
            CREATE TABLE first.cities_duro_temp
            distkey("continent")
            AS(select * from first.countries);
            GRANT SELECT ON first.cities_duro_temp TO user_one;
        """,
    )

    create_temp_table(table_without_config, connection())
    assert pytest.similar(
        redshift_execute.last_query,
        """
            DROP TABLE IF EXISTS first.cities_duro_temp;
            CREATE TABLE first.cities_duro_temp
            AS(select * from first.countries);;
        """,
    )


def test_drop_table():
    drop_table("first.cities", connection())
    assert pytest.similar(
        redshift_execute.last_query, "DROP TABLE IF EXISTS first.cities;"
    )


def test_drop_old_table():
    drop_old_table("first.cities", connection())
    assert pytest.similar(
        redshift_execute.last_query, "DROP TABLE IF EXISTS first.cities_duro_old;"
    )


def test_drop_temp_table():
    drop_temp_table("first.cities", connection())
    assert pytest.similar(
        redshift_execute.last_query, "DROP TABLE IF EXISTS first.cities_duro_temp;"
    )


def test_drop_view():
    drop_view("first.cities", connection())
    assert pytest.similar(
        redshift_execute.last_query, "DROP VIEW IF EXISTS first.cities;"
    )


def test_replace_old_table():
    replace_old_table("first.cities", connection())
    assert pytest.similar(
        redshift_execute.last_query,
        """
            DROP TABLE IF EXISTS first.cities_duro_old;
            CREATE TABLE IF NOT EXISTS first.cities (id int);
            ALTER TABLE first.cities RENAME TO cities_duro_old;
            ALTER TABLE first.cities_duro_temp RENAME TO cities;
        """,
    )
