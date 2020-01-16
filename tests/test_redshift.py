from unittest.mock import MagicMock

import pytest

from duro.create.redshift import (
    create_temp_table,
    drop_table,
    drop_old_table,
    drop_temp_table,
    drop_view,
    replace_old_table,
    get_snapshot_dates,
    create_snapshots_table,
    insert_new_snapshot_data,
    remove_old_snapshots,
    get_dependencies,
    update_view,
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
            drop table if exists first.cities_duro_temp;
            create table first.cities_duro_temp
            distkey("continent")
            as(select * from first.countries);
            grant select on first.cities_duro_temp to user_one;
        """,
    )

    create_temp_table(table_without_config, connection())
    print(redshift_execute.last_query)
    assert pytest.similar(
        redshift_execute.last_query,
        """
            drop table if exists first.cities_duro_temp;
            create table first.cities_duro_temp
            as (select * from first.countries);;
        """,
    )


def test_drop_table():
    drop_table("first.cities", connection())
    assert pytest.similar(
        redshift_execute.last_query, "drop table if exists first.cities;"
    )


def test_drop_old_table():
    drop_old_table("first.cities", connection())
    assert pytest.similar(
        redshift_execute.last_query, "drop table if exists first.cities_duro_old;"
    )


def test_drop_temp_table():
    drop_temp_table("first.cities", connection())
    assert pytest.similar(
        redshift_execute.last_query, "drop table if exists first.cities_duro_temp;"
    )


def test_drop_view():
    drop_view("first.cities", connection())
    assert pytest.similar(
        redshift_execute.last_query, "drop view if exists first.cities;"
    )


def test_replace_old_table():
    replace_old_table("first.cities", connection())
    assert pytest.similar(
        redshift_execute.last_query,
        """
            drop table if exists first.cities_duro_old;
            create table if not exists first.cities (id int);
            alter table first.cities rename to cities_duro_old;
            alter table first.cities_duro_temp rename to cities;
        """,
    )


def test_get_snapshot_dates():
    get_snapshot_dates("first.cities", connection())
    assert pytest.similar(
        redshift_execute.last_query,
        """
           select max(snapshot_timestamp),
                min(snapshot_timestamp)
           from first.cities_history
        """,
    )


def test_create_snapshots_table():
    create_snapshots_table("first.cities", connection())
    assert pytest.similar(
        redshift_execute.last_query,
        """
           create table first.cities_history as (
                select *, current_timestamp as snapshot_timestamp
                from first.cities
                limit 1
            );
            truncate table first.cities_history;
        """,
    )


def test_insert_new_snapshot_data():
    insert_new_snapshot_data("first.cities", connection())
    assert pytest.similar(
        redshift_execute.last_query,
        """
           insert into first.cities_history
           select *, current_timestamp
           from first.cities
        """,
    )


def test_remove_old_snapshots(table):
    table.snapshots_stored_for_mins = 180
    remove_old_snapshots(table, connection())
    assert pytest.similar(
        redshift_execute.last_query,
        """
           delete from first.cities_history
           where datediff('mins', 
                snapshot_timestamp::timestamp, 
                current_timestamp::timestamp) > 180
        """,
    )


def test_get_dependencies():
    get_dependencies("first", "cities", connection())
    assert pytest.similar(
        redshift_execute.last_query,
        """
            select distinct nv.nspname + '.' + v.relname as view_name,
                pg_get_viewdef(v.oid) as view_definition
            from pg_user, pg_namespace nv, pg_class v, pg_depend dv, 
                pg_depend dt, pg_class t, pg_namespace nt
            where nv.oid = v.relnamespace
                and v.relkind = 'v'
                and v.oid = dv.refobjid
                and dv.refclassid = 'pg_class'::regclass::oid
                and dv.classid = 'pg_rewrite'::regclass::oid
                and dv.deptype = 'i'
                and dv.objid = dt.objid
                and dv.refobjid <> dt.refobjid
                and dt.classid = 'pg_rewrite'::regclass::oid
                and dt.refclassid = 'pg_class'::regclass::oid
                and dt.refobjid = t.oid
                and t.relnamespace = nt.oid
                and (t.relkind = 'r' or t.relkind = 'v')
                and nt.nspname = 'first'
                and t.relname = 'cities';
        """,
    )


def test_update_view():
    view = "first.cities_view"
    definition = "select * from first.cities_duro_temp limit 20"
    update_view(view, definition, connection())
    assert pytest.similar(
        redshift_execute.last_query,
        """
            create or replace view first.cities_view as
            (select * from first.cities_duro_temp limit 20)
        """,
    )
