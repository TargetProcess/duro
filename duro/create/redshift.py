from datetime import timedelta
from typing import Tuple, List

import arrow
import psycopg2
from psycopg2.errorcodes import WRONG_OBJECT_TYPE, UNDEFINED_TABLE, UNDEFINED_COLUMN

from credentials import redshift_credentials
from utils.errors import TableCreationError, RedshiftConnectionError, DropOldTableError
from utils.logger import log_action
from utils.table import Table, temp_postfix, history_postfix, old_postfix


@log_action("create Redshift connection")
def create_connection():
    try:
        connection = psycopg2.connect(**redshift_credentials())
        connection.autocommit = True
        return connection
    except psycopg2.OperationalError:
        raise RedshiftConnectionError


@log_action("create temporary table")
def create_temp_table(table: Table, connection) -> int:
    create_query = table.get_query_with_dist_sort_keys()
    grant_select = table.load_grant_select_statements()
    full_query = f"""
        drop table if exists {table.name}{temp_postfix};
        {create_query}
        {grant_select};
        """
    try:
        with connection.cursor() as cursor:
            cursor.execute(full_query)
    except psycopg2.ProgrammingError as e:
        raise TableCreationError(table, str(e))
    return arrow.now().timestamp


@log_action("drop temporary table")
def drop_temp_table(table_name: str, connection):
    drop_table(f"{table_name}{temp_postfix}", connection)


@log_action("drop old table")
def drop_old_table(table_name: str, connection):
    drop_table(f"{table_name}{old_postfix}", connection)


def drop_table(table_name: str, connection):
    with connection.cursor() as cursor:
        query_drop = f"drop table if exists {table_name};"
        cursor.execute(query_drop)


@log_action("updating dependent views")
def update_dependent_views(table_name: str, connection):
    schema, table = table_name.split(".")
    dependencies = get_dependencies(schema, table, connection)
    if not dependencies:
        return

    for view_name, old_definition in dependencies:
        definition = old_definition.replace(table, f"{table}{temp_postfix}").rstrip(";")
        update_view(view_name, definition, connection)


@log_action("getting dependent views")
def get_dependencies(schema_name: str, table_name: str, connection) -> List[Tuple[str, str]]:
    # this is `information_schema.view_table_usage` without filtering by current user
    query = f"""
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
            and nt.nspname = '{schema_name}'
            and t.relname = '{table_name}'; 
    """
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()


def update_view(view_name: str, definition: str, connection):
    with connection.cursor() as cursor:
        query = f"""
            create or replace view {view_name} as
            ({definition})
        """
        cursor.execute(query)


@log_action("replace old table")
def replace_old_table(table_name: str, connection):
    short_table_name = table_name.split(".")[-1]

    drop_view(table_name, connection)

    update_dependent_views(table_name, connection)

    with connection.cursor() as cursor:
        query_replace = f"""
            drop table if exists {table_name}{old_postfix};
            create table if not exists {table_name} (id int);
            alter table {table_name} rename to {short_table_name}{old_postfix};
            alter table {table_name}{temp_postfix} rename to {short_table_name};
            """
        cursor.execute(query_replace)


def make_snapshot(table: Table, connection) -> bool:
    newest_snapshot, oldest_snapshot = get_snapshot_dates(table.name, connection)
    if not newest_snapshot:
        create_snapshots_table(table.name, connection)
        insert_new_snapshot_data(table.name, connection)
        return True

    if newest_snapshot:
        newest_snapshot_age = arrow.now() - newest_snapshot
        if newest_snapshot_age > timedelta(minutes=table.snapshots_interval_mins):
            insert_new_snapshot_data(table.name, connection)
            return True

    if oldest_snapshot:
        oldest_snapshot_age = arrow.now() - oldest_snapshot
        if oldest_snapshot_age > timedelta(minutes=table.snapshots_stored_for_mins):
            remove_old_snapshots(table, connection)

    return False


@log_action("get earliest and latest snapshot dates")
def get_snapshot_dates(table_name: str, connection) -> Tuple:
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"""
                select max(snapshot_timestamp),
                    min(snapshot_timestamp)
                from {table_name}{history_postfix}
            """
            )

            return cursor.fetchone()

    except psycopg2.ProgrammingError as e:
        if e.pgcode in (UNDEFINED_TABLE, UNDEFINED_COLUMN):
            return None, None


@log_action("create snapshots table")
def create_snapshots_table(table_name: str, connection):
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            create table {table_name}{history_postfix} as (
                select *, current_timestamp as snapshot_timestamp
                from {table_name}
                limit 1
            );
            truncate table {table_name}{history_postfix};
        """
        )


@log_action("insert new snapshot data")
def insert_new_snapshot_data(table_name: str, connection):
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            insert into {table_name}{history_postfix}
            select *, current_timestamp
            from {table_name} 
        """
        )


@log_action("remove old snapshots")
def remove_old_snapshots(table: Table, connection):
    with connection.cursor() as cursor:
        cursor.execute(
            f"""
            delete from {table.name}{history_postfix}
            where datediff('mins', 
                snapshot_timestamp::timestamp, 
                current_timestamp::timestamp) > {table.snapshots_stored_for_mins}
        """
        )


def drop_view(table_name: str, connection):
    with connection.cursor() as cursor:
        try:
            cursor.execute(f"drop view if exists {table_name};")
        except psycopg2.ProgrammingError as e:
            if e.pgcode == WRONG_OBJECT_TYPE:
                connection.rollback()
            else:
                raise
