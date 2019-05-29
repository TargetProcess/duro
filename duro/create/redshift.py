import arrow
import psycopg2

from duro.credentials import redshift_credentials
from utils.errors import TableCreationError, RedshiftConnectionError
from duro.utils.logger import log_action
from duro.utils.table import Table, temp_postfix


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
        DROP TABLE IF EXISTS {table.name}{temp_postfix};
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
def drop_temp_table(table: str, connection):
    drop_table(f"{table}{temp_postfix}", connection)


@log_action("drop old table")
def drop_old_table(table: str, connection):
    drop_table(f"{table}_duro_old", connection)


def drop_table(table: str, connection):
    with connection.cursor() as cursor:
        query_drop = f"DROP TABLE IF EXISTS {table};"
        cursor.execute(query_drop)


@log_action("replace old table")
def replace_old_table(table: str, connection):
    short_table_name = table.split(".")[-1]

    drop_view(table, connection)

    with connection.cursor() as cursor:
        query_replace = f"""
            DROP TABLE IF EXISTS {table}_duro_old;
            CREATE TABLE IF NOT EXISTS {table} (id int);
            ALTER TABLE {table} RENAME TO {short_table_name}_duro_old;
            ALTER TABLE {table}{temp_postfix} RENAME TO {short_table_name};
            """
        cursor.execute(query_replace)


def drop_view(table: str, connection):
    with connection.cursor() as cursor:
        try:
            cursor.execute(f"DROP VIEW IF EXISTS {table};")
        except psycopg2.ProgrammingError as e:
            if e.pgcode == "42809":
                connection.rollback()
            else:
                raise
