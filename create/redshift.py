from logging import Logger
from typing import Dict

import arrow
import psycopg2

from create.table_config import add_dist_sort_keys, load_grant_select_statements
from credentials import redshift_credentials
from errors import TableCreationError, RedshiftConnectionError


def create_connection():
    try:
        connection = psycopg2.connect(**redshift_credentials())
        connection.autocommit = True
        return connection
    except psycopg2.OperationalError:
        raise RedshiftConnectionError


def create_temp_table(table: str, query: str, config: Dict, connection,
                      logger: Logger) -> int:
    logger.info(f'Creating temp table for {table}')

    create_query = add_dist_sort_keys(table, query.rstrip(';\n'), config)
    grant_select = load_grant_select_statements(table, config)
    full_query = f'''DROP TABLE IF EXISTS {table}_temp;
                {create_query};
                {grant_select};
                '''
    try:
        with connection.cursor() as cursor:
            cursor.execute(full_query)
    except psycopg2.ProgrammingError as e:
        raise TableCreationError(table, str(e))
    return arrow.now().timestamp


def drop_temp_table(table: str, connection, logger: Logger):
    logger.info(f'Dropping temp table for {table}')
    drop_table(f'{table}_temp', connection)


def drop_old_table(table: str, connection, logger: Logger):
    logger.info(f'Dropping old table for {table}')
    drop_table(f'{table}_old', connection)


def drop_table(table: str, connection):
    with connection.cursor() as cursor:
        query_drop = f'DROP TABLE IF EXISTS {table};'
        cursor.execute(query_drop)


def replace_old_table(table: str, connection, logger: Logger):
    logger.info(f'Replacing old table for {table}')
    short_table_name = table.split('.')[-1]

    drop_view(table, connection)
    with connection.cursor() as cursor:
        query_replace = f'''DROP TABLE IF EXISTS {table}_old;
                CREATE TABLE IF NOT EXISTS {table} (id int);
                ALTER TABLE {table} RENAME TO {short_table_name}_old;
                ALTER TABLE {table}_temp RENAME TO {short_table_name};'''
        cursor.execute(query_replace)


def drop_view(table: str, connection):
    with connection.cursor() as cursor:
        try:
            cursor.execute(f'DROP VIEW IF EXISTS {table};')
        except psycopg2.ProgrammingError as e:
            if e.pgcode == '42809':
                connection.rollback()
            else:
                raise
