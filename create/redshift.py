import arrow
from typing import Dict

import psycopg2

from create.config import add_dist_sort_keys
from errors import TableCreationError


def create_temp_table(table: str, query: str, config: Dict, connection) -> int:
    print(f'Creating temp table for {table}')

    create_query = add_dist_sort_keys(table, query.rstrip(';\n'), config)
    full_query = f'''DROP TABLE IF EXISTS {table}_temp;
                {create_query}
                '''

    if config.get('users'):
        full_query += f'GRANT SELECT ON {table}_temp TO {config["users"]}'

    try:
        with connection.cursor() as cursor:
            cursor.execute(full_query)
    except psycopg2.ProgrammingError as e:
        raise TableCreationError(str(e))
    return arrow.now().timestamp


def drop_temp_table(table: str, connection):
    print(f'Dropping temp table for {table}')
    drop_table(f'{table}_temp', connection)


def drop_old_table(table: str, connection):
    print(f'Dropping old table for {table}')
    drop_table(f'{table}_old', connection)


def drop_table(table: str, connection):
    with connection.cursor() as cursor:
        query_drop = f'DROP TABLE IF EXISTS {table};'
        cursor.execute(query_drop)


def replace_old_table(table: str, connection):
    print(f'Replacing old table for {table}')
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
