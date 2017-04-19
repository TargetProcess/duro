import os
import sqlite3
from datetime import datetime as dt, timedelta
from typing import List, Tuple, Dict

import networkx as nx
import psycopg2

from credentials import redshift_credentials
from file_utils import read_file, read_config
from errors import TestsFailedError


def get_children(root: str, graph_file: str) -> List:
    graph = nx.nx_pydot.read_dot(graph_file)
    print(root, ':', graph[root].keys())
    return graph[root].keys()


def create_table(table: str, db_path: str, views_path: str):
    query, interval, last_created = load_info(table, db_path)
    if not should_be_created(interval, last_created):
        return
    config = load_config(table, views_path)

    connection = create_connection()
    creation_timestamp = create_temp_table(table, query, config, connection)

    tests_queries = load_tests(table, views_path)
    test_results = run_tests(tests_queries, connection)
    if not test_results:
        drop_temp_table(table, connection)
        return

    replace_old_table(table, connection)
    update_last_created(table, creation_timestamp, db_path)
    if 'tableau_extracts' in config:
        create_tableau_extract(table, config['tableau_extracts'])
    if 'sync_db' in config and 'sync_table' in config:
        sync_to_db(table, config)


def load_info(table: str, db: str) -> Tuple[str, int, int]:
    with sqlite3.connect(db) as connection:
        cursor = connection.cursor()
        cursor.execute('''SELECT query, interval, last_created 
                        FROM tables
                        WHERE table_name = ? ''', (table,))
        return cursor.fetchone()


def should_be_created(interval: int, last_created: int) -> bool:
    if last_created is None:
        return True

    delta = dt.now() - dt.fromtimestamp(last_created)
    return (delta.total_seconds() / 60) > interval


def create_connection():
    connection = psycopg2.connect(redshift_credentials())
    connection.autocommit = True
    return connection


def create_temp_table(table: str, query: str, config: Dict, connection) -> int:
    print(f'Creating temp table for {table}')
    create_query = add_dist_sort_keys(table, query, config)
    full_query = f'''DROP TABLE IF EXISTS {table}_temp;
                {create_query}
                '''
    if config.get('users'):
        full_query += f'GRANT SELECT ON {table}_temp TO {config["users"]}'

    with connection.cursor() as cursor:
        cursor.execute(full_query)
    return int(dt.now().timestamp())


def add_dist_sort_keys(table: str, query: str, config: Dict) -> str:
    distkey = f'distkey({config["distkey"]})' if config.get('distkey') else ''
    sortkey = f'distkey({config["sortkey"]})' if config.get('sortkey') else ''
    diststyle = f'distkey({config["diststyle"]})' if config.get(
        'diststyle') else ''
    return f'''CREATE TABLE {table}_temp
            {distkey} {sortkey} {diststyle}
            AS ({query});'''


def load_tests(table: str, path: str) -> str:
    folder, file = table.split('.')
    tests_file = os.path.join(path, folder, f'{file}_test.sql')
    if os.path.isfile(tests_file):
        return read_file(tests_file)
    else:
        tests_file = os.path.join(path, f'{table}_test.sql')
        if os.path.isfile(tests_file):
            return read_file(tests_file)
    return ''


def run_tests(tests_queries: str, connection) -> bool:
    if len(tests_queries) == 0:
        return True

    print(f'Running tests')
    with connection.cursor() as cursor:
        queries = (q for q in tests_queries.split() if len(q) > 0)
        results = []
        for query in queries:
            cursor.execute(query)
            results.append((cursor.description[0].name,
                            cursor.fetchone()[0]))

        passed = all((result[1] for result in results))
        if not passed:
            failed_columns = [result[0] for result in results if not result[1]]
            raise(TestsFailedError(f'Failed tests: {failed_columns}'))

        return passed


def drop_temp_table(table: str, connection):
    print(f'Dropping temp table for {table}')
    with connection.cursor() as cursor:
        query_drop = f'DROP TABLE IF EXISTS {self.table}_old;'
        cursor.execute(query_drop)


def load_config(full_table_name: str, path: str) -> Dict:
    schema, table = full_table_name.split('.')

    global_config = read_config(os.path.join(path, 'global.conf'))
    schema_config_outside = read_config(os.path.join(path, f'{schema}.conf'))
    schema_config_inside = read_config(
        os.path.join(path, schema, f'{schema}.conf'))
    table_config_outside = read_config(
        os.path.join(path, f'{schema}.{table}.conf'))
    table_config_inside = read_config(
        os.path.join(path, schema, f'{table}.conf'))

    return {**global_config,
            **schema_config_outside, **schema_config_inside,
            **table_config_outside, **table_config_inside}


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


def update_last_created(table: str, timestamp: int, db: str):
    with sqlite3.connect(db) as connection:
        cursor = connection.cursor()
        cursor.execute('''UPDATE tables 
                        SET last_created = ?
                        WHERE table_name = ? ''', (timestamp, table))


def create_tableau_extract(table: str, extract: str):
    print(f'Creating extract for {table}')
    pass


def sync_to_db(table: str, config: Dict):
    print(f'Syncing {table}')
    pass


def main(root_table: str, db_path: str, views_path: str):
    db = os.path.join(db_path, 'duro.db')
    graph_file = os.path.join(db_path, 'dependencies.dot')
    children = get_children(root_table, graph_file)
    for child in children:
        main(child, db_path, views_path)
    create_table(root_table, db, views_path)


if __name__ == '__main__':
    db_path = '.'
    views_path = './views'
    # main('custom.companies', path)
    main('tauspy.most_active_users', db_path, views_path)
