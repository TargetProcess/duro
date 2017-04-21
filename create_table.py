import configparser
import os
import sqlite3
import sys
from datetime import datetime as dt
from typing import List, Dict, NamedTuple

import networkx as nx
import psycopg2

from credentials import redshift_credentials
from errors import (TableNotFoundError, MaterializationError,
                    TableCreationError)
from file_utils import read_file, read_config


class Table(NamedTuple):
    name: str
    query: str
    interval: int
    last_created: int


class GlobalConfig(NamedTuple):
    db_path: str
    views_path: str
    graph: nx.DiGraph


def get_children(root: str, graph: nx.DiGraph) -> List:
    try:
        print(f'Children of {root}: {list(graph[root].keys())}')
        return list(graph[root].keys())
    except KeyError:
        raise TableNotFoundError(
            'There’s no table with this name in configuration db.')


def create_table(table: Table, db_path: str, views_path: str):
    if not should_be_created(table.interval, table.last_created):
        print(f'{table.name} is fresh enough')
        return
    config = load_config(table.name, views_path)
    print(f'Creating {table.name} with {table.interval}')
    connection = create_connection()
    creation_timestamp = create_temp_table(table.name, table.query, config,
                                           connection)

    tests_queries = load_tests(table.name, views_path)
    test_results = run_tests(tests_queries, connection)
    if not test_results:
        drop_temp_table(table.name, connection)
        return

    replace_old_table(table.name, connection)
    drop_old_table(table.name, connection)
    connection.close()

    update_last_created(table.name, creation_timestamp, db_path)
    if 'tableau_extracts' in config:
        create_tableau_extract(table.name, config['tableau_extracts'])
    if 'sync_db' in config and 'sync_table' in config:
        sync_to_db(table.name, config)


def load_info(table: str, db: str) -> Table:
    with sqlite3.connect(db) as connection:
        cursor = connection.cursor()
        cursor.execute('''SELECT query, interval, last_created 
                        FROM tables
                        WHERE table_name = ? ''', (table,))
        # noinspection PyArgumentList
        return Table(table, *cursor.fetchone())


def should_be_created(interval: int, last_created: int) -> bool:
    if last_created is None or interval is None:
        return True

    delta = dt.now() - dt.fromtimestamp(last_created)
    return (delta.total_seconds() / 60) > interval


def create_connection():
    connection = psycopg2.connect(**redshift_credentials())
    connection.autocommit = True
    return connection


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
    return int(dt.now().timestamp())


def add_dist_sort_keys(table: str, query: str, config: Dict) -> str:
    distkey = f'distkey("{config["distkey"]}")' if config.get('distkey') else ''
    sortkey = f'sortkey("{config["sortkey"]}")' if config.get('sortkey') else ''
    diststyle = f'diststyle {config["diststyle"]}' if config.get(
        'diststyle') else ''
    return f'''CREATE TABLE {table}_temp
            {distkey} {sortkey} {diststyle}
            AS ({query});'''


def load_tests(table: str, path: str) -> str:
    print(f'Loading tests for {table}')
    folder, file = table.split('.')
    tests_file = os.path.join(path, folder, f'{file}_test.sql')
    if os.path.isfile(tests_file):
        return read_file(tests_file).replace(f'{table}', f'{table}_temp')
    else:
        tests_file = os.path.join(path, f'{table}_test.sql')
        if os.path.isfile(tests_file):
            return read_file(tests_file).replace(f'{table}', f'{table}_temp')
    print(f'No tests for {table}')
    return ''


def run_tests(tests_queries: str, connection) -> bool:
    if len(tests_queries) == 0:
        return True

    print(f'Running tests')
    with connection.cursor() as cursor:
        queries = (q for q in tests_queries.split(';') if len(q) > 0)
        results = []
        for query in queries:
            cursor.execute(query)
            results.append((cursor.description[0].name,
                            cursor.fetchone()[0]))

        passed = all((result[1] for result in results))
        if not passed:
            failed_columns = [result[0] for result in results if not result[1]]
            print(f'Failed tests: {failed_columns}')

        return passed


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

    merged = {**global_config,
              **schema_config_outside, **schema_config_inside,
              **table_config_outside, **table_config_inside}

    for key, value in merged.items():
        if value in ('null', 'None', ''):
            merged[key] = None

    return merged


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


def create_tree(root: str, global_config: GlobalConfig, interval: int = None):
    children = get_children(root, global_config.graph)
    table = load_info(root, global_config.db_path)

    if table.interval is None and interval is not None:
        print(f'Updating interval for {root}')
        table = Table(table.name, table.query, interval, table.last_created)

    # for child in children:
    #     create_tree(child, global_config, table.interval)
    try:
        create_table(table, global_config.db_path, global_config.views_path)
    except MaterializationError as e:
        print(e)


def load_global_config() -> GlobalConfig:
    try:
        config = configparser.ConfigParser()
        config.read('config.conf')
        db_path = config['main'].get('db', './duro.db')
        views_path = config['main'].get('views', './views')
        graph_file_path = config['main'].get('graph', 'dependencies.dot')
        graph = nx.nx_pydot.read_dot(graph_file_path)
        return GlobalConfig(db_path, views_path, graph)
    except configparser.NoSectionError:
        print('No ’main’ section in config.conf')
        sys.exit(1)


def main(root_table: str):
    create_tree(root_table, load_global_config())


if __name__ == '__main__':
    main('satisfaction.widget')
    # custom.title_tags
    # feedback.contacts
