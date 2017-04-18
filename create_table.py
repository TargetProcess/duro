import os
import sqlite3
from typing import List, Tuple, Dict

import networkx as nx

from file_utils import read_file, read_config


def get_children(root: str, graph_file: str) -> List:
    graph = nx.nx_pydot.read_dot(graph_file)
    print(root, ':', graph[root].keys())
    return graph[root].keys()


def create_table(table: str, db_path: str, views_path: str):
    query, interval, last_created = load_info(table, db_path)
    if not should_be_created(interval, last_created):
        return
    config = load_config(table, views_path)
    timestamp = create_temp_table(table, query, config)
    tests_queries = load_tests(table, views_path)
    test_results = run_tests(tests_queries)
    if not test_results:
        drop_temp_table(table)
        return
    replace_old_table(table)
    update_last_created(table, timestamp, db_path)
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
    # TODO: add real delta calculations
    return True


def create_temp_table(table: str, query: str, config: Dict) -> int:
    # TODO: create tables
    return 0


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


def run_tests(tests_queries: str) -> bool:
    if len(tests_queries) == 0:
        return True

    return True


def drop_temp_table(table: str):
    pass


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


def replace_old_table(table: str):
    pass


def update_last_created(table: str, timestamp: int, db: str):
    with sqlite3.connect(db) as connection:
        cursor = connection.cursor()
        cursor.execute('''UPDATE tables 
                        SET last_created = ?
                        WHERE table_name = ? ''', (timestamp, table))


def create_tableau_extract(table: str, extract: str):
    pass


def sync_to_db(table: str, config: Dict):
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
