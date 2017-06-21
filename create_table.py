import configparser
import sys
from typing import List

import arrow
import networkx as nx
import psycopg2

from create.config import load_config
from create.data_tests import load_tests, run_tests
from create.process import process_and_upload_data
from create.redshift import (drop_old_table, drop_temp_table, replace_old_table,
                             create_temp_table)
from create.sqlite import (load_info, update_last_created, log_timestamps,
                           log_start, is_running)
from create.timestamps import Timestamps
from credentials import redshift_credentials
from errors import TableNotFoundError, MaterializationError
from file_utils import get_processor
from utils import GlobalConfig, Table

tables_to_create_count = 1


def get_children(root: str, graph: nx.DiGraph) -> List:
    global tables_to_create_count
    try:
        children = list(graph[root].keys())
        tables_to_create_count += len(children)
        print(f'Children of {root}: {children}')
        print(f'Tables remaining: {tables_to_create_count}')
        return children
    except KeyError:
        raise TableNotFoundError(
            'There’s no table with this name in dependencies graph.')


def create_table(table: Table, db_path: str, views_path: str,
                 force: bool = False):
    global tables_to_create_count
    if not should_be_created(table.interval, table.last_created, force,
                             is_running(table.name, db_path)):
        print(f'{table.name} is fresh enough')
        tables_to_create_count -= 1
        print(f'Tables remaining: {tables_to_create_count}')
        return

    ts = Timestamps()
    ts.log('start')
    # noinspection PyUnresolvedReferences
    log_start(table.name, db_path, ts.start)
    config = load_config(table.name, views_path)
    print(f'Creating {table.name} with interval {table.interval}')

    connection = create_connection()
    ts.log('connect')

    processor = get_processor(table.name, views_path)
    if processor:
        creation_timestamp = process_and_upload_data(table, processor,
                                                     connection, config, ts,
                                                     views_path)
    else:
        creation_timestamp = create_temp_table(table.name, table.query, config,
                                               connection)
        ts.log('create_temp')

    tests_queries = load_tests(table.name, views_path)
    test_results = run_tests(tests_queries, connection)
    ts.log('tests')
    if not test_results:
        drop_temp_table(table.name, connection)
        return

    replace_old_table(table.name, connection)
    ts.log('replace_old')
    drop_old_table(table.name, connection)
    ts.log('drop_old')
    connection.close()

    update_last_created(db_path, table.name, creation_timestamp, ts.duration)
    log_timestamps(table.name, db_path, ts)
    tables_to_create_count -= 1
    print(f'Tables remaining: {tables_to_create_count}')


def should_be_created(interval: int, last_created: int, force: bool, already_running: bool) -> bool:
    return True
    if already_running:
        return False
    if force:
        return True
    if last_created is None or interval is None:
        return True

    delta = arrow.now() - arrow.get(last_created)
    return (delta.total_seconds() / 60) > interval


def create_connection():
    connection = psycopg2.connect(**redshift_credentials())
    connection.autocommit = True
    return connection


def load_global_config() -> GlobalConfig:
    try:
        config = configparser.ConfigParser()
        config.read('config.conf')
        db_path = config['main'].get('db', './duro.db')
        views_path = config['main'].get('views', './views')
        graph_file_path = config['main'].get('graph', 'dependencies.dot')
        graph = nx.nx_pydot.read_dot(graph_file_path)
        # noinspection PyArgumentList
        return GlobalConfig(db_path, views_path, graph)
    except configparser.NoSectionError:
        print('No ’main’ section in config.conf')
        sys.exit(1)


def create_tree(root: str, global_config: GlobalConfig,
                interval: int = None, force_tree: bool = False):
    children = get_children(root, global_config.graph)
    table = load_info(root, global_config.db_path)

    if table.interval is None and interval is not None:
        print(f'Updating interval for {root}')
        # noinspection PyArgumentList
        table = Table(table.name, table.query, interval, table.last_created)

    for child in children:
        create_tree(child, global_config, table.interval, force_tree)
    try:
        create_table(table, global_config.db_path, global_config.views_path, force_tree)
    except MaterializationError as e:
        print(e)


def create(root_table: str, force_tree: bool = False):
    create_tree(root_table, load_global_config(), force_tree=force_tree)


if __name__ == '__main__':
    # main('tauspy.most_active_users')
    # main('tauspy.daily_active_users')
    create('custom.languages', force_tree=True)
    # main('tauspy.vizydrop_description')
    # main('licenses.changes')
    # feedback.contacts
