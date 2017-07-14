import configparser
import sys
from datetime import datetime
from typing import List
import time

import arrow
import networkx as nx

from create.config import load_config
from create.data_tests import load_tests, run_tests
from create.process import process_and_upload_data
from create.redshift import (drop_old_table, drop_temp_table, replace_old_table,
                             create_temp_table, create_connection)
from create.sqlite import (load_info, update_last_created, log_timestamps,
                           log_start, is_running, reset_start,
                           get_tables_to_create, propagate_force_flag)
from create.timestamps import Timestamps
from errors import (TableNotFoundError, MaterializationError)
from file_utils import load_processor
from utils import Table
from global_config import GlobalConfig, load_global_config


tables_to_create_count = 0


def get_children(root: str, graph: nx.DiGraph) -> List:
    try:
        children = list(graph[root].keys())
        print(f'Children of {root}: {children}')
        return children
    except KeyError:
        raise TableNotFoundError(
            'There’s no table with this name in dependencies graph.')


def create_table(table: Table, db_path: str, views_path: str,
                 force: bool = False):
    global tables_to_create_count

    ts = Timestamps()
    ts.log('start')
    # noinspection PyUnresolvedReferences
    # log_start(table.name, db_path, ts.start)
    config = load_config(table.name, views_path)
    print(f'Creating {table.name} with interval {table.interval}')

    connection = create_connection()
    ts.log('connect')

    processor = load_processor(table.name, views_path)
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

    update_last_created(db_path, table.name, creation_timestamp, ts.duration, force)
    log_timestamps(table.name, db_path, ts)
    tables_to_create_count -= 1
    print(f'Tables remaining: {tables_to_create_count}')


def wait_till_finished(table: str, db: str):
    timeout = 10
    while is_running(table, db):
        print(f'Waiting for {timeout} seconds')
        time.sleep(timeout)
        # timeout += 10


def should_be_created(table: Table, db_path: str, force: bool) -> bool:
    global tables_to_create_count

    if is_running(table.name, db_path):
        print('Already running, waiting till done')
        wait_till_finished(table.name, db_path)
        tables_to_create_count -= 1
        print(f'Tables remaining: {tables_to_create_count}')
        return False

    if force:
        return True

    if table.last_created is None or table.interval is None:
        return True

    delta = arrow.now() - arrow.get(table.last_created)
    fresh = (delta.total_seconds() / 60) <= table.interval

    if fresh:
        print(f'{table.name} is fresh enough')
        tables_to_create_count -= 1
        print(f'Tables remaining: {tables_to_create_count}')
        return False
    else:
        return True


def create_tree(root: str, global_config: GlobalConfig,
                interval: int = None, force_tree: bool = False):
    global tables_to_create_count

    table = load_info(root, global_config.db_path)

    if table.interval is None and interval is not None:
        print(f'Updating interval for {root}')
        # noinspection PyArgumentList
        table = Table(table.name, table.query, interval, table.last_created)

    if not should_be_created(table, global_config.db_path, force_tree):
        return

    children = get_children(root, global_config.graph)
    if force_tree:
        propagate_force_flag(table.name, global_config.db_path, global_config.graph)

    tables_to_create_count += len(children)
    print(f'Tables remaining: {tables_to_create_count}')

    handle_cycles(force_tree, global_config, table)

    for child in children:
        create_tree(child, global_config, table.interval, force_tree)
    try:
        create_table(table, global_config.db_path, global_config.views_path, force_tree)
    except MaterializationError as e:
        print(e)
        reset_start(table.name, global_config.db_path)


def handle_cycles(force_tree: bool, global_config: GlobalConfig, table: Table):
    if table.name == 'satisfaction.companies':
        create_table(table, global_config.db_path, global_config.views_path,
                     force_tree)


def create(root_table: str, force_tree: bool = False):
    global tables_to_create_count
    tables_to_create_count += 1
    create_tree(root_table, load_global_config(), force_tree=force_tree)


if __name__ == '__main__':
    # while True:
    #     new_tables = get_tables_to_create('./duro.db')
    #     print(datetime.now(), len(new_tables), 'new tables')
    #     print(new_tables)
    #     for t, _ in new_tables:
    #         create(t)
    #     time.sleep(30)
    new_tables = get_tables_to_create('./duro.db')
    print(len(new_tables))
    print(new_tables)