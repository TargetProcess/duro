import time
from logging import Logger
from typing import List
from utils.logger import setup_logger

import arrow
import networkx as nx

from create.sqlite import (load_info, is_running, reset_start)
from create.table import create_table
from errors import (TableNotFoundError, MaterializationError)
from utils.global_config import GlobalConfig
from utils.utils import Table


def create_tree(root: str, global_config: GlobalConfig,
                interval: int = None, remaining_tables: int = 1):
    tree_logger = setup_logger(f'{root}_tree')

    table = load_info(root, global_config.db_path)

    if table.interval is None and interval is not None:
        tree_logger.info(f'Updating interval for {root}')
        # noinspection PyArgumentList
        table = Table(table.name, table.query, interval, table.config,
                      table.last_created, table.force)

    if not should_be_created(table, global_config.db_path, tree_logger,
                             remaining_tables):
        return

    children = get_children(root, global_config.graph, tree_logger)

    remaining_tables += len(children)
    tree_logger.info(f'Tables remaining: {remaining_tables}')

    handle_cycles(global_config, table, remaining_tables)

    for child in children:
        create_tree(child, global_config, table.interval, remaining_tables)
    try:
        create_table(table, global_config.db_path, global_config.views_path, remaining_tables)
    except MaterializationError as e:
        tree_logger.error(e)
        reset_start(table.name, global_config.db_path)


def get_children(root: str, graph: nx.DiGraph, logger: Logger) -> List:
    try:
        children = list(graph[root].keys())
        logger.info(f'Children of {root}: {children}')
        return children
    except KeyError:
        raise TableNotFoundError(
            'There’s no table with this name in dependencies graph.')


def should_be_created(table: Table, db_path: str, logger: Logger,
                      remaining_tables: int) -> bool:
    if is_running(table.name, db_path):
        logger.info('Already running, waiting till done')
        wait_till_finished(table.name, db_path, logger)
        remaining_tables -= 1
        logger.info(f'Tables remaining: {remaining_tables}')
        return False

    if table.force:
        return True

    if table.last_created is None or table.interval is None:
        return True

    delta = arrow.now() - arrow.get(table.last_created)
    fresh = (delta.total_seconds() / 60) <= table.interval

    if fresh:
        logger.info(f'{table.name} is fresh enough')
        remaining_tables -= 1
        logger.info(f'Tables remaining: {remaining_tables}')
        return False
    else:
        return True


def handle_cycles(global_config: GlobalConfig, table: Table, remaining_tables: int):
    if table.name == 'satisfaction.companies':
        create_table(table, global_config.db_path, global_config.views_path, remaining_tables)


def wait_till_finished(table: str, db: str, logger: Logger):
    timeout = 10
    while is_running(table, db):
        logger.info(f'Waiting for {timeout} seconds')
        time.sleep(timeout)
