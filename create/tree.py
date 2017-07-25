import time
from logging import Logger
from typing import List

import arrow
import networkx as nx

from create.sqlite import (load_info, is_running, reset_start,
                           get_average_completion_time, get_time_running)
from create.table import create_table
from errors import (TableNotFoundInDBError, MaterializationError)
from notifications.slack import send_slack_notification
from utils.global_config import GlobalConfig
from utils.logger import setup_logger
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
        tree_logger.info(f'Creating {table.name}')
        create_table(table, global_config.db_path, global_config.views_path,
                     remaining_tables)
    except MaterializationError as e:
        tree_logger.error(e)
        reset_start(table.name, global_config.db_path)
        send_slack_notification(str(e), f'Error while creating {e.table}')


def get_children(root: str, graph: nx.DiGraph, logger: Logger) -> List:
    try:
        children = list(graph[root].keys())
        logger.info(f'Children of {root}: {children}')
        return children
    except KeyError as e:
        raise TableNotFoundInDBError(e)


def should_be_created(table: Table, db_path: str, logger: Logger,
                      remaining_tables: int) -> bool:
    if is_running(table.name, db_path):
        logger.info('Already running, waiting till done')
        finished = wait_till_finished(table.name, db_path, logger)
        if finished:
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


def handle_cycles(global_config: GlobalConfig, table: Table,
                  remaining_tables: int):
    if table.name == 'satisfaction.companies':
        create_table(table, global_config.db_path, global_config.views_path,
                     remaining_tables)


def wait_till_finished(table: str, db: str, logger: Logger) -> bool:
    timeout = 10
    average_time = get_average_completion_time(table, db)
    logger.info(f'Average completion time: {average_time}')
    while True:
        time.sleep(timeout)
        time_running = get_time_running(table, db)
        if time_running is None:
            logger.info('Waited until completion')
            return True
        if time_running > average_time * 5:
            logger.info('Can’t be running for so long, resetting')
            reset_start(table, db)
            return False
        logger.info(f'Runs for {time_running} seconds')
