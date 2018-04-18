import time
from logging import Logger
from typing import List

import arrow
import networkx as nx

from create.sqlite import (load_info, is_running, reset_start,
                           get_average_completion_time, get_time_running,
                           set_waiting, is_waiting)
from create.table import create_table
from errors import (MaterializationError,
                    TableNotFoundInGraphError)
from notifications.slack import send_slack_notification
from utils.global_config import GlobalConfig
from utils.logger import setup_logger
from utils.utils import Table


def create_tree(root: str, global_config: GlobalConfig,
                interval: int = None, remaining_tables: int = 1):
    tree_logger = setup_logger(f'{root}_tree')

    table = load_info(global_config.db_path, root)

    if table.interval is None and interval is not None:
        tree_logger.info(f'Updating interval for {root}')
        # noinspection PyArgumentList
        table.interval = interval

    if not should_be_created(table, global_config.db_path, tree_logger,
                             remaining_tables):
        return

    children = get_children(root, global_config.graph, tree_logger)

    remaining_tables += len(children)
    tree_logger.info(f'Tables remaining: {remaining_tables}')

    for child in children:
        set_waiting(global_config.db_path, table.name, True)
        create_tree(child, global_config, table.interval, remaining_tables)
        set_waiting(global_config.db_path, table.name, False)
    try:
        tree_logger.info(f'Creating {table.name}')
        create_table(table, global_config.db_path, global_config.views_path,
                     remaining_tables)
    except MaterializationError as e:
        tree_logger.error(e)
        reset_start(global_config.db_path, table.name)
        send_slack_notification(str(e), f'Error while creating {table.name}')


def get_children(root: str, graph: nx.DiGraph, logger: Logger) -> List:
    try:
        children = list(graph[root].keys())
        logger.info(f'Children of {root}: {children}')
        return children
    except KeyError as e:
        raise TableNotFoundInGraphError(e)


def should_be_created(table: Table, db_path: str, logger: Logger,
                      remaining_tables: int) -> bool:
    waiting, waiting_too_long = is_waiting(db_path, table.name)
    if waiting and not waiting_too_long:
        logger.info(
            f'{table.name} is waiting for its children to be updated, won’t be updated now')
        return False
    if waiting_too_long:
        logger.info('Can’t be waiting for so long, resetting the flag')
        set_waiting(db_path, table.name, False)

    if is_running(db_path, table.name):
        logger.info('Already running, waiting till done')
        finished = wait_till_finished(db_path, table.name, logger)
        if finished:
            remaining_tables -= 1
            logger.info(f'Tables remaining: {remaining_tables}')
            return False

    if table.force:
        logger.info(f'Force flag is set for {table.name}, will be updated now')
        return True

    if table.last_created is None or table.interval is None:
        logger.info(f'{table.name} is fresh enough, won’t be updated now')
        return True

    delta = arrow.now() - arrow.get(table.last_created)
    fresh = (delta.total_seconds() / 60) <= table.interval

    if fresh:
        logger.info(f'{table.name} is fresh enough')
        remaining_tables -= 1
        logger.info(f'Tables remaining: {remaining_tables}')
        return False

    return True


def wait_till_finished(db_str: str, table: str, logger: Logger) -> bool:
    timeout = 10
    average_time = get_average_completion_time(db_str, table)
    logger.info(f'Average completion time: {average_time}')
    while True:
        time.sleep(timeout)
        time_running = get_time_running(db_str, table)
        if time_running is None:
            logger.info('Waited until completion')
            return True
        if time_running > average_time * 5:
            logger.info('Can’t be running for so long, resetting')
            reset_start(db_str, table)
            return False
        logger.info(f'Runs for {time_running} seconds')
