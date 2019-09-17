import time
from typing import List

import arrow
import networkx as nx

from create.sqlite import (
    load_table_details,
    is_running,
    reset_start,
    get_average_completion_time,
    get_time_running,
    is_waiting,
    mark_table_as_not_waiting,
    mark_table_as_waiting,
)
from create.create_table import create_table
from utils.errors import (
    MaterializationError,
    TableNotFoundInGraphError,
    RedshiftConnectionError,
)
from notifications.slack import send_slack_notification
from utils.global_config import GlobalConfig
from utils.logger import setup_logger
from utils.table import Table

logger = setup_logger()


def create_tree(root: str, global_config: GlobalConfig, interval: int = None):
    db = global_config.db_path
    table = load_table_details(db, root)
    table.interval = table.interval or interval

    if not should_be_created(db, table):
        return

    children = list_children_for_table(root, global_config.graph)

    create_children(children, global_config, table)

    try:
        logger.info(f"Creating {table.name}")
        create_table(table, db, global_config.views_path)
    except RedshiftConnectionError as e:
        logger.error(e)
        reset_start(db, table.name)
        send_slack_notification(str(e), str(e))
    except MaterializationError as e:
        logger.error(e)
        reset_start(db, table.name)
        send_slack_notification(str(e), f"Error while creating {table.name}")


def create_children(children: List, global_config: GlobalConfig, table: Table):
    if children:
        logger.info(f"Creating children for {table.name}")

    for child in children:
        mark_table_as_waiting(global_config.db_path, table.name)
        create_tree(child, global_config, table.interval)
        mark_table_as_not_waiting(global_config.db_path, table.name)


def list_children_for_table(root: str, graph: nx.DiGraph) -> List:
    try:
        children = list(graph[root].keys())
        logger.info(f"Children of {root}: {children}")
        return children
    except KeyError as e:
        raise TableNotFoundInGraphError(e)


def should_be_created(db_path: str, table: Table) -> bool:
    waiting, waiting_too_long = is_waiting(db_path, table.name)

    if waiting and not waiting_too_long:
        logger.info(
            f"{table.name} is waiting for its children to be updated, won’t be updated now"
        )
        return False

    if waiting_too_long:
        logger.info(f"{table.name} can’t be waiting for so long, resetting the flag")
        mark_table_as_not_waiting(db_path, table.name)

    if is_running(db_path, table.name):
        logger.info("Already running, waiting till done")
        finished = wait_till_finished(db_path, table.name)

        if finished:
            return False

    if table.force:
        logger.info(f"Force flag is set for {table.name}, will be updated now")
        return True

    if table.last_created is None or table.interval is None:
        logger.info(f"{table.name} is fresh enough, won’t be updated now")
        return True

    time_since_last_created = arrow.now() - arrow.get(table.last_created)
    fresh = (time_since_last_created.total_seconds() / 60) <= table.interval

    if fresh:
        logger.info(f"{table.name} is fresh enough")
        return False

    return True


def wait_till_finished(db_str: str, table: str, timeout: int = 10) -> bool:
    average_time = get_average_completion_time(db_str, table)
    if not average_time:
        logger.info(f"No stats for this table yet, not waiting for completion")
        reset_start(db_str, table)
        return False
    logger.info(f"Average completion time: {average_time}")
    while True:
        time.sleep(timeout)
        time_running = get_time_running(db_str, table)
        if time_running is None:
            logger.info("Waited until completion")
            return True
        if average_time and time_running > average_time * 5:
            logger.info("Can’t be running for so long, resetting")
            reset_start(db_str, table)
            return False
        logger.info(f"Runs for {time_running} seconds")
