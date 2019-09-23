from typing import List

import networkx as nx

from scheduler.table_config import check_config_fields
from utils.errors import RootsWithoutIntervalError, SchedulerError, TablesWithoutRequiredFiles
from notifications.slack import send_slack_notification
from scheduler.commits import get_all_commits, get_latest_new_commit
from scheduler.graph import build_graph, save_graph_to_file, check_for_cycles
from scheduler.sqlite import save_to_db, build_table_configs
from scheduler.checks import find_tables_with_missing_files
from utils.file_utils import load_tables_in_path
from utils.global_config import load_global_config
from utils.graph_utils import find_roots_without_interval
from utils.logger import setup_logger

logger = setup_logger("scheduler")


def check_for_missing_intervals(graph: nx.DiGraph):
    roots_without_interval = find_roots_without_interval(graph)

    if roots_without_interval:
        error = f"Some roots don’t have an interval specified. These roots are: {', '.join(roots_without_interval)}"
        logger.error(error)
        raise RootsWithoutIntervalError(error)


def check_for_missing_files(views_path: str):
    missing_file_errors = find_tables_with_missing_files(views_path)
    if missing_file_errors:
        logger.error(missing_file_errors)
        raise TablesWithoutRequiredFiles(missing_file_errors)


def build_notification_message(new: List, updated: List) -> str:
    new_str = f'New tables: {", ".join(new)}. ' if new else ""
    updated_str = f'Updated tables: {", ".join(updated)}.' if updated else ""
    return f"{new_str}{updated_str}"


def schedule(views_path: str, db_path: str, strict=False, use_git=False):
    latest_commit = None

    if use_git:
        commits = get_all_commits(views_path)
        latest_commit = get_latest_new_commit(commits, db_path)
        if latest_commit is None:
            logger.info("No new commits")
            return

    check_for_missing_files(views_path)

    tables_list = load_tables_in_path(views_path)

    graph = build_graph(tables_list)
    logger.info(f"Built graph for {views_path}")

    save_graph_to_file(graph)
    logger.info(f"Saved graph to file")

    if strict:
        check_for_cycles(graph, logger)
    check_for_missing_intervals(graph)

    tables_and_configs = build_table_configs(graph, views_path)
    check_config_fields(tables_and_configs)

    new, updated = save_to_db(db_path, tables_and_configs, latest_commit)
    message = build_notification_message(new, updated)

    if use_git:
        logger.info(f"Rescheduled for commit {latest_commit}. {message}")
    else:
        logger.info(f"Rescheduled. {message}")

    if updated or new:
        send_slack_notification(message, "Rescheduled views", message_type="success")


if __name__ == "__main__":
    global_config = load_global_config()
    try:
        schedule(
            global_config.views_path,
            global_config.db_path,
            strict=False,
            use_git=global_config.use_git,
        )
    except SchedulerError as e:
        send_slack_notification(str(e), "Scheduler error")
        logger.error("Couldn‘t build a schedule for this views folder")
