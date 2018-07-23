import argparse

from create.data_tests import run_tests, load_tests
from create.process import process_and_upload_data
from create.redshift import (
    create_connection,
    drop_old_table,
    replace_old_table,
    drop_temp_table,
    create_temp_table,
)
from create.timestamps import Timestamps
from scheduler.table_config import parse_table_config
from utils.file_utils import load_processor, load_select_query
from utils.logger import setup_logger
from utils.utils import Table


def create_table(table: Table, views_path: str, verbose=False):
    logger = setup_logger(stdout=True)
    logger.info(f"Creating {table.name}")
    if verbose:
        logger.info(f"Using views path: {views_path}")

    connection = create_connection()

    processor = load_processor(views_path, table.name)

    if verbose:
        logger.info(f"Loaded processor: {processor}")
    if processor:
        process_and_upload_data(table, processor, connection, Timestamps(), views_path)
    else:
        create_temp_table(table, connection)

    tests_queries = load_tests(table.name, views_path)
    test_results, _ = run_tests(tests_queries, connection)
    if not test_results:
        drop_temp_table(table.name, connection)
        return

    replace_old_table(table.name, connection)
    drop_old_table(table.name, connection)
    connection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("table", help="table to create", type=str)
    parser.add_argument(
        "--path", "-p", default="./views/", help="folder containing the views"
    )
    parser.add_argument(
        "--verbose", "-v", default=False, help="Verbose", action="store_true"
    )
    args = parser.parse_args()

    table = Table(
        name=args.table,
        query=load_select_query(args.path, args.table),
        interval=None,
        config=parse_table_config(args.table, args.path),
    )
    create_table(table, args.path, args.verbose)
