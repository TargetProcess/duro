from duro.create.data_tests import load_tests, run_tests
from duro.create.process import process_and_upload_data
from duro.create.redshift import (
    drop_old_table,
    drop_temp_table,
    replace_old_table,
    create_temp_table,
    create_connection,
)
from duro.create.sqlite import update_last_created, log_timestamps, log_start
from duro.create.timestamps import Timestamps
from utils.errors import TestsFailedError
from duro.utils.file_utils import load_processor
from duro.utils.logger import setup_logger
from duro.utils.utils import Table


# pylint: disable=no-member
# noinspection PyUnresolvedReferences
def create_table(table: Table, db_path: str, views_path: str, remaining_tables: int):
    logger = setup_logger(table.name)
    ts = Timestamps()
    ts.log("start")

    log_start(db_path, table.name, ts.start)
    logger.info(f"Creating {table.name} with interval {table.interval}")

    connection = create_connection()
    ts.log("connect")

    processor = load_processor(views_path, table.name)
    if processor:
        creation_timestamp = process_and_upload_data(
            table, processor, connection, ts, views_path
        )
    else:
        creation_timestamp = create_temp_table(table, connection)
        ts.log("create_temp")

    tests = load_tests(table.name, views_path)
    test_results, failed_tests = run_tests(tests, connection)
    ts.log("tests")

    if not test_results:
        drop_temp_table(table.name, connection)
        raise TestsFailedError(table.name, failed_tests)

    replace_old_table(table.name, connection)
    ts.log("replace_old")
    drop_old_table(table.name, connection)
    ts.log("drop_old")
    connection.close()

    update_last_created(db_path, table.name, creation_timestamp, ts.duration)
    log_timestamps(db_path, table.name, ts)
    remaining_tables -= 1
    logger.info(f"Tables remaining: {remaining_tables}")
