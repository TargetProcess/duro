import asyncio
from async_timeout import timeout

from create.data_tests import load_tests, run_tests
from create.process import process_and_upload_data
from create.redshift import (
    drop_old_table,
    drop_temp_table,
    replace_old_table,
    create_temp_table,
    create_connection,
    make_snapshot,
)
from create.sqlite import (
    update_last_created,
    log_timestamps,
    log_start,
    get_average_completion_time,
)
from create.timestamps import Timestamps
from utils.errors import TestsFailedError, QueryTimeoutError
from utils.file_utils import load_processor
from utils.logger import setup_logger
from utils.table import Table


def run_create_table(table: Table, db_path: str, views_path: str):
    asyncio.run(run_with_timeout(table, db_path, views_path))


async def run_with_timeout(table: Table, db_path: str, views_path: str):
    timeout_length = 5 * get_average_completion_time(db_path, table.name)
    print(f"timeout is {timeout_length}")
    try:
        async with timeout(timeout_length):
            await create_table(table, db_path, views_path)
    except asyncio.TimeoutError:
        raise QueryTimeoutError(table, timeout_length)


# pylint: disable=no-member
# noinspection PyUnresolvedReferences
async def create_table(table: Table, db_path: str, views_path: str):
    logger = setup_logger(table.name)
    ts = Timestamps()
    ts.log("start")

    log_start(db_path, table.name, ts.start)
    logger.info(f"Creating {table.name} with interval {table.interval}")

    connection = create_connection()
    ts.log("connect")

    processor = load_processor(views_path, table.name)
    if processor:
        process_and_upload_data(table, processor, connection, ts, views_path)
    else:
        create_temp_table(table, connection)
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

    if table.store_snapshots:
        made_snapshot = make_snapshot(table, connection)
        if made_snapshot:
            ts.log("make_snapshot")

    connection.close()

    update_last_created(db_path, table.name, ts.start, ts.duration)
    log_timestamps(db_path, table.name, ts)
