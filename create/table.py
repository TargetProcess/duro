from create.config import load_table_config
from create.data_tests import load_tests, run_tests
from create.process import process_and_upload_data
from create.redshift import (drop_old_table, drop_temp_table, replace_old_table,
                             create_temp_table, create_connection)
from create.sqlite import (update_last_created, log_timestamps,
                           log_start)
from create.timestamps import Timestamps
from utils.file_utils import load_processor
from utils.logger import setup_logger
from utils.utils import Table


def create_table(table: Table, db_path: str, views_path: str,
                 remaining_tables: int):
    logger = setup_logger(table.name)
    ts = Timestamps()
    ts.log('start')
    # noinspection PyUnresolvedReferences
    log_start(table.name, db_path, ts.start)
    config = load_table_config(table.name, views_path)
    logger.info(f'Creating {table.name} with interval {table.interval}')

    connection = create_connection()
    ts.log('connect')

    processor = load_processor(table.name, views_path)
    if processor:
        creation_timestamp = process_and_upload_data(table, processor,
                                                     connection, config, ts,
                                                     views_path, logger)
    else:
        creation_timestamp = create_temp_table(table.name, table.query, config,
                                               connection, logger)
        ts.log('create_temp')

    tests_queries = load_tests(table.name, views_path, logger)
    test_results = run_tests(tests_queries, connection, logger)
    ts.log('tests')
    if not test_results:
        drop_temp_table(table.name, connection, logger)
        return

    replace_old_table(table.name, connection, logger)
    ts.log('replace_old')
    drop_old_table(table.name, connection, logger)
    ts.log('drop_old')
    connection.close()

    update_last_created(db_path, table.name, creation_timestamp, ts.duration)
    log_timestamps(table.name, db_path, ts)
    remaining_tables -= 1
    logger.info(f'Tables remaining: {remaining_tables}')
