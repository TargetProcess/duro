from create.config import load_config
from create.data_tests import run_tests, load_tests
from create.process import process_and_upload_data
from create.redshift import (create_connection, drop_old_table,
                             replace_old_table, drop_temp_table,
                             create_temp_table)
from create.timestamps import Timestamps
from file_utils import load_processor, load_query
from utils import Table

import argparse


def create_table(table: Table, views_path: str):
    config = load_config(table.name, views_path)
    print(f'Creating {table.name}')

    connection = create_connection()

    processor = load_processor(table.name, views_path)
    if processor:
        process_and_upload_data(table, processor,
                                connection, config,
                                Timestamps(),
                                views_path)
    else:
        create_temp_table(table.name, table.query, config,
                          connection)

    tests_queries = load_tests(table.name, views_path)
    test_results = run_tests(tests_queries, connection)
    if not test_results:
        drop_temp_table(table.name, connection)
        return

    replace_old_table(table.name, connection)
    drop_old_table(table.name, connection)
    connection.close()

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('table', help='table to create', type=str)
    parser.add_argument('--path', '-p', default='../materialized-views/', help='folder containing the views')
    args = parser.parse_args()
    table = Table(args.table, load_query(args.table, args.path), None, None)
    create_table(table, args.path)

