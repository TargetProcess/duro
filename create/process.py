import csv
import gzip
import importlib.machinery
import arrow
from typing import List, Dict, Tuple

import psycopg2
import tinys3

from create.config import load_dist_sort_keys, add_grant_select_statements
from create.timestamps import Timestamps
from credentials import s3_credentials
from file_utils import load_create_query
from utils import Table
from errors import ProcessorNotFoundError, RedshiftUploadError


def process_and_upload_data(table: Table, processor: str, connection,
                            config: Dict, ts: Timestamps,
                            views_path: str) -> int:
    data = select_data(table.query, connection)
    ts.log('select')
    processed_data, columns = process_data(data, processor)
    ts.log('process')
    return upload_to_temp_table(processed_data, columns,
                                table.name, config,
                                connection, ts, views_path)


def select_data(query: str, connection) -> List[Dict]:
    print('Selecting data')
    with connection.cursor() as cursor:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor]


def load_processor(processor: str) -> Tuple:
    try:
        loader = importlib.machinery.SourceFileLoader(f'{processor}.py',
                                                      f'{processor}.py')
        processor_module = loader.load_module()
        return processor_module.process, processor_module.columns
    except AttributeError:
        raise ProcessorNotFoundError(f'Could’t load a processor from {processor}.py')


def process_data(data: List[Dict], processor: str) -> Tuple[List[Dict], List]:
    print('Loading processor')
    process, columns = load_processor(processor)
    print('Processing data')
    return process(data), columns


def upload_to_temp_table(data: List[Dict], columns: List,
                         table: str, config: Dict,
                         connection, ts: Timestamps, views_path: str) -> int:
    filename = f'{s3_credentials()["folder"]}/{table}-{arrow.now().strftime("%Y-%m-%d-%H-%M")}.csv.gzip'
    print(f'Saving to CSV')
    save_to_csv(data, columns, filename)
    ts.log('csv')

    print(f'Uploading to S3')
    upload_to_s3(filename)
    ts.log('s3')

    drop_and_create_query = build_drop_and_create_query(table, config,
                                                        views_path)
    print(f'Copying {table} to Redshift')
    timestamp = copy_to_redshift(filename, table, connection,
                                 drop_and_create_query)
    ts.log('insert')

    return timestamp


def save_to_csv(data: List[Dict], columns: List, filename: str):
    with gzip.open(filename, 'wt', newline='') as output_file:
        dict_writer = csv.DictWriter(output_file,
                                     columns,
                                     delimiter=';', escapechar='\\')
        dict_writer.writeheader()
        dict_writer.writerows(data)


def upload_to_s3(filename: str):
    connection = tinys3.Connection(s3_credentials()['aws_access_key_id'],
                                   s3_credentials()['aws_secret_access_key'])
    with open(filename, 'rb') as f:
        connection.upload(filename, f, s3_credentials()['bucket'])


def build_drop_and_create_query(table: str, config: Dict, views_path: str):
    keys = load_dist_sort_keys(table, config)
    create_query = load_create_query(table, views_path).rstrip(';\n')
    if 'diststyle' not in create_query:
        create_query += f'{keys.diststyle}\n'
    if 'distkey' not in create_query:
        create_query += f'{keys.distkey}\n'
    if 'sortkey' not in create_query:
        create_query += f'{keys.sortkey}\n'

    create_query = add_grant_select_statements(table, create_query, config)
    return f'DROP TABLE IF EXISTS {table}_temp; {create_query}'


def copy_to_redshift(filename: str, table: str, connection,
                     drop_and_create_query: str) -> int:
    try:
        with connection.cursor() as cursor:
            cursor.execute(drop_and_create_query)
            connection.commit()
            cursor.execute(f'''COPY {table}_temp FROM 's3://{s3_credentials()["bucket"]}/{filename}'
            --region 'us-east-1'
            access_key_id '{s3_credentials()["aws_access_key_id"]}'
            secret_access_key '{s3_credentials()["aws_secret_access_key"]}'
            delimiter ';'
            ignoreheader 1
            emptyasnull blanksasnull csv gzip;''')
            connection.commit()
            return arrow.now().timestamp
    except psycopg2.Error:
        raise RedshiftUploadError


def remove_csv_files(filename: str):
    pass
