import csv
import importlib.machinery
import arrow
from typing import List, Dict

import tinys3

from create.config import load_dist_sort_keys
from create.timestamps import Timestamps
from credentials import s3_credentials
from file_utils import load_create_query
from utils import Table


def process_and_upload_data(table: Table, processor: str, connection,
                            config: Dict, ts: Timestamps,
                            views_path: str) -> int:
    data = select_data(table.query, connection)
    ts.log('select')
    processed_data = process_data(data, processor)
    ts.log('process')
    return upload_to_temp_table(processed_data,
                                table.name, config,
                                connection, ts, views_path)


def select_data(query: str, connection) -> List[Dict]:
    print('Selecting data')
    with connection.cursor() as cursor:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor]


def process_data(data: List[Dict], processor: str) -> List[Dict]:
    print('Loading processor')
    loader = importlib.machinery.SourceFileLoader(f'{processor}.py', f'{processor}.py')
    processor_module = loader.load_module()
    print('Processing data')
    return processor_module.process(data)


def upload_to_temp_table(data: List[Dict], table: str, config: Dict,
                         connection, ts: Timestamps, views_path: str) -> int:
    filename = f'{s3_credentials()["folder"]}/{table}-{arrow.now().strftime("%Y-%m-%d-%H-%M")}.csv'
    print(f'Saving to CSV')
    save_to_csv(data, filename)
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


def save_to_csv(data: List[Dict], filename: str):
    with open(filename, 'w') as output_file:
        dict_writer = csv.DictWriter(output_file, data[0].keys(),
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
    return create_query


def copy_to_redshift(filename: str, table: str, connection,
                     drop_and_create_query: str) -> int:
    with connection.cursor() as cursor:
        cursor.execute(drop_and_create_query)
        connection.commit()
        cursor.execute(f'''COPY {table} FROM 's3://{s3_credentials()["bucket"]}/{filename}'
        --region 'us-east-1'
        access_key_id '{s3_credentials()["aws_access_key_id"]}'
        secret_access_key '{s3_credentials()["aws_secret_access_key"]}'
        delimiter ';'
        ignoreheader 1
        emptyasnull blanksasnull csv;''')
        connection.commit()
        return arrow.now().timestamp


def remove_csv_files(filename: str):
    pass
