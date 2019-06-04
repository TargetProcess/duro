import csv
import gzip
import importlib.machinery
from os import makedirs
from typing import List, Dict, Tuple, Callable

import arrow
import psycopg2
import boto3

from create.timestamps import Timestamps
from credentials import s3_credentials
from utils.errors import ProcessorNotFoundError, RedshiftCopyError
from utils.file_utils import load_ddl_query
from utils.logger import log_action
from utils.table import Table, temp_postfix


def process_and_upload_data(
    table: Table, processor_name: str, connection, ts: Timestamps, views_path: str
) -> int:
    data = select_data(table.query, connection)
    ts.log("select")

    processing_function, columns = load_processor(processor_name)
    processed_data, columns = process_data(data, processing_function, columns)
    ts.log("process")

    folder = s3_credentials()["folder"]
    current_time = arrow.now().strftime("%Y-%m-%d-%H-%M")
    filename = f"{folder}/{table}-{current_time}.csv.gzip"

    makedirs(folder, exist_ok=True)
    save_to_csv(processed_data, columns, filename)
    ts.log("csv")

    upload_to_s3(filename)
    ts.log("s3")

    drop_and_create_query = build_drop_and_create_query(table, views_path)
    timestamp = copy_to_redshift(
        filename, table.name, connection, drop_and_create_query
    )
    ts.log("insert")

    return timestamp


@log_action("select data for processing")
def select_data(query: str, connection) -> List[Dict]:
    with connection.cursor() as cursor:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor]


@log_action("load processing function from processor module")
def load_processor(processor: str) -> Tuple:
    try:
        loader = importlib.machinery.SourceFileLoader(processor, processor)

        # pylint: disable=deprecated-method
        processor_module = loader.load_module()
        return processor_module.process, processor_module.columns
    except AttributeError:
        raise ProcessorNotFoundError(processor)


@log_action("process data")
def process_data(
    data: List[Dict], processing_function: Callable, columns: List
) -> Tuple[List[Dict], List]:
    return processing_function(data), columns


@log_action("save processed data to CSV")
def save_to_csv(data: List[Dict], columns: List, filename: str):
    with gzip.open(filename, "wt", newline="") as output_file:
        dict_writer = csv.DictWriter(
            output_file, columns, delimiter=";", escapechar="\\"
        )
        dict_writer.writeheader()
        dict_writer.writerows(data)


@log_action("upload processed data to CSV")
def upload_to_s3(filename: str):
    client = boto3.client(
        "s3",
        aws_access_key_id=s3_credentials()["aws_access_key_id"],
        aws_secret_access_key=s3_credentials()["aws_secret_access_key"],
    )

    client.upload_file(filename, s3_credentials()["bucket"], filename)


@log_action("build query to drop old table and create a new one")
def build_drop_and_create_query(table: Table, views_path: str):
    keys = table.load_dist_sort_keys()
    create_query = load_ddl_query(views_path, table.name).rstrip(";\n")
    if "diststyle" not in create_query:
        create_query += f"{keys.diststyle}\n"
    if "distkey" not in create_query:
        create_query += f"{keys.distkey}\n"
    if "sortkey" not in create_query:
        create_query += f"{keys.sortkey}\n"

    grant_select = table.load_grant_select_statements()
    return f"DROP TABLE IF EXISTS {table.name}{temp_postfix}; {create_query}; {grant_select}"


@log_action("insert processed data into Redshift table")
def copy_to_redshift(
    filename: str, table_name: str, connection, drop_and_create_query: str
) -> int:
    try:
        with connection.cursor() as cursor:
            cursor.execute(drop_and_create_query)
            connection.commit()
            cursor.execute(
                f"""
                COPY {table_name}{temp_postfix} 
                FROM 's3://{s3_credentials()["bucket"]}/{filename}'
                --region 'us-east-1'
                access_key_id '{s3_credentials()["aws_access_key_id"]}'
                secret_access_key '{s3_credentials()["aws_secret_access_key"]}'
                delimiter ';'
                ignoreheader 1
                emptyasnull blanksasnull csv gzip;
            """
            )
            connection.commit()
            return arrow.now().timestamp
    except psycopg2.Error:
        raise RedshiftCopyError(table_name)
