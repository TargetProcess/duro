import csv
import gzip
import os
import subprocess
from typing import List, Dict, Tuple

import arrow
import psycopg2
import boto3

from create.timestamps import Timestamps
from credentials import s3_credentials
from utils.errors import RedshiftCopyError, ProcessorRunError
from utils.file_utils import load_ddl_query, find_requirements_txt
from utils.logger import log_action
from utils.table import Table, temp_postfix


def process_and_upload_data(
    table: Table, processor_path: str, connection, ts: Timestamps, views_path: str
):
    data = select_data(table.query, connection)

    folder = s3_credentials()["folder"]
    os.makedirs(folder, exist_ok=True)
    selected, processed = build_filenames(folder, table.name)
    save_selected_to_csv(data, selected)
    ts.log("select")

    run_processor(views_path, processor_path, table.name, selected, processed)
    ts.log("process")

    upload_to_s3(processed)
    ts.log("s3")

    os.remove(selected)
    os.remove(processed)

    drop_and_create_query = build_drop_and_create_query(table, views_path)
    copy_to_redshift(processed, table.name, connection, drop_and_create_query)
    ts.log("insert")


@log_action("select data for processing")
def select_data(query: str, connection) -> List[Dict]:
    with connection.cursor() as cursor:
        cursor.execute(query)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor]


def build_filenames(folder: str, table_name: str) -> Tuple[str, str]:
    current_time = arrow.now().strftime("%Y-%m-%d-%H-%M")
    selected_filename = f"{folder}/{table_name}_select-{current_time}.csv"
    processed_filename = f"{folder}/{table_name}-{current_time}.csv"
    return selected_filename, processed_filename


@log_action("create virtual environment and run processor")
def run_processor(
    views_path: str,
    processor_path: str,
    table_name: str,
    selected_filename: str,
    processed_filename: str,
):
    venv_path = f"./venvs/{table_name}"
    subprocess.run(["python", "-m", "venv", venv_path])
    requirements = find_requirements_txt(views_path, table_name)
    if requirements:
        subprocess.run([f"{venv_path}/bin/pip", "install", "-r", requirements])
    run_result = subprocess.run(
        [
            f"{venv_path}/bin/python",
            processor_path,
            selected_filename,
            processed_filename,
        ]
    )

    if run_result.returncode != 0:
        error_message = f"""Failed run for {venv_path}/bin/python/{processor_path}
        stderr: 
        {run_result.stderr}
        stdout:
        {run_result.stdout}"""
        raise ProcessorRunError(table_name, error_message)


@log_action("save selected data to CSV")
def save_selected_to_csv(data: List[Dict], filename: str):
    columns = data[0].keys()
    with open(filename, "w") as output_file:
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
):
    try:
        with connection.cursor() as cursor:
            cursor.execute(drop_and_create_query)
            connection.commit()
            cursor.execute(
                f"""
                COPY {table_name}{temp_postfix} 
                FROM 's3://{s3_credentials()["bucket"]}/{filename}'
                access_key_id '{s3_credentials()["aws_access_key_id"]}'
                secret_access_key '{s3_credentials()["aws_secret_access_key"]}'
                delimiter ';'
                ignoreheader 1
                emptyasnull blanksasnull csv;
            """
            )

            connection.commit()
    except psycopg2.Error:
        raise RedshiftCopyError(table_name)
