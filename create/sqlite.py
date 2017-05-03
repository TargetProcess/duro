import sqlite3

from create.timestamps import Timestamps
from utils import Table


def load_info(table: str, db: str) -> Table:
    with sqlite3.connect(db) as connection:
        cursor = connection.cursor()
        cursor.execute('''SELECT query, interval, last_created 
                        FROM tables
                        WHERE table_name = ? ''', (table,))
        # noinspection PyArgumentList
        return Table(table, *cursor.fetchone())


def update_last_created(table: str, timestamp: int, db: str):
    with sqlite3.connect(db) as connection:
        cursor = connection.cursor()
        cursor.execute('''UPDATE tables 
                        SET last_created = ?
                        WHERE table_name = ? ''', (timestamp, table))


def log_timestamps(table: str, db: str, timestamps: Timestamps):
    with sqlite3.connect(db) as connection:
        question_marks = f'{"?, " * len(timestamps.values)} ?'
        try:
            connection.execute(f'''INSERT INTO timestamps
            VALUES ({question_marks})''',
                               (table, *timestamps.values))
        except sqlite3.OperationalError:
            connection.execute(f'{build_query_to_create_timestamps_table()}')
            connection.execute(f'''INSERT INTO timestamps
                        VALUES ({question_marks})''',
                               (table, *timestamps.values))


def build_query_to_create_timestamps_table():
    events = [f'"{event}" int' for event in Timestamps.__slots__]
    return f'''CREATE TABLE IF NOT EXISTS timestamps 
            ("table" text, 
            {",".join(events)}
            )'''
