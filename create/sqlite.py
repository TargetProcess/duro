import sqlite3

from create.timestamps import Timestamps
from utils import Table
from typing import List, Tuple


def load_info(table: str, db: str) -> Table:
    with sqlite3.connect(db) as connection:
        cursor = connection.cursor()
        cursor.execute('''SELECT query, interval, last_created 
                        FROM tables
                        WHERE table_name = ? ''', (table,))
        # noinspection PyArgumentList
        return Table(table, *cursor.fetchone())


def update_last_created(db: str, table: str, timestamp: int, duration: int):
    with sqlite3.connect(db) as connection:
        cursor = connection.cursor()
        cursor.execute('''UPDATE tables 
                        SET last_created = ?,
                            mean = 
                                (CASE WHEN times_run IS NOT NULL AND mean IS NOT NULL 
                                    THEN (mean * times_run + ?) / (times_run + 1)
                                ELSE ? END),
                            times_run = 
                                (CASE WHEN times_run IS NOT NULL THEN times_run + 1
                                ELSE 1 END),
                            started = NULL
                        WHERE table_name = ? ''',
                       (timestamp, duration, duration, table))


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


def log_start(table: str, db: str, start_ts: int):
    with sqlite3.connect(db) as connection:
        connection.execute(f'''UPDATE tables SET started = ?
                        WHERE table_name = ?''',
                           (start_ts, table))


def is_running(table: str, db: str) -> bool:
    with sqlite3.connect(db) as connection:
        cursor = connection.cursor()
        cursor.execute(f'''SELECT started 
                    FROM tables
                    WHERE table_name = ?''',
                       (table,))
        result = cursor.fetchone()
        return result[0] if result else False


def build_query_to_create_timestamps_table():
    events = [f'"{event}" int' for event in Timestamps.__slots__]
    return f'''CREATE TABLE IF NOT EXISTS timestamps 
            ("table" text, 
            {",".join(events)}
            )'''


def get_tables_to_create(db: str) -> List[Tuple]:
    print('Getting tables')
    with sqlite3.connect(db) as connection:
        cursor = connection.cursor()
        cursor.execute(f'''SELECT table_name, force_tree 
            FROM tables
            WHERE (force = 1 
            OR force_tree = 1)
            --OR (strftime('%s', 'now') - last_created) / 60 - interval > 0
            --OR last_created IS NULL
            AND deleted IS NULL
                            ''')
        return cursor.fetchall()
