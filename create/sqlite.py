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
        return cursor.fetchone()[0]


def build_query_to_create_timestamps_table():
    events = [f'"{event}" int' for event in Timestamps.__slots__]
    return f'''CREATE TABLE IF NOT EXISTS timestamps 
            ("table" text, 
            {",".join(events)}
            )'''
