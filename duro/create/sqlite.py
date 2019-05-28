import json
import sqlite3
from typing import List, Tuple

import arrow

from create.timestamps import Timestamps
from utils.errors import TableNotFoundInDBError
from utils.utils import Table


def load_table_details(db_str: str, table: str) -> Table:
    with sqlite3.connect(db_str) as connection:
        connection.row_factory = sqlite3.Row
        try:
            cursor = connection.cursor()
            cursor.execute(
                """
                SELECT query, interval, 
                    config, last_created, 
                    force, waiting
                FROM tables
                WHERE table_name = ?
            """,
                (table,),
            )
            row = cursor.fetchone()

            # noinspection PyTypeChecker
            return Table(
                name=table,
                query=row["query"],
                interval=row["interval"],
                config=json.loads(row["config"]),
                last_created=row["last_created"],
                force=row["force"],
                waiting=row["waiting"],
            )

        except TypeError:
            raise TableNotFoundInDBError(table)


def update_last_created(db_str: str, table: str, timestamp: int, duration: int):
    with sqlite3.connect(db_str) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            UPDATE tables 
            SET last_created = ?,
                mean = 
                    (CASE 
                        WHEN times_run IS NOT NULL 
                            AND mean IS NOT NULL 
                        THEN (mean * times_run + ?) / (times_run + 1)
                        
                        ELSE ? 
                    END),
                    
                times_run = 
                    (CASE 
                        WHEN times_run IS NOT NULL 
                        THEN times_run + 1
                        
                        ELSE 1 
                    END),
                    
                started = NULL,
                force = NULL,
                waiting = NULL
            WHERE table_name = ? 
            """,
            (timestamp, duration, duration, table),
        )


def log_timestamps(db_str: str, table: str, timestamps: Timestamps):
    with sqlite3.connect(db_str) as connection:
        question_marks = f'{"?, " * len(timestamps.values)} ?'
        try:
            connection.execute(
                f"""
                INSERT INTO timestamps
                VALUES ({question_marks})
            """,
                (table, *timestamps.values),
            )

        except sqlite3.OperationalError:
            connection.execute(f"{build_query_to_create_timestamps_table()}")
            connection.execute(
                f"""
                INSERT INTO timestamps
                VALUES ({question_marks})
            """,
                (table, *timestamps.values),
            )


def log_start(db_str: str, table: str, start_ts: int):
    with sqlite3.connect(db_str) as connection:
        connection.execute(
            """
            UPDATE tables SET started = ?
            WHERE table_name = ?
        """,
            (start_ts, table),
        )


def reset_start(db_str: str, table: str):
    with sqlite3.connect(db_str) as connection:
        connection.execute(
            """
            UPDATE tables SET started = NULL
            WHERE table_name = ?
        """,
            (table,),
        )


def reset_all_starts(db_str: str):
    with sqlite3.connect(db_str) as connection:
        connection.execute("UPDATE tables SET started = NULL")


def mark_table_as_waiting(db_str: str, table: str):
    set_waiting_flag(db_str, table, True)


def mark_table_as_not_waiting(db_str: str, table: str):
    set_waiting_flag(db_str, table, False)


def set_waiting_flag(db_str: str, table: str, waiting: bool):
    with sqlite3.connect(db_str) as connection:
        connection.execute(
            """
            UPDATE tables SET waiting = ?
            WHERE table_name = ?
        """,
            (waiting if not waiting else arrow.now().timestamp, table),
        )


def is_waiting(db_str: str, table: str, threshold: int = 7200) -> Tuple[bool, bool]:
    with sqlite3.connect(db_str) as connection:
        cursor = connection.cursor()
        cursor.execute(
            """
            SELECT waiting 
            FROM tables
            WHERE table_name = ?
        """,
            (table,),
        )

        result = cursor.fetchone()
        if not result or not result[0]:
            return False, False

    time_waiting = get_time_waiting(db_str, table)
    if time_waiting > threshold:
        return True, True

    return True, False


def is_running(db_str: str, table: str) -> bool:
    with sqlite3.connect(db_str) as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""
            SELECT started 
            FROM tables
            WHERE table_name = ?
        """,
            (table,),
        )

        result = cursor.fetchone()
        return bool(result[0]) if result else False


def get_time_running(db_str: str, table: str) -> int:
    with sqlite3.connect(db_str) as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""
            SELECT strftime('%s', 'now') - started 
            FROM tables
            WHERE table_name = ?
        """,
            (table,),
        )
        result = cursor.fetchone()
        return result[0] if result else None


def get_time_waiting(db_str: str, table: str) -> int:
    with sqlite3.connect(db_str) as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""
            SELECT strftime('%s', 'now') - waiting 
            FROM tables
            WHERE table_name = ?
        """,
            (table,),
        )
        result = cursor.fetchone()
        return result[0] if result else None


def get_average_completion_time(db_str: str, table: str) -> int:
    with sqlite3.connect(db_str) as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""
            SELECT mean 
            FROM tables
            WHERE table_name = ?
        """,
            (table,),
        )
        result = cursor.fetchone()
        return result[0] if result else None


def build_query_to_create_timestamps_table():
    events = [f'"{event}" int' for event in Timestamps.__slots__]
    return f"""
        CREATE TABLE IF NOT EXISTS timestamps 
        ("table" text, 
        {", ".join(events)})
    """


def get_tables_to_create(db_str: str) -> List[Tuple]:
    with sqlite3.connect(db_str) as connection:
        cursor = connection.cursor()
        cursor.execute(
            f"""
            SELECT table_name 
            FROM tables
            WHERE 
                (
                    force = 1
                    OR (strftime('%s', 'now') - last_created) / 60 - interval > 0
                    OR last_created IS NULL
                )
                AND deleted IS NULL
            ORDER BY force DESC
        """
        )
        return cursor.fetchall()
