import sqlite3

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

