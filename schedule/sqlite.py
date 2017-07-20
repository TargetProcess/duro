import json
import sqlite3
from typing import List, Tuple

import arrow
import networkx as nx

from schedule.table_config import parse_table_config


def save_to_db(graph: nx.DiGraph, db_path: str, sql_path: str,
               commit: str) -> Tuple:
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    nodes = dict(graph.nodes(data=True))
    tables_and_queries = [(table, data['contents'],
                           data['interval'],
                           json.dumps(parse_table_config(table, sql_path)))
                          for table, data in nodes.items()]
    updates = save_tables(tables_and_queries, cursor)
    save_commit(commit, cursor)
    mark_deleted_tables(tables_and_queries, cursor)

    connection.commit()
    connection.close()
    return updates


def save_tables(tables_and_queries: List[Tuple], cursor) -> Tuple:
    try:
        updated_tables = 0
        new_tables = 0

        for table, query, interval, config in tables_and_queries:
            if is_already_in_db(table, cursor):
                updated_tables += update_table(table, query, interval, config,
                                               cursor)
            else:
                insert_table(table, query, interval, config, cursor)
                new_tables += 1

        return updated_tables, new_tables

    except sqlite3.OperationalError as e:
        if str(e).startswith('no such table'):
            create_tables_table(cursor)
            return save_tables(tables_and_queries, cursor)
        else:
            raise


def is_already_in_db(table: str, cursor) -> bool:
    return cursor.execute('''SELECT table_name
                    FROM tables
                    WHERE table_name = ?
                    ''', (table,)).fetchone() is not None


def insert_table(table: str, query: str, interval: int, config: str, cursor):
    cursor.execute('''INSERT INTO tables 
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                   (table, query,
                    interval, config,
                    None, 0, 0,
                    1, 0, None))


def should_be_updated(table: str, query: str, interval: int,
                      config: str, cursor) -> bool:
    cursor.execute('''SELECT query, interval, config
                    FROM tables
                    WHERE table_name = ?''', (table, ))
    current = cursor.fetchone()
    if current != (query, interval, config):
        return True
    return False


def update_table(table: str, query: str, interval: int, config: str,
                 cursor) -> int:
    if should_be_updated(table, query, interval, config, cursor):
        cursor.execute('''UPDATE tables 
                        SET query = ?, 
                            interval = ?, 
                            config = ?, 
                            force = 1
                        WHERE table_name = ?''',
                       (query, interval, config, table))
        return cursor.rowcount
    else:
        return 0


def create_tables_table(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS tables
                    (table_name text, query text, 
                    interval integer, config text, 
                    last_created integer, mean real, times_run integer,
                    force integer, started integer, deleted integer);''')


def save_commit(commit: str, cursor):
    if commit is not None:
        try:
            cursor.execute('''INSERT INTO commits VALUES (?, ?)''',
                           (commit, arrow.now().timestamp))
        except sqlite3.OperationalError as e:
            if str(e).startswith('no such table'):
                cursor.execute('''CREATE TABLE IF NOT EXISTS commits
                                            (hash text, processed integer)''')
                save_commit(commit, cursor)
            else:
                raise


def mark_deleted_tables(tables_and_queries: List[Tuple], cursor):
    tables = tuple(table for table, _, _, _ in tables_and_queries)
    cursor.execute(f'''UPDATE tables
                    SET deleted = strftime('%s', 'now')
                    WHERE table_name NOT IN {str(tables)}
                    AND deleted IS NULL''')
