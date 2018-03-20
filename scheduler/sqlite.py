import json
import sqlite3
from typing import List, Tuple, Optional, NamedTuple

import arrow
import networkx as nx

from scheduler.table_config import parse_table_config
from utils.utils import Table


def save_to_db(graph: nx.DiGraph, db_path: str, views_path: str,
               commit: str) -> Tuple:
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()

    tables_and_queries = build_table_configs(graph, views_path)

    updates = save_tables(tables_and_queries, cursor)
    save_commit(commit, cursor)
    mark_deleted_tables(tables_and_queries, cursor)

    connection.commit()
    connection.close()
    return updates


def build_table_configs(graph: nx.DiGraph, views_path: str) -> List[Table]:
    nodes = dict(graph.nodes(data=True))
    return [Table(table,
                  data['contents'],
                  data['interval'],
                  json.dumps(parse_table_config(table, views_path)))
            for table, data in nodes.items()]


def save_tables(tables_and_queries: List[Table], cursor) -> Tuple[List, List]:
    try:
        updated_tables = []
        new_tables = []

        for table in tables_and_queries:
            if is_already_in_db(table.name, cursor):
                updated = update_table(table.name, table.query, table.interval,
                                       table.config, cursor)
                if updated:
                    updated_tables.append(updated)
            else:
                insert_table(table.name, table.query,
                             table.interval, table.config, cursor)
                new_tables.append(table.name)

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
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                   (table, query,
                    interval, config,
                    None, 0, 0,
                    1, None, None, None))


def should_be_updated(table: str, query: str, interval: int,
                      config: str, cursor) -> bool:
    cursor.execute('''SELECT query, interval, config
                    FROM tables
                    WHERE table_name = ?''', (table,))
    current = cursor.fetchone()
    if current != (query, interval, config):
        return True
    return False


def update_table(table: str, query: str, interval: int, config: str,
                 cursor) -> Optional[str]:
    if should_be_updated(table, query, interval, config, cursor):
        cursor.execute('''UPDATE tables
                        SET query = ?, 
                            interval = ?, 
                            config = ?, 
                            force = 1
                        WHERE table_name = ?''',
                       (query, interval, config, table))
        return table

    return None


def create_tables_table(cursor):
    cursor.execute('''CREATE TABLE IF NOT EXISTS tables
                    (table_name text, query text, 
                    interval integer, config text, 
                    last_created integer, mean real, times_run integer,
                    force integer, started integer, deleted integer,
                    waiting integer);''')


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


def mark_deleted_tables(tables_and_queries: List[Table], cursor):
    tables = tuple(table.name for table in tables_and_queries)
    cursor.execute(f'''UPDATE tables
                    SET deleted = strftime('%s', 'now')
                    WHERE table_name NOT IN {str(tables)}
                    AND deleted IS NULL''')
