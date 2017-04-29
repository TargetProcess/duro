import sqlite3
from datetime import datetime as dt

import networkx as nx


def save_to_db(graph: nx.DiGraph, db_path: str, commit: str):
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    # cursor.execute('''DROP TABLE IF EXISTS tables''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS tables
                        (table_name text, query text, 
                        interval integer, last_created integer);''')

    nodes = dict(graph.nodes(data=True))
    tables_and_queries = [(k, v['contents'], v['interval']) for k, v in
                          nodes.items()]
    for table, query, interval in tables_and_queries:
        cursor.execute('''UPDATE tables SET query = ?, interval = ?
                        WHERE table_name = ? ''', (query, interval, table))
        if cursor.rowcount == 0:
            cursor.execute('''INSERT INTO tables 
                            VALUES (?, ?, ?, ?)''',
                           (table, query, interval, None))

    if commit is not None:
        cursor.execute('''INSERT INTO commits VALUES (?, ?)''',
                       (commit, int(dt.now().timestamp())))

    connection.commit()
    connection.close()
