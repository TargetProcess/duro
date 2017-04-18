import re
from pprint import pprint
import sqlite3

import networkx as nx

from errors import NotADAGError, RootsWithoutIntervalError, MaterializationError
from file_utils import list_view_files
from graph_utils import find_roots_without_interval, detect_cycles, copy_graph_without_attributes
# from schedule_ansible import build_single_schedule
from schedule_cron import build_single_schedule


def build_graph(folder: str) -> nx.DiGraph:
    graph = nx.DiGraph()
    graph.add_nodes_from(list_view_files(folder))
    nodes_list = graph.nodes()
    for node, query in graph.nodes_iter(data=True):
        for other_node in nodes_list:
            if re.search(r'\b' + re.escape(other_node) + r'\b',
                         query.get('contents')):
                graph.add_edge(node, other_node)
    return graph


def draw_subgraphs(graph: nx.DiGraph):
    subgraphs = nx.weakly_connected_component_subgraphs(graph)
    counter = 1
    for subgraph in subgraphs:
        nx.nx_pydot.to_pydot(subgraph).write_png(f'graph{counter}.png')
        counter += 1


def create_schedule(graph: nx.DiGraph, yml_file: str):
    nodes = dict(graph.nodes(data=True))
    nodes_with_interval = ((k, v['interval']) for k, v in nodes.items() if
                           v.get('interval'))
    schedule = '\n'.join((build_single_schedule(table, interval)
                          for (table, interval) in nodes_with_interval))

    with open(yml_file, 'w') as file:
        file.write(schedule)


def save_to_db(graph: nx.DiGraph, db_path: str):
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    cursor.execute('''DROP TABLE IF EXISTS tables''')
    cursor.execute('''CREATE TABLE IF NOT EXISTS tables
                        (table_name text, query text, interval integer, last_created integer);''')

    nodes = dict(graph.nodes(data=True))
    tables_and_queries = [(k, v['contents'], v['interval']) for k, v in nodes.items()]
    for table, query, interval in tables_and_queries:
        cursor.execute('''UPDATE tables SET query = ?, interval = ?
                        WHERE table_name = ? ''', (query, interval, table))
        if cursor.rowcount == 0:
            cursor.execute('''INSERT INTO tables VALUES (?, ?, ?, ?)''', (table, query, interval, None))

    connection.commit()
    connection.close()


def main(sql_folder, yml_file, strict=False, db_path='duro.db'):
    graph = build_graph(sql_folder)
    is_dag, cycles = detect_cycles(graph, strict)
    nx.nx_pydot.to_pydot(graph).write_png('dependencies.png')
    nx.nx_pydot.write_dot(copy_graph_without_attributes(graph, ['contents', 'interval']), 'dependencies.dot')

    if not is_dag:
        print('Views dependency graph is not a DAG. Cycles detected:')
        for cycle in cycles:
            print(cycle)
        raise NotADAGError

    roots_without_interval = find_roots_without_interval(graph)

    if roots_without_interval:
        print('Some roots don’t have an interval specified. These roots are:',
              roots_without_interval)
        raise RootsWithoutIntervalError

    create_schedule(graph, yml_file)
    save_to_db(graph, db_path)


if __name__ == '__main__':
    try:
        main('./views', 'schedule.yml', strict=False)
    except MaterializationError:
        print('Couldn‘t build a schedule for this views folder.')
