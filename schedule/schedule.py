import re
from itertools import takewhile
from typing import List

import networkx as nx


from errors import (NotADAGError, RootsWithoutIntervalError,
                    MaterializationError)
from file_utils import list_view_files
from schedule.sqlite import save_to_db
from schedule.commits import get_previous_commit, get_all_commits
from graph_utils import (find_roots_without_interval, detect_cycles,
                         copy_graph_without_attributes)


def get_updated_views(commits: List, db: str) -> List[str]:
    previous_commit = get_previous_commit(db)
    return list(takewhile(lambda commit: commit != previous_commit, commits))


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


def create_schedule(graph: nx.DiGraph):
    nodes = dict(graph.nodes(data=True))
    nodes_with_interval = ((k, v['interval']) for k, v in nodes.items() if
                           v.get('interval'))
    # for (table, interval) in nodes_with_interval:
        # build_single_schedule(table, interval)


def main(sql_folder, strict=False, db_path='../duro.db', use_git=False):
    latest_commit = None
    if use_git:
        commits = get_all_commits(sql_folder)
        views = get_updated_views(commits, db_path)
        latest_commit = commits[0] if len(commits) > 0 else None
        if len(views) == 0:
            print('No new commits')
            return

    graph = build_graph(sql_folder)
    is_dag, cycles = detect_cycles(graph, strict)
    nx.nx_pydot.to_pydot(graph).write_png('dependencies.png')
    nx.nx_pydot.write_dot(
        copy_graph_without_attributes(graph, ['contents', 'interval']),
        'dependencies.dot')

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

    # create_schedule(graph)
    save_to_db(graph, db_path, latest_commit)


if __name__ == '__main__':
    try:
        main('../views', strict=False, use_git=False)
    except MaterializationError:
        print('Couldn‘t build a schedule for this views folder.')
