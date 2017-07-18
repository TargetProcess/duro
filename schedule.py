import re
from itertools import takewhile
from logging import Logger
from typing import List

import networkx as nx

from errors import (NotADAGError, RootsWithoutIntervalError,
                    MaterializationError)
from schedule.commits import get_previous_commit, get_all_commits
from schedule.sqlite import save_to_db
from utils.file_utils import list_view_files
from utils.graph_utils import (find_roots_without_interval, detect_cycles,
                               copy_graph_without_attributes)
from utils.logger import setup_logger


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


def main(sql_folder, logger: Logger, strict=False, db_path='./duro.db',
         use_git=False):
    latest_commit = None
    if use_git:
        commits = get_all_commits(sql_folder)
        views = get_updated_views(commits, db_path)
        latest_commit = commits[0] if len(commits) > 0 else None
        if len(views) == 0:
            logger.info('No new commits')
            return

    graph = build_graph(sql_folder)
    logger.info(f'Built graph for {sql_folder}')
    is_dag, cycles = detect_cycles(graph, strict)
    nx.nx_pydot.to_pydot(graph).write_png('dependencies.png')
    nx.nx_pydot.write_dot(
        copy_graph_without_attributes(graph, ['contents', 'interval']),
        'dependencies.dot')
    logger.info(f'Saved graph to file')

    if not is_dag:
        logger.error('Views dependency graph is not a DAG. Cycles detected:')
        for cycle in cycles:
            print(cycle)
        raise NotADAGError

    roots_without_interval = find_roots_without_interval(graph)

    if roots_without_interval:
        logger.error(
            'Some roots don’t have an interval specified. These roots are:',
            roots_without_interval)
        raise RootsWithoutIntervalError

    save_to_db(graph, db_path, latest_commit)
    if use_git:
        logger.info(f'Rescheduled for commit {latest_commit}')
    else:
        logger.info('Rescheduled')


if __name__ == '__main__':
    logger = setup_logger('scheduler')
    try:
        main('./views', logger, strict=False, use_git=False)
    except MaterializationError:
        logger.error('Couldn‘t build a schedule for this views folder')
