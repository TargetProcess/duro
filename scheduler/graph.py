import re
from logging import Logger
from typing import List

import networkx as nx

from errors import NotADAGError
from utils.graph_utils import copy_graph_without_attributes, detect_cycles


def build_graph(tables: List) -> nx.DiGraph:
    graph = nx.DiGraph()
    graph.add_nodes_from(tables)
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


def save_graph_to_file(graph: nx.DiGraph):
    nx.nx_pydot.to_pydot(graph).write_png('dependencies.png')
    nx.nx_pydot.write_dot(
        copy_graph_without_attributes(graph, ['contents', 'interval']),
        'dependencies.dot')


def check_for_cycles(graph: nx.DiGraph, logger: Logger):
    valid, cycles = detect_cycles(graph)
    if not valid:
        logger.error('Views dependency graph is not a DAG. Cycles detected:')
        for cycle in cycles:
            logger.error(sorted(cycle))
        raise NotADAGError(f'Graph in is not a DAG. Number of cycles: {len(cycles)}. '
                           f'First cycle: {cycles[0]}')
