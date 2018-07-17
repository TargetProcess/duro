from typing import List, Tuple

import networkx as nx


def find_roots_without_interval(graph: nx.DiGraph) -> List[str]:
    return find_sources_without_attribute(graph, 'interval')


def find_sources(graph: nx.DiGraph) -> List[str]:
    return [node for (node, in_degree) in graph.in_degree().items() if
            in_degree == 0]


def find_sources_without_attribute(graph: nx.DiGraph,
                                   attribute: str) -> List[str]:
    roots = find_sources(graph)
    nodes = dict(graph.nodes(data=True))
    return [root for root in roots if not nodes[root].get(attribute)]


def detect_cycles(source_graph: nx.DiGraph) -> Tuple[bool, List]:
    return (nx.is_directed_acyclic_graph(source_graph),
            nx.simple_cycles(source_graph))


def copy_graph_without_attributes(source_graph: nx.DiGraph,
                                  attributes: List) -> nx.DiGraph:
    graph = source_graph.copy()
    for node in graph:
        for attribute in attributes:
            if attribute in graph.node[node]:
                del graph.node[node][attribute]
    return graph


def get_all_successors(graph: nx.DiGraph, node) -> List:
    return list(nx.dfs_preorder_nodes(graph, node))
