import networkx as nx
from typing import List


def find_roots_without_interval(graph: nx.DiGraph) -> List[str]:
    return find_sources_without_attribute(graph, 'interval')


def find_sources(graph: nx.DiGraph) -> List[str]:
    return [node for (node, in_degree) in graph.in_degree().items() if in_degree == 0]


def find_sources_without_attribute(graph: nx.DiGraph, attribute: str) -> List[str]:
    roots = find_sources(graph)
    nodes = dict(graph.nodes(data=True))
    return [root for root in roots if not nodes[root].get(attribute)]