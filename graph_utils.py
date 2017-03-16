import networkx as nx


def find_roots_without_interval(graph: nx.DiGraph) -> list:
    return find_sources_without_attribute(graph, 'interval')


def find_sources(graph: nx.DiGraph) -> list:
    return [node for (node, in_degree) in graph.in_degree().items() if in_degree == 0]


def find_sources_without_attribute(graph: nx.DiGraph, attribute: str) -> list:
    roots = find_sources(graph)
    nodes = dict(graph.nodes(data=True))
    return [root for root in roots if not nodes[root].get(attribute)]