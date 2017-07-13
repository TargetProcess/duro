import networkx as nx
from typing import List, Tuple


def find_roots_without_interval(graph: nx.DiGraph) -> List[str]:
    return find_sources_without_attribute(graph, 'interval')


def find_sources(graph: nx.DiGraph) -> List[str]:
    return [node for (node, in_degree) in graph.in_degree().items() if in_degree == 0]


def find_sources_without_attribute(graph: nx.DiGraph, attribute: str) -> List[str]:
    roots = find_sources(graph)
    nodes = dict(graph.nodes(data=True))
    return [root for root in roots if not nodes[root].get(attribute)]


def detect_cycles(source_graph: nx.DiGraph, strict: bool) -> Tuple:
    # TODO: add tests
    if strict:
        graph = source_graph
    else:
        nodes = dict(source_graph.nodes(data=True))
        nodes_with_interval = [k for k, v in nodes.items() if v.get('interval') is not None]
        graph = source_graph.copy().subgraph(nodes_with_interval)

    return nx.is_directed_acyclic_graph(graph), nx.simple_cycles(graph)


def copy_graph_without_attributes(source_graph: nx.DiGraph, attributes: List) -> nx.DiGraph:
    graph = source_graph.copy()
    for node in graph:
        for attribute in attributes:
            if attribute in graph.node[node]:
                del graph.node[node][attribute]
    return graph


def get_all_successors(graph: nx.DiGraph, node) -> List:
    return list(nx.dfs_preorder_nodes(graph, node))
