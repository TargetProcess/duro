import networkx as nx

from graph_utils import find_sources, find_sources_without_attribute

no_sources = nx.DiGraph([[1, 2], [3, 4], [3, 1], [4, 2], [2, 3]], name='no sources')

no_edges = nx.DiGraph(name='no edges')
no_edges.add_nodes_from([1, 2, 3, 4])

simple_tree = nx.DiGraph([[1, 2], [1, 3], [1, 4], [1, 5]])

two_trees = nx.DiGraph([[1, 2], [1, 3], [1, 4], [6, 7], [6, 8]])

two_trees_and_a_cycle = nx.DiGraph([[1, 2], [1, 3], [1, 4], [1, 5], [6, 7],
                                    [6, 8], [9, 10], [10, 11], [11, 9]])

two_trees_and_a_node = nx.DiGraph([[1, 2], [1, 3], [1, 4], [1, 5], [6, 7], [6, 8]])
two_trees_and_a_node.add_node(9)

graph_one = nx.DiGraph([[0, 6], [1, 3], [1, 5], [1, 6], [3, 4], [3, 5], [3, 7],
                        [4, 5], [5, 7], [5, 8], [7, 9], [8, 9]])

growing_network = nx.DiGraph(
    [(0, 1), (0, 2), (0, 3), (0, 4), (1, 5), (2, 6), (2, 7), (5, 8), (1, 9),
     (9, 10), (1, 11), (0, 12), (3, 13),
     (0, 14), (5, 15), (1, 16), (15, 17), (9, 18), (1, 19)])

scale_free = nx.DiGraph(
    [(1, 0), (2, 1), (9, 1), (0, 2), (1, 2), (0, 3), (0, 3), (5, 3), (8, 3), (6, 3),
     (1, 4), (0, 5), (5, 6), (0, 7),
     (0, 7), (1, 8), (0, 8), (1, 10), (2, 10), (0, 11), (0, 12), (0, 13), (4, 14)]
)

graph = two_trees
nx.nx_pydot.to_pydot(graph).write_png('graph-test.png')


# print(graph.edges())
# edges = [(b, a) for (a, b) in graph.edges()]
# print(edges)


def test_find_sources():
    assert find_sources(no_sources) == []
    assert find_sources(no_edges) == [1, 2, 3, 4]
    assert find_sources(simple_tree) == [1]
    assert find_sources(two_trees) == [1, 6]
    assert find_sources(two_trees_and_a_node) == [1, 6, 9]
    assert find_sources(two_trees_and_a_cycle) == [1, 6]
    assert find_sources(graph_one) == [0, 1]
    assert find_sources(growing_network) == [0]
    assert find_sources(scale_free) == [9]


def test_find_sources_without_attribute():
    two_trees_and_a_cycle_copy = two_trees_and_a_cycle.copy()
    two_trees_and_a_cycle_copy.node[1]['attr'] = 'value'
    assert find_sources_without_attribute(two_trees_and_a_cycle_copy, 'attr') == [6]
    two_trees_and_a_cycle_copy.node[2]['attr'] = 'value'
    assert find_sources_without_attribute(two_trees_and_a_cycle_copy, 'attr') == [6]
    two_trees_and_a_cycle_copy.node[6]['another_attr'] = 'value'
    assert find_sources_without_attribute(two_trees_and_a_cycle_copy, 'attr') == [6]
    two_trees_and_a_cycle_copy.node[6]['attr'] = 'value'
    assert find_sources_without_attribute(two_trees_and_a_cycle_copy, 'attr') == []

    simple_tree_copy = simple_tree.copy()
    assert find_sources_without_attribute(simple_tree_copy, 'attr') == [1]
    simple_tree_copy.node[1]['attr'] = None
    assert find_sources_without_attribute(simple_tree_copy, 'attr') == [1]
