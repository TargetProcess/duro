import networkx as nx

from utils.graph_utils import (find_sources, find_sources_without_attribute,
                               copy_graph_without_attributes,
                               get_all_successors, detect_cycles)

no_sources = nx.DiGraph([[1, 2], [3, 4], [3, 1], [4, 2], [2, 3]],
                        name='no sources')

no_edges = nx.DiGraph(name='no edges')
no_edges.add_nodes_from([1, 2, 3, 4])

simple_tree = nx.DiGraph([[1, 2], [1, 3], [1, 4], [1, 5]])
simple_cycle = nx.DiGraph([[1, 2], [2, 1]])

two_trees = nx.DiGraph([[1, 2], [1, 3], [1, 4], [6, 7], [6, 8]])

two_trees_and_a_cycle = nx.DiGraph([[1, 2], [1, 3], [1, 4], [1, 5], [6, 7],
                                    [6, 8], [9, 10], [10, 11], [11, 9]])

two_cycles = nx.DiGraph([['a', 'b'], ['b', 'c'], ['c', 'a'],
                         ['c', 'e'], ['e', 'f'], ['f', 'c']])

two_trees_and_a_node = nx.DiGraph(
    [[1, 2], [1, 3], [1, 4], [1, 5], [6, 7], [6, 8]])
two_trees_and_a_node.add_node(9)

graph_one = nx.DiGraph([[0, 6], [1, 3], [1, 5], [1, 6], [3, 4], [3, 5], [3, 7],
                        [4, 5], [5, 7], [5, 8], [7, 9], [8, 9]])

growing_network = nx.DiGraph(
    [(0, 1), (0, 2), (0, 3), (0, 4), (1, 5), (2, 6), (2, 7), (5, 8), (1, 9),
     (9, 10), (1, 11), (0, 12), (3, 13),
     (0, 14), (5, 15), (1, 16), (15, 17), (9, 18), (1, 19)])

scale_free = nx.DiGraph(
    [(1, 0), (2, 1), (9, 1), (0, 2), (1, 2), (0, 3), (0, 3), (5, 3), (8, 3),
     (6, 3),
     (1, 4), (0, 5), (5, 6), (0, 7),
     (0, 7), (1, 8), (0, 8), (1, 10), (2, 10), (0, 11), (0, 12), (0, 13),
     (4, 14)]
)

graph = two_trees
nx.nx_pydot.to_pydot(graph).write_png('graph-test.png')


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
    assert find_sources_without_attribute(two_trees_and_a_cycle_copy,
                                          'attr') == [6]
    two_trees_and_a_cycle_copy.node[2]['attr'] = 'value'
    assert find_sources_without_attribute(two_trees_and_a_cycle_copy,
                                          'attr') == [6]
    two_trees_and_a_cycle_copy.node[6]['another_attr'] = 'value'
    assert find_sources_without_attribute(two_trees_and_a_cycle_copy,
                                          'attr') == [6]
    two_trees_and_a_cycle_copy.node[6]['attr'] = 'value'
    assert find_sources_without_attribute(two_trees_and_a_cycle_copy,
                                          'attr') == []

    simple_tree_copy = simple_tree.copy()
    assert find_sources_without_attribute(simple_tree_copy, 'attr') == [1]
    simple_tree_copy.node[1]['attr'] = None
    assert find_sources_without_attribute(simple_tree_copy, 'attr') == [1]


def test_copy_graph_without_attributes():
    graph = nx.DiGraph()
    graph.add_nodes_from([(1, {'a': 42, 'b': 44}), (2, {'a': 43}), (3,), (4,)])

    no_attributes = copy_graph_without_attributes(graph, ['a', 'b', 'c']).nodes(
        data=True)
    for node in no_attributes:
        assert node[1].get('a') is None
        assert node[1].get('b') is None
        assert node[1].get('c') is None

    graph = nx.DiGraph()
    graph.add_nodes_from([1, 2, 3, 4])
    nx.set_node_attributes(graph, 'a', {1: 42, 2: 43})
    nx.set_node_attributes(graph, 'b', {1: 44})

    no_a_attribute = copy_graph_without_attributes(graph, ['a']).nodes(
        data=True)
    for node in no_attributes:
        assert node[1].get('a') is None
    assert no_a_attribute[0][1].get('b') == 44


def test_get_all_successor():
    two_level_tree = nx.DiGraph([[1, 2], [1, 3], [1, 4], [2, 5], [2, 6]])
    three_level_tree = nx.DiGraph(
        [[1, 2], [1, 3], [1, 4], [2, 5], [2, 6], [3, 7], [5, 8]])

    assert get_all_successors(simple_tree, 1) == [1, 2, 3, 4, 5]
    assert sorted(get_all_successors(two_level_tree, 1)) == [1, 2, 3, 4, 5, 6]
    assert sorted(get_all_successors(three_level_tree, 1)) == [1, 2, 3, 4, 5, 6,
                                                               7, 8]
    assert sorted(get_all_successors(three_level_tree, 2)) == [2, 5, 6, 8]
    assert sorted(get_all_successors(graph_one, 1)) == [1, 3, 4, 5, 6, 7, 8, 9]
    assert sorted(get_all_successors(graph_one, 5)) == [5, 7, 8, 9]


def test_detect_cycles():
    graphs = [simple_tree,
              two_trees_and_a_cycle,
              two_cycles,
              simple_cycle]
    results = []
    for graph in graphs:
        is_dag, cycles = detect_cycles(graph)
        sorted_cycles = sorted(sorted(list(cycle)) for cycle in cycles)
        results.append((is_dag, sorted_cycles))
    assert results == [(True, []),
                       (False, [[9, 10, 11]]),
                       (False, [['a', 'b', 'c'], ['c', 'e', 'f']]),
                       (False, [[1, 2]])]
