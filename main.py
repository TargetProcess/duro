import networkx as nx
from graph_utils import find_roots_without_interval
from file_utils import list_sql_files, list_sql_files_with_content


def mock_graph():
    graph = nx.DiGraph()
    graph.add_node('consolidated', interval='1h')
    graph.add_node('feedback')
    graph.add_node('requests')
    graph.add_edge('consolidated', 'feedback')
    # graph.add_edge('feedback', 'consolidated')
    graph.add_edge('feedback', 'requests')
    graph.add_node('arr')
    graph.add_node('companies')
    graph.add_node('account')
    graph.add_node('first_purchase')
    graph.add_node('last_purchase')
    graph.add_node('dates')
    graph.add_edge('arr', 'companies')
    graph.add_edge('arr', 'dates')
    graph.add_edge('companies', 'account')
    graph.add_edge('companies', 'first_purchase')
    graph.add_edge('companies', 'last_purchase')
    # graph.add_edge('last_purchase', 'arr')
    graph.add_edge('account', 'feedback')
    # graph.add_node('owners')


def build_graph() -> nx.DiGraph:
    graph = nx.DiGraph()
    graph.add_nodes_from(list_sql_files_with_content('./create'))
    nodes_list = graph.nodes()
    for node, query in graph.nodes_iter(data=True):
        # print(query)
        for other_node in nodes_list:
            if f'{other_node} ' in query.get('contents'):
                graph.add_edge(node, other_node)
    return graph


def main():
    graph = build_graph()
    # print(dict(graph.nodes(data=True)))
    print('Is DAG:', nx.is_directed_acyclic_graph(graph))
    for cycle in nx.simple_cycles(graph):
        print('cycle:', cycle)

    s = nx.weakly_connected_component_subgraphs(graph)
    n = 1
    for i in s:
        # print(nx.topological_sort(i))
        nx.nx_pydot.to_pydot(i).write_png(f'graph{n}.png')
        n += 1

    # print(nx.nodes(graph))

    print('roots without interval:', find_roots_without_interval(graph))
    nx.nx_pydot.to_pydot(graph).write_png('graph.png')


if __name__ == '__main__':
    main()
