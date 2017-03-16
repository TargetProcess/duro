import networkx as nx
from graph_utils import find_roots_without_interval
from file_utils import list_view_files
from errors import NotADAGError, RootsWithoutIntervalError, MaterializationError
from pprint import pprint
import re


def build_graph(folder: str) -> nx.DiGraph:
    graph = nx.DiGraph()
    pprint(list_view_files(folder))
    graph.add_nodes_from(list_view_files(folder))
    nodes_list = graph.nodes()
    for node, query in graph.nodes_iter(data=True):
        for other_node in nodes_list:
            if re.search(r'\b' + re.escape(other_node) + r'\b', query.get('contents')):
                graph.add_edge(node, other_node)
    return graph


def draw_subgraphs(graph: nx.DiGraph):
    subgraphs = nx.weakly_connected_component_subgraphs(graph)
    counter = 1
    for subgraph in subgraphs:
        nx.nx_pydot.to_pydot(subgraph).write_png(f'graph{counter}.png')
        counter += 1


def create_schedule(graph: nx.DiGraph, yml_file: str):
    schedule = ''

    with open(yml_file, 'w') as file:
        file.write(schedule)


def main(sql_folder, yml_file):
    graph = build_graph(sql_folder)
    is_dag = nx.is_directed_acyclic_graph(graph)
    nx.nx_pydot.to_pydot(graph).write_png('graph.png')

    if not is_dag:
        print('Views dependency graph is not a DAG. Cycles detected:')
        for cycle in nx.simple_cycles(graph):
            print(cycle)
        raise NotADAGError

    # draw_subgraphs(graph)

    roots_without_interval = find_roots_without_interval(graph)

    if roots_without_interval:
        print('Some roots don’t have an interval specified. These roots are:',
              roots_without_interval)
        raise RootsWithoutIntervalError

    create_schedule(graph, yml_file)


if __name__ == '__main__':
    try:
        main('./views', 'schedule.yml')
    except MaterializationError:
        print('Couldn‘t build a schedule for this views folder.')
