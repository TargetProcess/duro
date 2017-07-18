import configparser
from typing import NamedTuple

import sys

import networkx as nx


class GlobalConfig(NamedTuple):
    db_path: str
    views_path: str
    logs_path: str
    graph: nx.DiGraph


def load_global_config() -> GlobalConfig:
    try:
        config = configparser.ConfigParser()
        config.read('config.conf')
        db_path = config['main'].get('db', './duro.db')
        views_path = config['main'].get('views', './views')
        graph_file_path = config['main'].get('graph', 'dependencies.dot')
        logs_path = config['main'].get('logs', './logs')
        graph = nx.nx_pydot.read_dot(graph_file_path)
        # noinspection PyArgumentList
        return GlobalConfig(db_path, views_path, logs_path, graph)
    except configparser.NoSectionError:
        print('No ’main’ section in config.conf')
        sys.exit(1)

