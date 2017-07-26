import configparser
from functools import lru_cache
from typing import NamedTuple, Union

import sys

import networkx as nx


class GlobalConfig(NamedTuple):
    db_path: str
    views_path: str
    logs_path: str
    graph: nx.DiGraph


class SlackConfig(NamedTuple):
    url: str
    success_channel: str
    failure_channel: str
    log_channel: str


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


@lru_cache()
def load_slack_config() -> Union[SlackConfig, None]:
    try:
        config = configparser.ConfigParser()
        config.read('config.conf')
        url = config['slack']['url']
        channel = config['slack'].get('channel')
        success_channel = config['slack'].get('success_channel', channel)
        failure_channel = config['slack'].get('failure_channel', channel)
        log_channel = config['slack'].get('log_channel', channel)
        # noinspection PyArgumentList
        return SlackConfig(url, success_channel,
                           failure_channel, log_channel)
    except configparser.Error as e:
        print(e)
        return None
    except KeyError as e:
        print(f'No value for {e}')
        return None
