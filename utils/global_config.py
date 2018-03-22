import configparser
from typing import NamedTuple, Optional

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


def load_global_config(config_file='config.conf') -> GlobalConfig:
    try:
        config = configparser.ConfigParser()
        config.read(config_file)

        db_path = config['main'].get('db', './duro.db')
        views_path = config['main'].get('views', './views')
        graph_file_path = config['main'].get('graph', 'dependencies.dot')
        logs_path = config['main'].get('logs', './logs')
        try:
            graph = nx.nx_pydot.read_dot(graph_file_path)
        except FileNotFoundError:
            graph = None
        # noinspection PyArgumentList
        return GlobalConfig(db_path, views_path, logs_path, graph)
    except (configparser.NoSectionError, KeyError):
        raise ValueError(
            'No ’main’ section in config.conf (or maybe file doesn’t exist at all)')


def load_slack_config(config_file='config.conf') -> Optional[SlackConfig]:
    try:
        config = configparser.ConfigParser()
        config.read(config_file)

        url = config['slack']['url']
        channel = config['slack'].get('channel')
        success_channel = config['slack'].get('success_channel', channel)
        failure_channel = config['slack'].get('failure_channel', channel)
        log_channel = config['slack'].get('log_channel', channel)
        return SlackConfig(url, success_channel,
                           failure_channel, log_channel)
    except configparser.Error as e:
        print(e)
        return None
    except KeyError as e:
        print(f'No value for {e}')
        return None
