from typing import NamedTuple

import networkx as nx


class Table(NamedTuple):
    name: str
    query: str
    interval: int
    last_created: int


class GlobalConfig(NamedTuple):
    db_path: str
    views_path: str
    graph: nx.DiGraph


class DistSortKeys(NamedTuple):
    distkey: str
    diststyle: str
    sortkey: str
