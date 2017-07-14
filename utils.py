from typing import NamedTuple

import networkx as nx


class Table(NamedTuple):
    name: str
    query: str
    interval: int
    last_created: int




class DistSortKeys(NamedTuple):
    distkey: str
    diststyle: str
    sortkey: str
