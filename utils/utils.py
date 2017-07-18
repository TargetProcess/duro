from typing import NamedTuple, Dict


class Table(NamedTuple):
    name: str
    query: str
    interval: int
    config: Dict
    last_created: int
    force: bool


class DistSortKeys(NamedTuple):
    distkey: str
    diststyle: str
    sortkey: str
