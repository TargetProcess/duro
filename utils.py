from typing import NamedTuple


class Table(NamedTuple):
    name: str
    query: str
    interval: int
    last_created: int
    force: bool


class DistSortKeys(NamedTuple):
    distkey: str
    diststyle: str
    sortkey: str
