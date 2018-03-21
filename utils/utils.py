from typing import NamedTuple, Dict


class DistSortKeys(NamedTuple):
    distkey: str
    diststyle: str
    sortkey: str


class Table:
    def __init__(self, name: str, query: str, interval: int,
                 config: Dict, last_created: int = None,
                 force: bool = None, waiting: bool = None):
        self.name = name
        self.query = query
        self.interval = interval
        self.config = config
        self.last_created = last_created
        self.force = force
        self.waiting = waiting

    def __repr__(self):
        return f'Table({self.name}, {self.query}, {self.interval}, {self.config}, {self.last_created}, {self.force}, {self.waiting})'

    def __str__(self):
        return f'{self.name} — {self.interval}'
