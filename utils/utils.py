import json
from typing import NamedTuple, Dict, Optional

temp_postfix = '_duro_temp'


class DistSortKeys(NamedTuple):
    distkey: str
    diststyle: str
    sortkey: str


class Table:
    def __init__(self, name: str, query: str, interval: Optional[int],
                 config: Dict = None, last_created: int = None,
                 force: bool = None, waiting: bool = None):
        self.name = name
        self.query = query
        self.interval = interval
        self.config = config
        self.config_json = json.dumps(config) if config else None
        self.last_created = last_created
        self.force = force
        self.waiting = waiting

    def __repr__(self):
        return f'Table({self.name}, {self.query}, {self.interval}, ' \
               f'{self.config}, {self.last_created}, {self.force}, ' \
               f'{self.waiting})'

    def __str__(self):
        return f'{self.name} — {self.interval}'
