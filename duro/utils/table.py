import json
from typing import NamedTuple, Dict, Optional

from utils.utils import convert_interval_to_integer

temp_postfix = "_duro_temp"
old_postfix = "_duro_old"
history_postfix = "_history"


class DistSortKeys(NamedTuple):
    distkey: str
    diststyle: str
    sortkey: str


class Table:
    def __init__(
        self,
        name: str,
        query: str,
        interval: Optional[int],
        config: Dict = None,
        last_created: int = None,
        force: bool = None,
        waiting: bool = None,
    ):
        self.name = name
        self.query = query
        self.interval = interval
        self.config = config if config else {}
        self.config_json = json.dumps(config) if config else None
        self.last_created = last_created
        self.force = force
        self.waiting = waiting

        self.snapshots_interval_mins = convert_interval_to_integer(
            self.config.get("snapshots_interval")
        )

        self.snapshots_stored_for_mins = convert_interval_to_integer(
            self.config.get("snapshots_stored_for")
        )

    def __repr__(self):
        return f"Table({self.name}, {self.query}, {self.interval}, {self.config}, {self.last_created}, {self.force}, {self.waiting})"

    def __str__(self):
        return f"{self.name} — {self.interval}"

    def load_dist_sort_keys(self) -> DistSortKeys:
        if not self.config:
            return DistSortKeys("", "", "")

        distkey = (
            f'distkey("{self.config["distkey"]}")' if self.config.get("distkey") else ""
        )
        diststyle = (
            f'diststyle {self.config["diststyle"]}'
            if self.config.get("diststyle")
            else ""
        )
        sortkey = (
            f'sortkey("{self.config["sortkey"]}")' if self.config.get("sortkey") else ""
        )

        # noinspection PyArgumentList
        return DistSortKeys(distkey, diststyle, sortkey)

    def get_query_with_dist_sort_keys(self) -> str:
        query = self.query.rstrip(";\n")
        keys = self.load_dist_sort_keys()
        return f"""
            create table {self.name}{temp_postfix}
            {keys.distkey} {keys.sortkey} {keys.diststyle}
            as (
                {query}
            );
        """

    def load_grant_select_statements(self) -> str:
        if not self.config:
            return ""

        users = self.config.get("grant_select")
        if users is not None:
            return f"""
                grant select on {self.name}{temp_postfix} 
                to {self.config['grant_select']}
            """

        return ""

    @property
    def store_snapshots(self) -> bool:
        return bool(self.snapshots_interval_mins)
