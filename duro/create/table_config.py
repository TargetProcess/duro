from typing import Dict

from utils.table import DistSortKeys, Table, temp_postfix


def load_dist_sort_keys(config: Dict) -> DistSortKeys:
    if not config:
        return DistSortKeys("", "", "")

    distkey = f'distkey("{config["distkey"]}")' if config.get("distkey") else ""
    diststyle = f'diststyle {config["diststyle"]}' if config.get("diststyle") else ""
    sortkey = f'sortkey("{config["sortkey"]}")' if config.get("sortkey") else ""

    # noinspection PyArgumentList
    return DistSortKeys(distkey, diststyle, sortkey)


def add_dist_sort_keys(table: Table) -> str:
    query = table.query.rstrip(";\n")
    keys = load_dist_sort_keys(table.config)
    return f"""CREATE TABLE {table.name}{temp_postfix}
            {keys.distkey} {keys.sortkey} {keys.diststyle}
            AS (
            {query}
            );"""


def load_grant_select_statements(table: str, config: Dict) -> str:
    if not config:
        return ""

    users = config.get("grant_select")
    if users is not None:
        return f"""GRANT SELECT ON {table}{temp_postfix} TO {config['grant_select']}"""

    return ""


def has_snapshots(table: Table) -> bool:
    return bool(table.config.get("snapshots_interval"))
