from typing import Dict
import os

from file_utils import read_config
from utils import DistSortKeys


def load_dist_sort_keys(table: str, config: Dict) -> DistSortKeys:
    distkey = f'distkey("{config["distkey"]}")' if config.get('distkey') else ''
    diststyle = f'diststyle {config["diststyle"]}' if config.get(
        'diststyle') else ''
    sortkey = f'sortkey("{config["sortkey"]}")' if config.get('sortkey') else ''

    # noinspection PyArgumentList
    return DistSortKeys(distkey, diststyle, sortkey)


def add_dist_sort_keys(table: str, query: str, config: Dict) -> str:
    keys = load_dist_sort_keys(table, config)
    return f'''CREATE TABLE {table}_temp
            {keys.distkey} {keys.sortkey} {keys.diststyle}
            AS ({query});'''


def add_grant_select_statements(table: str, query: str, config: Dict) -> str:
    return f'''{query}; 
        GRANT SELECT ON {table}_temp TO sqlpad, tableau, tp_user, livechat, feedback'''


def load_table_config(full_table_name: str, path: str) -> Dict:
    schema, table = full_table_name.split('.')

    global_config = read_config(os.path.join(path, 'global.conf'))
    schema_config_outside = read_config(os.path.join(path, f'{schema}.conf'))
    schema_config_inside = read_config(
        os.path.join(path, schema, f'{schema}.conf'))
    table_config_outside = read_config(
        os.path.join(path, f'{schema}.{table}.conf'))
    table_config_inside = read_config(
        os.path.join(path, schema, f'{table}.conf'))

    merged = {**global_config,
              **schema_config_outside, **schema_config_inside,
              **table_config_outside, **table_config_inside}

    for key, value in merged.items():
        if value in ('null', 'None', ''):
            merged[key] = None

    return merged
