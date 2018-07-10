import configparser
import glob
import os
from functools import lru_cache
from itertools import chain
from typing import List, Tuple, Dict, Optional, Callable

from utils.utils import temp_postfix


class TableFile:
    def __init__(self, filename: str, views_path: str):
        self.filename = filename
        short_filename = filename.replace(f'{views_path}/', '', 1)
        self.table, self.interval = parse_filename(short_filename)
        self.select_query = load_select_query(self.table, views_path)


def parse_filename(filename: str) -> Tuple:
    split = os.path.splitext(filename)[0].split()
    interval = split[2] if len(split) > 1 else None
    folder, table = split[0].split('/') if '/' in split[0] else (None, split[0])
    if '.' in table:
        return table, interval
    else:
        if folder is None:
            raise ValueError('No schema specified')
        return f'{folder}.{table}', interval


def load_table_from_file(filename: str, views_path: str) -> TableFile:
    return TableFile(filename, views_path)


def list_tables_in_path(views_path: str) -> List[Tuple[str, Dict]]:
    views = [load_table_from_file(file, views_path)
             for file in glob.glob(views_path + '/**/*.sql', recursive=True)
             if is_query(file)]

    return [(view.table,
             {
                 'contents': view.select_query,
                 'interval': convert_interval_to_integer(view.interval)
             }
             )
            for view in views]


def is_query(filename: str) -> bool:
    if not filename:
        return False

    return (is_sql_query(filename)
            and not is_test(filename)
            and not is_processor_select_query(filename)
            and os.path.isfile(filename))


def is_sql_query(filename: str) -> bool:
    return filename.endswith('.sql')


def is_test(filename: str) -> bool:
    return filename.endswith('_test.sql')


def is_processor(filename: str) -> bool:
    return filename.endswith('.py')


def is_processor_select_query(filename: str) -> bool:
    if not filename.endswith('_select.sql'):
        return False

    ddl_filename = filename.replace('_select.sql', '.sql')
    return has_processor(ddl_filename)


def has_processor(filename: str) -> bool:
    processor_filename = f'{os.path.splitext(filename)[0].split()[0]}.py'

    return os.path.isfile(processor_filename)


def has_processor_select_query(filename: str) -> bool:
    pass


def is_processor_ddl(filename: str) -> bool:
    if not filename or not is_sql_query(filename):
        return False

    return has_processor(filename)


def load_processor(table: str, views_path: str) -> str:
    return find_file_for_table(table, views_path, is_processor)


def load_ddl_query(table: str, views_path: str) -> str:
    ddl_file = find_file_for_table(table, views_path, is_processor_ddl)
    query = read_file(ddl_file)
    return query.lower().replace(f'create table {table}',
                                 f'create table {table}{temp_postfix}')


def load_query(table: str, views_path: str) -> str:
    return read_file(find_file_for_table(table, views_path, is_query))


def load_select_query(table: str, views_path: str) -> str:
    if load_processor(table, views_path):
        processor_select_query = find_file_for_table(table, views_path, is_processor_select_query)
        return read_file(processor_select_query)

    return load_query(table, views_path)


def find_file_for_table(table: str, views_path: str, match: Callable) -> str:
    folder, file = table.split('.')
    files_inside = [file for file in
                    glob.glob(os.path.join(views_path, folder, f'{file}*'))
                    if match(file)]

    if files_inside and os.path.isfile(files_inside[0]):
        return files_inside[0]

    else:
        files_outside = [file for file in
                         glob.glob(os.path.join(views_path, table, '*'))
                         if match(file)]
        if files_outside and os.path.isfile(files_outside[0]):
            return files_outside[0]

    return ''


def find_tables_with_missing_files() -> Optional[str]:
    # tests without select
    # .py without select
    # .py without ddl
    # forbidden postfixes
    pass


def read_file(filename: str) -> str:
    try:
        with open(filename) as file:
            return "\n".join(line.strip() for line in file)
    except FileNotFoundError:
        raise ValueError(f'{filename} not found')


@lru_cache()
def read_config(filename: str) -> Dict:
    config = configparser.ConfigParser()
    try:
        with open(filename) as lines:
            lines = chain(("[top]",), lines)
            config.read_file(lines)
        return dict(config.items('top'))
    except FileNotFoundError:
        return {}


def convert_interval_to_integer(interval: Optional[str]) -> Optional[int]:
    if interval is None:
        return None
    units = {'m': 1, 'h': 60, 'd': 1440, 'w': 10080}
    unit = interval[-1].lower()
    if unit not in units.keys():
        raise ValueError('Invalid interval')

    try:
        value = int(interval[:-1])
        return value * units[unit]
    except ValueError:
        raise ValueError('Invalid interval')
