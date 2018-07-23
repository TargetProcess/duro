import configparser
import glob
import os
from functools import lru_cache
from itertools import chain
from typing import List, Tuple, Dict, Optional, Callable

from utils.utils import temp_postfix

select_postfix = '_select'
test_postfix = '_test'


class TableFile:
    def __init__(self, filename: str, views_path: str):
        self.filename = filename
        short_filename = remove_view_path(views_path, filename)
        self.table, self.interval = parse_filename(short_filename)
        self.select_query = load_select_query(views_path, self.table)


def remove_view_path(views_path: str, filename: str) -> str:
    return filename.replace(f'{views_path}/', '', 1)


def load_table_from_file(views_path: str, filename: str) -> TableFile:
    return TableFile(filename, views_path)


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


def list_files(views_path: str,
               match: Callable = lambda x: True,
               mask: str = '*.*') -> List:
    return [file
            for file in glob.glob(views_path + f'/**/{mask}', recursive=True)
            if match(file)
            ]


def load_tables_in_path(views_path: str) -> List[Tuple[str, Dict]]:
    files = list_files(views_path, is_query)
    views = [load_table_from_file(views_path, file)
             for file in files]

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
    if not filename:
        return False
    return filename.endswith('.sql')


def is_test(filename: str) -> bool:
    if not filename:
        return False
    return filename.endswith(f'{test_postfix}.sql')


def is_processor(filename: str) -> bool:
    if not filename:
        return False
    return filename.endswith('.py')


def is_processor_select_query(filename: str) -> bool:
    if not filename or not filename.endswith(f'{select_postfix}.sql'):
        return False

    ddl_filename = filename.replace(f'{select_postfix}.sql', '.sql')
    return has_processor(ddl_filename)


def is_processor_ddl(filename: str) -> bool:
    if not filename or not is_sql_query(filename):
        return False

    return has_processor(filename)


def has_processor(filename: str) -> bool:
    if not filename:
        return False
    processor_filename = f'{os.path.splitext(filename)[0].split()[0]}.py'

    return os.path.isfile(processor_filename)


def list_tests(views_path: str) -> List[str]:
    files = list_files(views_path, match=is_test)
    short_filenames = [remove_view_path(views_path, filename)
                       for filename in files]
    return [parse_filename(filename)[0]
            for filename in short_filenames]


def list_processors(views_path: str) -> List:
    files = list_files(views_path, match=is_processor)
    short_filenames = [remove_view_path(views_path, filename)
                       for filename in files]
    return [parse_filename(filename)[0]
            for filename in short_filenames]


def load_processor(views_path: str, table: str) -> str:
    return find_file_for_table(views_path, table, is_processor)


def load_ddl_query(views_path: str, table: str) -> str:
    ddl_file = find_file_for_table(views_path, table, is_processor_ddl)
    query = read_file(ddl_file)
    return query.lower().replace(f'create table {table}',
                                 f'create table {table}{temp_postfix}')


def load_query(views_path: str, table: str) -> str:
    return read_file(find_file_for_table(views_path, table, is_query))


def load_select_query(views_path: str, table: str) -> str:
    if load_processor(views_path, table):
        processor_select_query = find_file_for_table(views_path, table,
                                                     is_processor_select_query)
        return read_file(processor_select_query)

    return load_query(views_path, table)


def find_file_for_table(views_path: str, table: str, match: Callable) -> str:
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
