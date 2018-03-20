import configparser
import glob
import os
from functools import lru_cache
from itertools import chain
from typing import List, Tuple, NamedTuple, Dict, Optional


class ViewFile(NamedTuple):
    filename: str
    table: str
    interval: str
    contents: str


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


def parse_view(filename: str, path: str) -> ViewFile:
    table, interval = parse_filename(filename.replace(f'{path}/', '', 1))

    # noinspection PyArgumentList
    return ViewFile(filename, table, interval, read_file(filename))


def list_view_files(path: str) -> List[Tuple[str, dict]]:
    views = [parse_view(file, path)
             for file in glob.glob(path + '/**/*.sql', recursive=True)
             if is_query(file)]
    return [(view.table, {'contents': view.contents,
                          'interval': convert_interval_to_integer(
                              view.interval)})
            for view in views]


def is_query(file: str) -> bool:
    return (file.endswith('.sql')
            and not file.endswith('_test.sql')
            and not is_processor_ddl(file)
            and os.path.isfile(file))


def is_processor_ddl(filename: str) -> bool:
    if '_ddl' not in filename:
        return False
    path = os.path.dirname(filename)
    file = os.path.basename(filename)
    table_name = file.replace('_ddl', '')
    processor_filename = f'{os.path.splitext(table_name)[0].split()[0]}.py'
    processor_file = os.path.join(path, processor_filename)

    if os.path.isfile(processor_file) and file.endswith('_ddl.sql'):
        return True
    return False


def read_file(filename: str) -> str:
    with open(filename) as file:
        return "\n".join(line.strip() for line in file)


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


def convert_interval_to_integer(interval: str) -> Optional[int]:
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


def find_file_for_table(table: str, path: str, match: callable) -> str:
    folder, file = table.split('.')
    files_inside = [file for file in
                    glob.glob(os.path.join(path, folder, f'{file}*'))
                    if match(file)]

    if files_inside and os.path.isfile(files_inside[0]):
        return files_inside[0]

    else:
        files_outside = [file for file in
                         glob.glob(os.path.join(path, table, '*'))
                         if match(file)]
        if files_outside and os.path.isfile(files_outside[0]):
            return files_outside[0]

    return ''


def load_processor(table: str, path: str) -> str:
    return find_file_for_table(table, path, lambda s: s.endswith('.py'))


def load_ddl_query(table: str, path: str) -> str:
    query = read_file(
        find_file_for_table(table, path, lambda s: s.endswith('_ddl.sql')))
    return query.lower().replace(f'create table {table}',
                                 f'create table {table}_temp')


def load_query(table: str, path: str) -> str:
    return read_file(find_file_for_table(table, path, is_query))


def find_tables_with_missing_files() -> Optional[str]:
    # tests without select
    # .py without select
    # .py without ddl
    # forbidden postfixes
    pass
