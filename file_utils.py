from os.path import splitext
import glob
from typing import List, Tuple, NamedTuple, Dict
from functools import lru_cache
import configparser
from itertools import chain
import os


class ViewFile(NamedTuple):
    filename: str
    table: str
    interval: str
    contents: str


def parse_filename(filename: str) -> Tuple:
    split = splitext(filename)[0].split()
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
             if not file.endswith('_test.sql')]

    return [(view.table, {'contents': view.contents, 'interval': convert_interval_to_integer(view.interval)})
            for view in views]


@lru_cache()
def read_file(filename: str) -> str:
    with open(filename) as file:
        return " ".join(line.strip() for line in file)


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


def unzip(list_of_tuples: List) -> Tuple[List, List]:
    return [item[0] for item in list_of_tuples], [item[1] for item in list_of_tuples]


def convert_interval_to_integer(interval: str) -> int:
    if interval is None:
        return None
    units = {'m': 1, 'h': 60, 'd': 1440, 'w': 10080}
    unit = interval[-1]
    if unit not in units.keys():
        raise ValueError('Invalid interval')

    try:
        value = int(interval[:-1])
        return value * units[unit]
    except:
        raise ValueError('Invalid interval')


def get_processor(table: str, path: str) -> str:
    print(f'Loading tests for {table}')
    folder, file = table.split('.')
    processor_file = os.path.join(path, folder, f'{file}.py')
    if os.path.isfile(processor_file):
        return os.path.splitext(processor_file)[0]
    else:
        processor_file = os.path.join(path, f'{table}.py')
        if os.path.isfile(processor_file):
            return os.path.splitext(processor_file)[0]
    return ''


def load_create_query(table: str, path: str) -> str:
    print(f'Loading tests for {table}')
    folder, file = table.split('.')
    create_file = os.path.join(path, folder, f'create_{file}.sql')
    if os.path.isfile(create_file):
        return read_file(create_file)
    else:
        create_file = os.path.join(path, f'create_{table}.sql')
        if os.path.isfile(create_file):
            return read_file(create_file)
    return ''
