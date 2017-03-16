from os.path import splitext, isfile
import os
from typing import List, Tuple, NamedTuple


class ViewFile(NamedTuple):
    filename: str
    filename_with_path: str
    table: str
    interval: str
    contents: str


def parse_filename(filename: str) -> Tuple:
    split = splitext(filename)[0].split()
    try:
        return split[0], split[2]
    except IndexError:
        return split[0], None


def parse_view(filename: str, path: str) -> ViewFile:
    table, interval = parse_filename(filename)
    filename_with_path = os.path.join(path, filename)
    # noinspection PyArgumentList
    return ViewFile(filename, filename_with_path, table, interval, read_file(filename_with_path))


def list_view_files(path: str) -> List[Tuple[str, dict]]:
    views = [parse_view(file, path) for file in os.listdir(path)
             if isfile(os.path.join(path, file)) and splitext(file)[1] == '.sql']

    return [(view.table, {'contents': view.contents, 'interval': view.interval})
            for view in views]


def read_file(filename: str) -> str:
    with open(filename) as file:
        return " ".join(line.strip() for line in file)


def unzip(list_of_tuples: List) -> Tuple[List, List]:
    return [item[0] for item in list_of_tuples], [item[1] for item in list_of_tuples]
