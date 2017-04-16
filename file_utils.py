from os.path import splitext
import glob
from typing import List, Tuple, NamedTuple


class ViewFile(NamedTuple):
    filename: str
    # filename_with_path: str
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

    return [(view.table, {'contents': view.contents, 'interval': view.interval})
            for view in views]


def read_file(filename: str) -> str:
    with open(filename) as file:
        return " ".join(line.strip() for line in file)


def unzip(list_of_tuples: List) -> Tuple[List, List]:
    return [item[0] for item in list_of_tuples], [item[1] for item in list_of_tuples]
