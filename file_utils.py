from os.path import splitext, isfile
import os


def parse_filename(filename):
    split = splitext(filename)[0].split()
    # if split[-1] == split[0]:
    #     raise ValueError('There’s no schedule, table name or .sql extension in the filename')
    return split[0] #, split[-1]


def list_sql_files(path):
    files = [file for file in os.listdir(path) if isfile(os.path.join(path, file))]
    parsed_names = [parse_filename(file) for file in files]
    return parsed_names


def list_sql_files_with_content(path: str) -> zip:
    files = [file for file in os.listdir(path) if isfile(os.path.join(path, file))]
    parsed_names = [parse_filename(file) for file in files]
    contents = [{'contents': read_file(path, file)} for file in files]
    return zip(parsed_names, contents)


def read_file(path: str, file: str) -> str:
    with open(os.path.join(path, file)) as f:
        return " ".join(line.strip() for line in f)

