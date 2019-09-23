import os
import re
import shutil
import sqlite3
from pathlib import Path
import sys

sys.path.append("../duro")

import logzero
import pytest
from git import Repo

from duro.utils.table import Table
from tests.create_test_db import ddl, inserts

EMPTY_DB_PATH = "./empty.db"
PREPARED_DB_PATH = "./test_data.db"
VIEWS_PATH = "./views"


def create_test_db(db_name) -> str:
    # db = './new_test_data.db'
    if os.path.exists(db_name):
        return db_name
    connection = sqlite3.connect(db_name, isolation_level=None)
    connection.executescript(ddl)
    connection.executescript(inserts)
    connection.commit()
    return db_name


@pytest.fixture
def views_path() -> str:
    return VIEWS_PATH


@pytest.fixture
def views_path_with_missing_files() -> str:
    renames_short = [
        ("cities — 24h.sql", "citis — 24h.sql"),
        ("countries — 1h.sql", "countris — 1h.sql"),
        ("countries_select.sql", "countris_select.sql"),
    ]

    renames = [
        (
            Path(os.path.join(VIEWS_PATH, "first", src)),
            Path(os.path.join(VIEWS_PATH, "first", dst)),
        )
        for src, dst in renames_short
    ]

    for src, dst in renames:
        src.rename(dst)

    yield VIEWS_PATH

    for src, dst in renames:
        dst.rename(src)


@pytest.fixture
def logger():
    return logzero.logger


@pytest.fixture
def empty_db_str() -> str:
    yield EMPTY_DB_PATH
    try:
        os.remove(EMPTY_DB_PATH)
    except FileNotFoundError:
        pass


@pytest.fixture
def empty_db_cursor():
    connection = sqlite3.connect(EMPTY_DB_PATH, isolation_level=None)
    yield connection.cursor()
    connection.close()
    try:
        os.remove(EMPTY_DB_PATH)
    except FileNotFoundError:
        pass


@pytest.fixture
def db_str() -> str:
    db = create_test_db(PREPARED_DB_PATH)
    yield db
    try:
        os.remove(db)
    except FileNotFoundError:
        pass


@pytest.fixture
def db_connection():
    db = create_test_db(PREPARED_DB_PATH)
    connection = sqlite3.connect(db, isolation_level=None)
    connection.row_factory = sqlite3.Row
    yield connection
    try:
        os.remove(db)
    except FileNotFoundError:
        pass


@pytest.fixture
def db_cursor():
    db = create_test_db(PREPARED_DB_PATH)
    connection = sqlite3.connect(db, isolation_level=None)
    connection.row_factory = sqlite3.Row
    yield connection.cursor()
    connection.close()
    try:
        os.remove(db)
    except FileNotFoundError:
        pass


@pytest.fixture
def empty_git():
    repo_name = "empty repository"
    repo = Repo.init(repo_name)
    yield repo.working_dir
    shutil.rmtree(f"./{repo_name}")


@pytest.fixture
def non_empty_git():
    repo_name = "repository"
    repo = Repo.init(repo_name)
    files = ["first_file.sql", "second_file.sql"]
    for index, f in enumerate(files):
        open(os.path.join(repo_name, f), "wb").close()
        repo.index.add([f])
        repo.index.commit(f"commit #{index}")
    yield repo.working_dir
    shutil.rmtree(f"./{repo_name}")


@pytest.fixture
def empty_config():
    return "configs/empty_config.conf"


@pytest.fixture
def full_config():
    return "configs/full_config.conf"


@pytest.fixture
def partial_config():
    return "configs/partial_config.conf"


@pytest.fixture
def table():
    return Table(
        name="first.cities",
        query="select * from first.countries",
        interval=None,
        config={"grant_select": "user_one", "distkey": "continent"},
    )


@pytest.fixture
def table_without_config():
    return Table(
        name="first.cities", query="select * from first.countries", interval=None
    )


def similar_query(first_query: str, second_query: str, *args) -> bool:
    """
    True if all strings are the same after we remove all spaces,
    tabs, and newlines.
    """
    queries = (first_query, second_query, *args)
    if all(q is None for q in queries):
        return True

    single_lines = [re.sub("[ \t\n]", "", query) for query in queries]
    return all(query == single_lines[0] for query in single_lines[1:])


def pytest_configure():
    pytest.similar = similar_query
