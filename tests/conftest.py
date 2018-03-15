import pytest
import logzero
import sqlite3
from git import Repo
import shutil
import os


DB_PATH = './test.db'


@pytest.fixture
def views_path() -> str:
    return './views'


@pytest.fixture
def logger():
    return logzero.logger


@pytest.fixture
def db_str() -> str:
    yield DB_PATH
    os.remove(DB_PATH)


@pytest.fixture
def db_cursor():
    connection = sqlite3.connect(DB_PATH, isolation_level=None)
    yield connection.cursor()
    connection.close()


@pytest.fixture
def empty_git():
    repo_name = 'empty repository'
    repo = Repo.init(repo_name)
    print(repo.git_dir)
    yield repo.working_dir
    shutil.rmtree(f'./{repo_name}')


@pytest.fixture
def non_empty_git():
    repo_name = 'repository'
    repo = Repo.init(repo_name)
    files = ['first_file.sql', 'second_file.sql']
    for index, f in enumerate(files):
        open(os.path.join(repo_name, f), 'wb').close()
        repo.index.add([f])
        repo.index.commit(f'commit #{index}')
    yield repo.working_dir
    shutil.rmtree(f'./{repo_name}')

