from time import sleep

import pytest

from errors import GitError
from scheduler.commits import (get_all_commits, get_previous_commit,
                               get_latest_new_commit)
from scheduler.sqlite import save_commit, build_table_configs


def test_get_all_commits(empty_git, non_empty_git):
    assert get_all_commits(empty_git) == []
    assert len(get_all_commits(non_empty_git)) == 2
    with pytest.raises(GitError):
        get_all_commits('.')


def test_save_and_read_commit(db_str, db_cursor):
    commits = ['063fac57a03eadfd5077e2c972504426916769ab',
               '92012fc409ee64934fc10c8cea54ce9ef6e2114b']
    assert get_latest_new_commit(commits, db_str) == commits[0]
    save_commit(commits[1], db_cursor)
    commit = get_previous_commit(db_str)
    assert commit == commits[1]
    sleep(1)
    assert get_latest_new_commit(commits, db_str) == commits[0]
    save_commit(commits[0], db_cursor)
    commit = get_previous_commit(db_str)
    assert commit == commits[0]
    assert get_latest_new_commit(commits, db_str) is None


def test_build_table_configs(views_path, graph):
    configs = build_table_configs(graph, views_path)
    print(configs)
