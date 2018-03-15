import pytest

from errors import GitError
from schedule.commits import get_all_commits, get_previous_commit
from schedule.sqlite import save_commit
from time import sleep


def test_get_all_commits(empty_git, non_empty_git):
    assert get_all_commits(empty_git) == []
    assert len(get_all_commits(non_empty_git)) == 2
    with pytest.raises(GitError):
        get_all_commits('.')


def test_save_and_read_commit(db_str, db_cursor):
    commits = ['063fac57a03eadfd5077e2c972504426916769ab',
               '92012fc409ee64934fc10c8cea54ce9ef6e2114b']
    save_commit(commits[0], db_cursor)
    commit = get_previous_commit(db_str)
    assert commit == commits[0]
    sleep(1)
    save_commit(commits[1], db_cursor)
    commit = get_previous_commit(db_str)
    assert commit == commits[1]
