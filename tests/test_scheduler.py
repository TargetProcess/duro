from time import sleep

import pytest

from errors import GitError
from scheduler.commits import (get_all_commits, get_previous_commit,
                               get_latest_new_commit)
from scheduler.graph import build_graph
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


def test_build_graph(views_path):
    graph = build_graph(views_path)
    assert graph.nodes() == ['first.cities', 'first.countries',
                             'second.child', 'second.parent']
    assert graph.edges() == [('second.child', 'first.cities'),
                             ('second.parent', 'second.child')]

    second_parent = [n for n in graph.nodes(data=True)
                     if n[0] == 'second.parent'][0]
    assert second_parent[1]['contents'] == 'select * from second.child limit 10'
    assert second_parent[1]['interval'] == 24


def test_build_table_configs(views_path):
    graph_with_queries = build_graph(views_path)
    configs = build_table_configs(graph_with_queries, views_path)

    second_parent = [t for t in configs
                     if t.name == 'second.parent'][0]
    assert second_parent.query == 'select * from second.child limit 10'
    assert second_parent.interval == 24
    assert second_parent.config == {}

    second_child = [t for t in configs
                    if t.name == 'second.child'][0]
    assert second_child.query == 'select city, country from first.cities'
    assert second_child.interval is None
    assert second_child.config == {'distkey': 'city', 'diststyle': 'all'}
