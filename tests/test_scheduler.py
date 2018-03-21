from time import sleep

import pytest

from errors import GitError
from scheduler.commits import (get_all_commits, get_previous_commit,
                               get_latest_new_commit)
from scheduler.graph import build_graph
from scheduler.sqlite import save_commit, build_table_configs
from scheduler.table_config import parse_permissions, parse_table_config


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


def test_parse_permissions():
    global_ = {'grant_select': 'Jane'}
    schema = {'grant_select': 'Tegan, Sara'}
    first_table = {'grant_select': '+Kendrick'}
    second_table = {'grant_select': '-Sara'}
    third_table = {'grant_select': '-Valerie'}
    another_schema = {'a': 42}

    first = [global_, schema, first_table]
    second = [global_, schema, second_table]
    third = [global_, schema, third_table]
    fourth = [global_, another_schema, first_table]

    assert parse_permissions('grant_select',
                             first) == 'Kendrick, Sara, Tegan'
    assert parse_permissions('grant_select',
                             second) == 'Tegan'
    assert parse_permissions('grant_select',
                             third) == 'Sara, Tegan'
    assert parse_permissions('grant_select',
                             [global_, first_table]) == 'Jane, Kendrick'
    assert parse_permissions('grant_select',
                             fourth) == 'Jane, Kendrick'
    assert parse_permissions('another_key', first) == ''


def test_parse_table_config(views_path):
    sc_config = parse_table_config('second.child', views_path)
    assert sc_config['distkey'] == 'city'
    assert sc_config['diststyle'] == 'all'
    assert sc_config.get('grant_select') is None

    fc_config = parse_table_config('first.cities', views_path)
    assert fc_config.get('distkey') is None
    assert fc_config.get('diststyle') is None
    assert fc_config.get('grant_select') == 'jane, john'

    fco_config = parse_table_config('first.countries', views_path)
    assert fco_config.get('distkey') is None
    assert fco_config.get('diststyle') is None
    assert fco_config.get('grant_select') == 'joan, john'
