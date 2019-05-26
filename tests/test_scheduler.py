from time import sleep

import pytest

from errors import GitError
from scheduler.commits import (
    get_all_commits,
    get_previous_commit,
    get_latest_new_commit,
)
from scheduler.graph import build_graph
from scheduler.sqlite import (
    save_commit,
    build_table_configs,
    is_already_in_db,
    insert_table,
    update_table,
    create_tables_table,
    should_be_updated,
    save_to_db,
)
from scheduler.table_config import parse_permissions, parse_table_config
from utils.file_utils import load_tables_in_path
from utils.utils import Table


def test_get_all_commits(empty_git, non_empty_git):
    assert get_all_commits(empty_git) == []
    assert len(get_all_commits(non_empty_git)) == 2
    with pytest.raises(GitError):
        get_all_commits(".")


def test_save_and_read_commit(empty_db_str, empty_db_cursor):
    commits = [
        "063fac57a03eadfd5077e2c972504426916769ab",
        "92012fc409ee64934fc10c8cea54ce9ef6e2114b",
    ]
    assert get_latest_new_commit(commits, empty_db_str) == commits[0]
    save_commit(commits[1], empty_db_cursor)
    commit = get_previous_commit(empty_db_str)
    assert commit == commits[1]
    sleep(1)
    assert get_latest_new_commit(commits, empty_db_str) == commits[0]
    save_commit(commits[0], empty_db_cursor)
    commit = get_previous_commit(empty_db_str)
    assert commit == commits[0]
    assert get_latest_new_commit(commits, empty_db_str) is None


def test_build_graph(views_path):
    tables = load_tables_in_path(views_path)
    graph = build_graph(tables)
    assert graph.nodes() == [
        "first.cities",
        "first.countries",
        "first.countries_detailed",
        "second.child",
        "second.parent",
    ]
    assert graph.edges() == [
        ("first.countries_detailed", "first.countries"),
        ("second.child", "first.cities"),
        ("second.parent", "second.child"),
    ]

    second_parent = [n for n in graph.nodes(data=True) if n[0] == "second.parent"][0]
    assert second_parent[1]["contents"] == "select * from second.child limit 10"
    assert second_parent[1]["interval"] == 24


def test_build_table_configs(views_path):
    tables = load_tables_in_path(views_path)
    graph_with_queries = build_graph(tables)
    configs = build_table_configs(graph_with_queries, views_path)

    second_parent = [t for t in configs if t.name == "second.parent"][0]
    assert second_parent.query == "select * from second.child limit 10"
    assert second_parent.interval == 24
    assert second_parent.config == {"diststyle": "even"}

    second_child = [t for t in configs if t.name == "second.child"][0]
    assert second_child.query == "select city, country from first.cities"
    assert second_child.interval is None
    assert second_child.config == {"distkey": "city", "diststyle": "all"}


def test_parse_permissions():
    global_ = {"grant_select": "Jane"}
    schema = {"grant_select": "Tegan, Sara"}
    first_table = {"grant_select": "+Kendrick"}
    second_table = {"grant_select": "-Sara"}
    third_table = {"grant_select": "-Valerie"}
    another_schema = {"a": 42}

    first = [global_, schema, first_table]
    second = [global_, schema, second_table]
    third = [global_, schema, third_table]
    fourth = [global_, another_schema, first_table]

    assert parse_permissions("grant_select", first) == "Kendrick, Sara, Tegan"
    assert parse_permissions("grant_select", second) == "Tegan"
    assert parse_permissions("grant_select", third) == "Sara, Tegan"
    assert parse_permissions("grant_select", [global_, first_table]) == "Jane, Kendrick"
    assert parse_permissions("grant_select", fourth) == "Jane, Kendrick"
    assert parse_permissions("another_key", first) == ""


def test_parse_table_config(views_path):
    sc_config = parse_table_config("second.child", views_path)
    assert sc_config["distkey"] == "city"
    assert sc_config["diststyle"] == "all"
    assert sc_config.get("grant_select") is None

    fc_config = parse_table_config("first.cities", views_path)
    assert fc_config.get("distkey") is None
    assert fc_config.get("diststyle") is None
    assert fc_config.get("grant_select") == "jane, john"

    fco_config = parse_table_config("first.countries", views_path)
    assert fco_config.get("distkey") is None
    assert fco_config.get("diststyle") is None
    assert fco_config.get("grant_select") == "joan, john"


def test_create_tables_table(empty_db_cursor):
    create_tables_table(empty_db_cursor)

    empty_db_cursor.execute(
        """
        select count(*) from sqlite_master 
        where type='table'
        and name = 'tables';
    """
    )

    assert empty_db_cursor.fetchone()[0] == 1
    empty_db_cursor.execute("select * from tables")


def test_is_already_in_db(empty_db_cursor):
    assert is_already_in_db("first.cities", empty_db_cursor) is False

    create_tables_table(empty_db_cursor)
    empty_db_cursor.execute(
        """
        INSERT INTO tables
        (table_name) 
        values ('first.coutries');
    """
    )
    assert is_already_in_db("first.cities", empty_db_cursor) is False

    empty_db_cursor.execute(
        """
        INSERT INTO tables
        (table_name) 
        values ('first.cities');
    """
    )
    assert is_already_in_db("first.cities", empty_db_cursor) is True


def test_insert_table(empty_db_cursor, views_path):
    create_tables_table(empty_db_cursor)

    sample_table = Table("schema.table", "select", 40)
    insert_table(sample_table, empty_db_cursor)

    assert is_already_in_db("schema.table", empty_db_cursor) is True
    row = empty_db_cursor.execute("select * from tables").fetchone()
    assert row[0] == "schema.table"
    assert row[1] == "select"
    assert row[2] == 40
    assert row[3] is None
    assert row[7] == 1

    tables = load_tables_in_path(views_path)
    graph_with_queries = build_graph(tables)
    configs = build_table_configs(graph_with_queries, views_path)
    second_parent = [t for t in configs if t.name == "second.parent"][0]
    insert_table(second_parent, empty_db_cursor)

    assert is_already_in_db("second.parent", empty_db_cursor) is True
    row = empty_db_cursor.execute(
        """
        select * from tables 
        where table_name = 'second.parent'
    """
    ).fetchone()
    assert row[0] == "second.parent"
    assert row[1] == "select * from second.child limit 10"
    assert row[2] == 24
    assert row[3] == '{"diststyle": "even"}'
    assert row[7] == 1


def test_should_be_updated(empty_db_cursor):
    create_tables_table(empty_db_cursor)
    table = Table("schema.table", "select", 40, {"key": "value"})
    assert should_be_updated(table, empty_db_cursor) is True

    insert_table(table, empty_db_cursor)

    assert should_be_updated(table, empty_db_cursor) is False

    table.interval = 20
    assert should_be_updated(table, empty_db_cursor) is True

    table.interval = 40
    table.name = "new name"
    assert should_be_updated(table, empty_db_cursor) is True


def test_update_table(empty_db_cursor):
    create_tables_table(empty_db_cursor)
    table = Table("schema.table", "select", 40, {"key": "value"})
    insert_table(table, empty_db_cursor)

    assert update_table(table, empty_db_cursor) is None

    table.interval = 20
    assert update_table(table, empty_db_cursor) == "schema.table"
    row = empty_db_cursor.execute("select * from tables").fetchone()
    assert row[0] == "schema.table"
    assert row[1] == "select"
    assert row[2] == 20
    assert row[7] == 1


def test_save_to_db(empty_db_str, views_path, empty_db_cursor):
    tables = load_tables_in_path(views_path)
    graph = build_graph(tables)
    save_to_db(graph, empty_db_str, views_path, None)
    empty_db_cursor.execute(
        """select * from tables 
                where table_name = 'second.parent'
                """
    )
    second_parent = empty_db_cursor.fetchone()
    assert second_parent[0] == "second.parent"
    assert second_parent[1] == "select * from second.child limit 10"
    assert second_parent[2] == 24
    assert second_parent[3] == '{"diststyle": "even"}'
    assert second_parent[7] == 1

    graph.add_node("schema.table", {"contents": "select", "interval": 40})
    save_to_db(graph, empty_db_str, views_path, None)
    empty_db_cursor.execute(
        """select * from tables 
                    where table_name = 'schema.table'
                    """
    )
    table = empty_db_cursor.fetchone()
    assert table[0] == "schema.table"
    assert table[1] == "select"
    assert table[2] == 40
    assert table[3] is None

    empty_db_cursor.execute("select count(*) from tables")
    assert empty_db_cursor.fetchone()[0] == 6

    save_to_db(graph, empty_db_str, views_path, "commit_hash")
    empty_db_cursor.execute("select * from commits")
    commit = empty_db_cursor.fetchone()
    assert commit[0] == "commit_hash"

    graph.remove_node("schema.table")
    save_to_db(graph, empty_db_str, views_path, "commit_hash")
    empty_db_cursor.execute(
        """select count(*) from tables 
                        where table_name = 'schema.table'
                        and deleted is not null
                        """
    )
    assert empty_db_cursor.fetchone()[0] == 1
