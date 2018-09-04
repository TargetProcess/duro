import pytest

from scheduler.query import is_table_used_in_query, remove_comments


def test_remove_comments():
    assert remove_comments("SELECT * from schema.table") == "SELECT * from schema.table"
    assert remove_comments("SELECT * from --schema.table") == "SELECT * from "
    assert pytest.similar(
        remove_comments(
            """SELECT * from schem.table
            -- from schema.table"""
        ),
        "SELECT * from schem.table",
    )
    assert pytest.similar(
        remove_comments(
            """SELECT *
            from schema.table -- from schema.table2"""
        ),
        """SELECT * from schema.table""",
    )


def test_is_table_used_in_query():
    table = "schema.table"

    queries = [
        ("SELECT * from schema.table", True),
        ("SELECT * from schema.table2", False),
        ("SELECT * from schema2.table", False),
        ("SELECT * from --schema.table", False),
        (
            """SELECT * from schem.table
            -- from schema.table""",
            False,
        ),
        (
            """SELECT * from schema.table
            -- from schema.table""",
            True,
        ),
        (
            """SELECT *
           from schema.table -- from schema.table2""",
            True,
        ),
        (
            """SELECT *
           from schema2.table -- from schema.table""",
            False,
        ),
        ('SELECT * from "schema".table', True),
        ('SELECT * from "schema"."table"', True),
        ('SELECT * from schema."table"', True),
        ('SELECT * from "schema2"."table"', False),
        ("SELECT * from a_schema.table", False),
    ]

    results = [(query, is_table_used_in_query(table, query)) for query, _ in queries]

    assert results == queries
