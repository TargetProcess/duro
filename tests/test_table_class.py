import pytest

from duro.create.sqlite import load_table_details
from duro.utils.table import Table


def test_load_dist_sort_keys(db_str):
    child = load_table_details(db_str, "second.child")
    keys = child.load_dist_sort_keys()
    assert keys.distkey == 'distkey("city")'
    assert keys.diststyle == "diststyle all"
    assert keys.sortkey == ""

    parent = load_table_details(db_str, "second.parent")
    keys = parent.load_dist_sort_keys()
    assert keys.distkey == ""
    assert keys.diststyle == "diststyle even"
    assert keys.sortkey == ""

    empty_table = Table('empty', '', None)
    empty_config = empty_table.load_dist_sort_keys()
    assert empty_config.distkey == ""
    assert empty_config.diststyle == ""
    assert empty_config.sortkey == ""


# noinspection PyUnresolvedReferences
def test_get_query_with_dist_sort_keys(db_str):
    child = load_table_details(db_str, "second.child")
    query = child.get_query_with_dist_sort_keys()
    assert pytest.similar(
        query,
        """
        CREATE TABLE second.child_duro_temp
        distkey("city")  diststyle all
        AS (select city, country from first.cities);""",
    )

    parent = load_table_details(db_str, "second.parent")
    query = parent.get_query_with_dist_sort_keys()
    assert pytest.similar(
        query,
        """
        CREATE TABLE second.parent_duro_temp diststyle even
        AS (select * from second.child limit 10);""",
    )


# noinspection PyUnresolvedReferences
def test_load_grant_select_statements(db_str):
    child = load_table_details(db_str, "second.child")
    grant = child.load_grant_select_statements()
    assert grant == ""

    cities = load_table_details(db_str, "first.cities")
    grant = cities.load_grant_select_statements()
    assert grant == "GRANT SELECT ON first.cities_duro_temp TO jane, john"

    countries = load_table_details(db_str, "first.countries")
    grant = countries.load_grant_select_statements()
    assert grant == "GRANT SELECT ON first.countries_duro_temp TO joan, john"
