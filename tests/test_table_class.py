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

    empty_table = Table("empty", "", None)
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
        create table second.child_duro_temp
        distkey("city")  diststyle all
        as (select city, country from first.cities);""",
    )

    parent = load_table_details(db_str, "second.parent")
    query = parent.get_query_with_dist_sort_keys()
    assert pytest.similar(
        query,
        """
        create table second.parent_duro_temp diststyle even
        as (select * from second.child limit 10);""",
    )


def test_load_grant_select_statements(db_str):
    child = load_table_details(db_str, "second.child")
    grant = child.load_grant_select_statements()
    assert grant == ""

    cities = load_table_details(db_str, "first.cities")
    grant = cities.load_grant_select_statements()
    assert pytest.similar(grant, "grant select on first.cities_duro_temp to jane, john")

    countries = load_table_details(db_str, "first.countries")
    grant = countries.load_grant_select_statements()
    assert pytest.similar(
        grant, "grant select on first.countries_duro_temp to joan, john"
    )


def test_has_snapshots(db_str):
    child = load_table_details(db_str, "second.child")
    assert child.store_snapshots is True

    parent = load_table_details(db_str, "second.parent")
    assert parent.store_snapshots is False
