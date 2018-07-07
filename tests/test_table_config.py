import pytest

from create.sqlite import load_info
from create.table_config import (load_dist_sort_keys, add_dist_sort_keys,
                                 load_grant_select_statements)


def test_load_dist_sort_keys(db_str):
    child = load_info(db_str, 'second.child')
    keys = load_dist_sort_keys(child.config)
    assert keys.distkey == 'distkey("city")'
    assert keys.diststyle == 'diststyle all'
    assert keys.sortkey == ''

    parent = load_info(db_str, 'second.parent')
    keys = load_dist_sort_keys(parent.config)
    assert keys.distkey == ''
    assert keys.diststyle == 'diststyle even'
    assert keys.sortkey == ''

    empty = load_dist_sort_keys({})
    assert empty.distkey == ''
    assert empty.diststyle == ''
    assert empty.sortkey == ''


# noinspection PyUnresolvedReferences
def test_add_dist_sort_keys(db_str):
    child = load_info(db_str, 'second.child')
    query = add_dist_sort_keys(child.name, child.query, child.config)
    assert pytest.similar(query, '''
        CREATE TABLE second.child_temp
        distkey("city")  diststyle all
        AS (select city, country from first.cities);''')

    child = load_info(db_str, 'second.parent')
    query = add_dist_sort_keys(child.name, child.query, child.config)
    assert pytest.similar(query, '''
        CREATE TABLE second.parent_temp diststyle even
        AS (select * from second.child limit 10);''')


# noinspection PyUnresolvedReferences
def test_load_grant_select_statements(db_str):
    child = load_info(db_str, 'second.child')
    grant = load_grant_select_statements(child.name, child.config)
    assert grant == ''

    cities = load_info(db_str, 'first.cities')
    grant = load_grant_select_statements(cities.name, cities.config)
    assert grant == 'GRANT SELECT ON first.cities_temp TO jane, john'

    countries = load_info(db_str, 'first.countries')
    grant = load_grant_select_statements(countries.name, countries.config)
    assert grant == 'GRANT SELECT ON first.countries_temp TO joan, john'
