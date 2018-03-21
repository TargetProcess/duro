import os

import pytest

from utils.file_utils import (parse_filename, convert_interval_to_integer,
                              is_processor_ddl, is_query, load_ddl_query,
                              load_processor, load_query, parse_view,
                              list_view_files)


def filenames(views_path):
    return [
        os.path.join(views_path, 'first', 'countries — 1h.sql'),
        os.path.join(views_path, 'first', 'countries_ddl.sql'),
        os.path.join(views_path, 'first', 'cities_test.sql'),
        os.path.join(views_path, 'first', 'non-existent.sql'),
        os.path.join(views_path, 'second', 'child.sql'),
        os.path.join(views_path, 'second', 'child.conf'),
        '',
        None
    ]


def test_parse_filename():
    with pytest.raises(ValueError, message='No schema specified'):
        assert parse_filename('cities.sql')
    assert parse_filename('first.cities.sql') == ('first.cities', None)
    assert parse_filename('first/cities.sql') == ('first.cities', None)
    assert parse_filename('first/cities — 1m.sql') == ('first.cities', '1m')
    assert parse_filename('first/first.cities.sql') == ('first.cities', None)
    assert parse_filename('second/first.cities.sql') == ('first.cities', None)
    assert parse_filename('first/first.cities — 1h.sql') == ('first.cities', '1h')
    assert parse_filename('second/first.cities - 1h.sql') == ('first.cities', '1h')


def test_parse_view(views_path):
    name = os.path.join(views_path, 'first', 'cities — 24h.sql')
    cities = parse_view(name, views_path)
    assert cities.filename == name
    assert cities.table == 'first.cities'
    assert cities.interval == '24h'
    assert cities.contents == '''select city, country
from first.cities_raw'''


def test_list_view_files(views_path):
    views = list_view_files(views_path)
    assert len(views) == 4

    s_child = [v for v in views if v[0] == 'second.child'][0]
    assert s_child[1]['contents'] == 'select city, country from first.cities'
    assert s_child[1]['interval'] is None

    s_parent = [v for v in views if v[0] == 'second.parent'][0]
    assert s_parent[1]['contents'] == 'select * from second.child limit 10'
    assert s_parent[1]['interval'] == 24

    f_countries = [v for v in views if v[0] == 'first.countries'][0]
    assert f_countries[1]['contents'] == 'select country, continent\nfrom first.countries_raw;'
    assert f_countries[1]['interval'] == 60


def test_convert_interval_to_integer():
    assert convert_interval_to_integer('1m') == 1
    assert convert_interval_to_integer('30m') == 30
    assert convert_interval_to_integer('4h') == 240
    assert convert_interval_to_integer('1d') == 1440
    assert convert_interval_to_integer('1w') == 10080
    assert convert_interval_to_integer(None) is None

    with pytest.raises(ValueError, message='Invalid interval'):
        assert convert_interval_to_integer('1z')
        assert convert_interval_to_integer('')
        assert convert_interval_to_integer('asdf')


def test_is_processor_ddl(views_path):
    results = [is_processor_ddl(f)
               for f in filenames(views_path)]

    assert results == [False, True, False,
                       False, False, False,
                       False, False]


def test_is_query(views_path):
    results = [is_query(f)
               for f in filenames(views_path)]

    assert results == [True, False, False,
                       False, True, False,
                       False, False]


def test_load_ddl_query(views_path):
    ddl = load_ddl_query('first.countries', views_path)
    assert ddl == '''create table first.countries_temp(
city text,
country text
)'''

    with pytest.raises(ValueError):
        load_ddl_query('first.cities', views_path)


def test_load_processor(views_path):
    processor = load_processor('first.countries', views_path)
    assert processor == './views/first/countries.py'

    assert load_processor('first.cities', views_path) == ''


def test_load_query(views_path):
    query = load_query('second.child', views_path)
    assert query == 'select city, country from first.cities'

    query = load_query('second.parent', views_path)
    assert query == 'select * from second.child limit 10'

    with pytest.raises(ValueError):
        load_query('second.non-existent.sql', views_path)
