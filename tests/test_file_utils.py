import os

import pytest

from utils.file_utils import (
    parse_filename,
    convert_interval_to_integer,
    is_processor_ddl,
    is_query,
    load_ddl_query,
    load_processor,
    load_query,
    load_table_from_file,
    load_tables_in_path,
    list_tests,
    is_test,
    is_sql_query,
    is_processor,
    is_processor_select_query,
    has_processor,
    list_files,
    load_select_query,
)


def filenames(views_path):
    return [
        os.path.join(views_path, "first", "countries — 1h.sql"),
        os.path.join(views_path, "first", "countries_select.sql"),
        os.path.join(views_path, "first", "countries.py"),
        os.path.join(views_path, "first", "cities_test.sql"),
        os.path.join(views_path, "first", "non-existent.sql"),
        os.path.join(views_path, "second", "child.sql"),
        os.path.join(views_path, "second", "child.conf"),
        "",
        None,
    ]


def test_parse_filename():
    with pytest.raises(ValueError, message="No schema specified"):
        assert parse_filename("cities.sql")
    assert parse_filename("first.cities.sql") == ("first.cities", None)
    assert parse_filename("first/cities.sql") == ("first.cities", None)
    assert parse_filename("first/cities — 1m.sql") == ("first.cities", "1m")
    assert parse_filename("first/first.cities.sql") == ("first.cities", None)
    assert parse_filename("second/first.cities.sql") == ("first.cities", None)
    assert parse_filename("first/first.cities — 1h.sql") == ("first.cities", "1h")
    assert parse_filename("second/first.cities - 1h.sql") == ("first.cities", "1h")


def test_load_table_from_file(views_path):
    name = os.path.join(views_path, "first", "cities — 24h.sql")
    cities = load_table_from_file(views_path, name)
    assert cities.filename == name
    assert cities.table == "first.cities"
    assert cities.interval == "24h"
    assert pytest.similar(
        cities.select_query,
        """select city, country 
                             from first.cities_raw""",
    )

    name = os.path.join(views_path, "first", "countries — 1h.sql")
    cities = load_table_from_file(views_path, name)
    assert cities.filename == name
    assert cities.table == "first.countries"
    assert cities.interval == "1h"
    assert pytest.similar(
        cities.select_query,
        """select country, continent
                             from first.countries_raw;""",
    )


def test_list_view_files(views_path):
    views = load_tables_in_path(views_path)
    assert len(views) == 4

    s_child = [v for v in views if v[0] == "second.child"][0]
    assert s_child[1]["contents"] == "select city, country from first.cities"
    assert s_child[1]["interval"] is None

    s_parent = [v for v in views if v[0] == "second.parent"][0]
    assert s_parent[1]["contents"] == "select * from second.child limit 10"
    assert s_parent[1]["interval"] == 24

    f_countries = [v for v in views if v[0] == "first.countries"][0]
    assert (
        f_countries[1]["contents"]
        == "select country, continent\nfrom first.countries_raw;"
    )
    assert f_countries[1]["interval"] == 60


def test_convert_interval_to_integer():
    assert convert_interval_to_integer("1m") == 1
    assert convert_interval_to_integer("30m") == 30
    assert convert_interval_to_integer("4h") == 240
    assert convert_interval_to_integer("1d") == 1440
    assert convert_interval_to_integer("1w") == 10080
    assert convert_interval_to_integer(None) is None

    with pytest.raises(ValueError, message="Invalid interval"):
        assert convert_interval_to_integer("1z")
        assert convert_interval_to_integer("")
        assert convert_interval_to_integer("asdf")


def test_is_processor_ddl(views_path):
    results = [is_processor_ddl(f) for f in filenames(views_path)]

    assert results == [True, False, False, False, False, False, False, False, False]


def test_is_query(views_path):
    results = [is_query(f) for f in filenames(views_path)]

    assert results == [True, False, False, False, False, True, False, False, False]


def test_has_processor(views_path):
    results = [has_processor(f) for f in filenames(views_path)]

    assert results == [True, False, True, False, False, False, False, False, False]


def test_is_processor_select_query(views_path):
    results = [is_processor_select_query(f) for f in filenames(views_path)]
    assert results == [False, True, False, False, False, False, False, False, False]


def test_is_test():
    assert is_test("") is False
    assert is_test(None) is False
    assert is_test("table_tst.sql") is False
    assert is_test("table_test.sql") is True


def test_is_processor():
    assert is_processor("") is False
    assert is_test(None) is False
    assert is_processor("table.py") is True
    assert is_processor("table.sql") is False


def test_is_sql_query():
    assert is_sql_query("") is False
    assert is_sql_query(None) is False
    assert is_sql_query("table.py") is False
    assert is_sql_query("table.sql") is True


def test_load_ddl_query(views_path):
    ddl = load_ddl_query(views_path, "first.countries")
    assert pytest.similar(
        ddl,
        """create table first.countries_duro_temp(
                            city text,
                            country text
                            )""",
    )

    with pytest.raises(ValueError):
        load_ddl_query(views_path, "first.cities")


def test_load_processor(views_path):
    processor = load_processor(views_path, "first.countries")
    assert processor == "./views/first/countries.py"

    assert load_processor(views_path, "first.cities") == ""


def test_load_query(views_path):
    query = load_query(views_path, "second.child")
    assert query == "select city, country from first.cities"

    query = load_query(views_path, "second.parent")
    assert query == "select * from second.child limit 10"

    with pytest.raises(ValueError):
        load_query(views_path, "second.non-existent.sql")


def test_load_select_query(views_path):
    countries = load_select_query(views_path, "first.countries")
    assert pytest.similar(
        countries,
        """
            select country, continent
            from first.countries_raw;""",
    )

    child = load_select_query(views_path, "second.child")
    assert child == "select city, country from first.cities"


def test_list_files(views_path):
    sql_files = list_files(views_path, mask="*.sql")
    assert len(sql_files) == 7
    assert sql_files[1] == "./views/first/cities_test.sql"

    all_files = list_files(views_path, mask="*")
    assert len(all_files) == 14

    no_files = list_files(views_path, match=lambda x: False)
    assert no_files == []

    test_files = list_files(views_path, match=is_test)
    assert test_files == [
        "./views/first.countries_test.sql",
        "./views/first/cities_test.sql",
    ]

    python_processors = list_files(views_path, match=is_processor, mask="*.py")
    assert python_processors == ["./views/first/countries.py"]

    processor_selects = list_files(views_path, match=is_processor_select_query)
    assert processor_selects == ["./views/first/countries_select.sql"]


def test_list_tests(views_path):
    tests = list_tests(views_path)
    assert tests == ["first.countries_test", "first.cities_test"]
