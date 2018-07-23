import pytest

from utils.checks import find_tables_with_missing_files


def test_find_tables_with_missing_files(views_path_with_missing_files):
    errors = find_tables_with_missing_files(views_path_with_missing_files)
    assert pytest.similar(
        errors,
        """
        Some tables have tests, but not a SELECT query: first.countries, first.cities.
        Some processors don’t have a SELECT query: first.countries.
        Some processors don’t have a CREATE TABLE query: first.countries.""",
    )


def test_dont_find_tables_with_missing_files(views_path):
    no_errors = find_tables_with_missing_files(views_path)
    assert no_errors == ""
