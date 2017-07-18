from utils.file_utils import parse_filename, convert_interval_to_integer
import pytest


def test_parse_filename():
    with pytest.raises(ValueError, message='No schema specified'):
        assert parse_filename('companies.sql')
    assert parse_filename('custom.companies.sql') == ('custom.companies', None)
    assert parse_filename('custom/companies.sql') == ('custom.companies', None)
    assert parse_filename('custom/companies — 1m.sql') == ('custom.companies', '1m')
    assert parse_filename('custom/custom.companies.sql') == ('custom.companies', None)
    assert parse_filename('feedback/custom.companies.sql') == ('custom.companies', None)
    assert parse_filename('custom/custom.companies — 1h.sql') == ('custom.companies', '1h')
    assert parse_filename('feedback/custom.companies - 1h.sql') == ('custom.companies', '1h')


def test_convert_interval_to_integer():
    assert convert_interval_to_integer('1m') == 1
    assert convert_interval_to_integer('30m') == 30
    assert convert_interval_to_integer('4h') == 240
    assert convert_interval_to_integer('1d') == 1440
    assert convert_interval_to_integer('1w') == 10080
    # noinspection PyTypeChecker
    assert convert_interval_to_integer(None) is None

    with pytest.raises(ValueError, message='Invalid interval'):
        assert convert_interval_to_integer('1z')
        assert convert_interval_to_integer('')
        assert convert_interval_to_integer('asdf')
