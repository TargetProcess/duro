from file_utils import parse_filename
import pytest


def test_parse_filename():
    with pytest.raises(ValueError, message='No schema specified'):
        assert parse_filename('companies.sql') == ('companies', None)
    assert parse_filename('custom.companies.sql') == ('custom.companies', None)
    assert parse_filename('custom/companies.sql') == ('custom.companies', None)
    assert parse_filename('custom/companies — 1m.sql') == ('custom.companies', '1m')
    assert parse_filename('custom/custom.companies.sql') == ('custom.companies', None)
    assert parse_filename('feedback/custom.companies.sql') == ('custom.companies', None)
    assert parse_filename('custom/custom.companies — 1h.sql') == ('custom.companies', '1h')
    assert parse_filename('feedback/custom.companies - 1h.sql') == ('custom.companies', '1h')

