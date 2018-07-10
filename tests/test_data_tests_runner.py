import pytest

from create.data_tests import parse_tests_results, load_tests


def test_parse_tests_results():
    results = [('First test', True), ('Second test', True)]
    assert parse_tests_results(results) == (True, None)
    results = [('First test', False), ('Second test', True)]
    assert parse_tests_results(results) == (False, ['First test'])


# noinspection PyUnresolvedReferences
def test_load_tests(views_path):
    cities_reference = '''
        select (city = 'Paris') as correct_capital_of_france
        from first.cities_temp
        where country = 'France';

        select (city = 'Ottawa')  as correct_capital_of_canada
        from first.cities_temp
        where country = 'Canada';
    '''
    cities_tests = load_tests('first.cities', views_path)
    assert pytest.similar(cities_reference, cities_tests)

    countries_reference = '''
        select (continent = 'Europe') as correct_continent_for_france
        from first.countries_temp
        where country = 'France';
    '''

    countries_tests = load_tests('first.countries', views_path)
    assert pytest.similar(countries_reference, countries_tests)
