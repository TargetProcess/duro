from create.table_config import parse_permissions


def test_parse_permissions():
    global_ = {'grant_select': 'fourth'}
    schema = {'grant_select': 'first, second'}
    first_table = {'grant_select': '+third'}
    second_table = {'grant_select': '-second'}
    third_table = {'grant_select': '-fifth'}
    another_schema = {'a': 42}

    first = [global_, schema, first_table]
    second = [global_, schema, second_table]
    third = [global_, schema, third_table]
    fourth = [global_, another_schema, first_table]

    assert parse_permissions('grant_select',
                             first) == 'first, second, third'
    assert parse_permissions('grant_select',
                             second) == 'first'
    assert parse_permissions('grant_select',
                             third) == 'first, second'
    assert parse_permissions('grant_select',
                             [global_, first_table]) == 'fourth, third'
    assert parse_permissions('grant_select',
                             fourth) == 'fourth, third'
    assert parse_permissions('another_key', first) == ''
