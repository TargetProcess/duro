from scheduler.table_config import parse_permissions


def test_parse_permissions():
    global_ = {'grant_select': 'Jane'}
    schema = {'grant_select': 'Tegan, Sara'}
    first_table = {'grant_select': '+Kendrick'}
    second_table = {'grant_select': '-Sara'}
    third_table = {'grant_select': '-Valerie'}
    another_schema = {'a': 42}

    first = [global_, schema, first_table]
    second = [global_, schema, second_table]
    third = [global_, schema, third_table]
    fourth = [global_, another_schema, first_table]

    assert parse_permissions('grant_select',
                             first) == 'Kendrick, Sara, Tegan'
    assert parse_permissions('grant_select',
                             second) == 'Tegan'
    assert parse_permissions('grant_select',
                             third) == 'Sara, Tegan'
    assert parse_permissions('grant_select',
                             [global_, first_table]) == 'Jane, Kendrick'
    assert parse_permissions('grant_select',
                             fourth) == 'Jane, Kendrick'
    assert parse_permissions('another_key', first) == ''
