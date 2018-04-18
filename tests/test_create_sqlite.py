import pytest
import arrow

from create.sqlite import (load_info, update_last_created,
                           log_start, log_timestamps,
                           reset_start, reset_all_starts,
                           set_waiting, is_waiting,
                           is_running, get_time_running,
                           get_time_waiting, get_average_completion_time,
                           build_query_to_create_timestamps_table,
                           get_tables_to_create)
from create.timestamps import Timestamps
from errors import TableNotFoundInDBError
from server.sqlite import set_table_for_update, get_table_details


def test_load_info(db_str):
    first = load_info(db_str, 'first.cities')
    assert first.name == 'first.cities'
    assert first.interval == 1440
    assert first.waiting is None
    assert first.config['grant_select'] == 'jane, john'

    with pytest.raises(TableNotFoundInDBError):
        load_info(db_str, 'non-existent')


# noinspection PyTypeChecker
def test_update_last_created(db_str, db_cursor):
    update_last_created(db_str, 'first.cities', 1522151698, 127)

    db_cursor.execute('''
        SELECT last_created, mean,
            times_run, started, force, waiting
        FROM tables
        WHERE table_name = 'first.cities'
    ''')

    row = db_cursor.fetchone()
    assert row['last_created'] == 1522151698
    assert row['waiting'] is None
    assert row['force'] is None
    assert row['started'] is None
    assert row['mean'] == 127
    assert row['times_run'] == 1

    update_last_created(db_str, 'first.cities', 1522151835, 367)

    db_cursor.execute('''
            SELECT last_created, mean,
                times_run, started, force, waiting
            FROM tables
            WHERE table_name = 'first.cities'
        ''')

    row = db_cursor.fetchone()
    assert row['last_created'] == 1522151835
    assert row['waiting'] is None
    assert row['force'] is None
    assert row['started'] is None
    assert row['mean'] == 247
    assert row['times_run'] == 2


def test_get_average_completion_time(db_str):
    update_last_created(db_str, 'first.cities', 1522151698, 127)
    assert get_average_completion_time(db_str, 'first.cities') == 127

    update_last_created(db_str, 'first.cities', 1522151835, 367)
    assert get_average_completion_time(db_str, 'first.cities') == 247

    update_last_created(db_str, 'first.countries', 1522151835, 367)
    assert get_average_completion_time(db_str, 'first.cities') == 247
    assert get_average_completion_time(db_str, 'first.countries') == 367

    update_last_created(db_str, 'first.cities', 1522151895, 406)
    assert get_average_completion_time(db_str, 'first.cities') == 300


def test_log_start(db_str, db_cursor):
    log_start(db_str, 'first.cities', 1522151835)

    db_cursor.execute('''
        SELECT started 
        FROM tables
        WHERE table_name == 'first.cities'
    ''')
    assert db_cursor.fetchone()[0] == 1522151835

    log_start(db_str, 'non-existent', 1522151835)
    db_cursor.execute('''
        SELECT started 
        FROM tables
        WHERE table_name == 'first.cities'
    ''')
    assert db_cursor.fetchone()[0] == 1522151835


def test_reset_start(db_str, db_cursor):
    log_start(db_str, 'first.cities', 1522151835)
    reset_start(db_str, 'first.countries')

    db_cursor.execute('''
        SELECT started 
        FROM tables
        WHERE table_name == 'first.cities'
    ''')
    assert db_cursor.fetchone()[0] == 1522151835

    reset_start(db_str, 'first.cities')

    db_cursor.execute('''
        SELECT started 
        FROM tables
        WHERE table_name == 'first.cities'
    ''')
    assert db_cursor.fetchone()[0] is None


def test_reset_all_starts(db_str, db_cursor):
    log_start(db_str, 'first.cities', 1522151835)
    log_start(db_str, 'first.countries', 1522151855)

    reset_all_starts(db_str)
    db_cursor.execute('''
        SELECT started 
        FROM tables
    ''')
    for row in db_cursor:
        assert row[0] is None


def test_log_timestamps(db_str, db_cursor):
    now = arrow.now().timestamp
    ts = Timestamps()
    ts.log('start')
    ts.log('insert')
    log_timestamps(db_str, 'second.child', ts)
    details = get_table_details(db_cursor, 'second.child')[0]
    assert details['start'] == now
    assert details['insert'] == now
    assert details['finish'] == now


def test_get_time_running(db_str):
    start = arrow.now().replace(seconds=-180).timestamp
    log_start(db_str, 'first.cities', start)
    time = get_time_running(db_str, 'first.cities')
    assert time is not None
    assert time < 190
    assert time >= 180

    time = get_time_running(db_str, 'first.countries')
    assert time is None


def test_is_running(db_str):
    assert is_running(db_str, 'first.cities') is False

    start = arrow.now().replace(seconds=-180).timestamp
    log_start(db_str, 'first.cities', start)
    assert is_running(db_str, 'first.cities') is True
    assert is_running(db_str, 'first.countries') is False


def test_set_waiting(db_str, db_cursor):
    set_waiting(db_str, 'first.cities', True)

    db_cursor.execute('''
                    SELECT waiting 
                    FROM tables
                    WHERE table_name == 'first.cities'
                ''')
    timestamp = db_cursor.fetchone()[0]
    assert arrow.now().timestamp - timestamp < 10

    set_waiting(db_str, 'first.cities', False)

    db_cursor.execute('''
            SELECT waiting 
            FROM tables
            WHERE table_name == 'first.cities'
        ''')
    assert db_cursor.fetchone()[0] == 0

    set_waiting(db_str, 'non-existent', True)
    db_cursor.execute('''
            SELECT waiting 
            FROM tables
            WHERE table_name == 'first.cities'
        ''')
    assert db_cursor.fetchone()[0] == 0


def test_get_time_waiting(db_str):
    set_waiting(db_str, 'first.cities', True)
    time = get_time_waiting(db_str, 'first.cities')
    assert time is not None
    assert time < 10

    time = get_time_waiting(db_str, 'first.countries')
    assert time is None


def test_is_waiting(db_str, db_cursor):
    assert is_waiting(db_str, 'first.cities') == (False, False)

    set_waiting(db_str, 'first.cities', True)
    assert is_waiting(db_str, 'first.cities') == (True, False)

    old = arrow.now().replace(minutes=-180).timestamp
    db_cursor.execute('''
        UPDATE tables SET waiting = ?
        WHERE table_name = ?
    ''', (old, 'first.cities'))

    assert is_waiting(db_str, 'first.cities') == (True, True)
    assert is_waiting(db_str, 'first.cities', 200*60) == (True, False)


def test_build_query_to_create_timestamps_table():
    query = build_query_to_create_timestamps_table()
    assert query == '''
        CREATE TABLE IF NOT EXISTS timestamps 
        ("table" text, 
        "start" int,"connect" int,"select" int,"create_temp" int,"process" int,"csv" int,"s3" int,"insert" int,"clean_csv" int,"tests" int,"replace_old" int,"drop_old" int,"finish" int)
    '''


def test_get_tables_to_create(db_str, db_connection, db_cursor):
    tables = get_tables_to_create(db_str)
    assert len(tables) == 4
    assert tables[0][0] == 'first.cities'

    now = arrow.now().timestamp
    update_last_created(db_str, 'first.countries', now, 367)
    tables = get_tables_to_create(db_str)
    assert len(tables) == 3
    assert tables[0][0] == 'first.cities'

    update_last_created(db_str, 'second.child', now, 100)
    update_last_created(db_str, 'second.parent', now, 100)
    tables = get_tables_to_create(db_str)
    assert tables == [('first.cities',)]

    update_last_created(db_str, 'first.cities', now, 100)
    tables = get_tables_to_create(db_str)
    assert tables == []

    config = 'test_config.conf'
    set_table_for_update(db_connection, 'first.cities', 0, config)
    tables = get_tables_to_create(db_str)
    assert tables == [('first.cities',)]

    update_last_created(db_str, 'first.cities', now, 100)
    tables = get_tables_to_create(db_str)
    assert tables == []

    last_week = arrow.now().replace(weeks=-1).timestamp
    update_last_created(db_str, 'second.child', last_week, 100)
    update_last_created(db_str, 'second.parent', last_week, 100)
    tables = get_tables_to_create(db_str)
    assert tables == [('second.parent',)]

    set_table_for_update(db_connection, 'first.cities', 0, config)
    tables = get_tables_to_create(db_str)
    assert tables == [('first.cities',), ('second.parent',)]

    db_cursor.execute('''
        UPDATE tables
        SET deleted = 1522151835
        WHERE table_name = 'first.cities' 
    ''')
    tables = get_tables_to_create(db_str)
    assert tables == [('second.parent',)]


