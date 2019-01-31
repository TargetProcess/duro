import arrow

from create.sqlite import mark_table_as_waiting, mark_table_as_not_waiting, log_start
from tree import should_be_created


def test_should_be_created(db_str, db_cursor, table):
    interval = 20
    # assert should_be_created(db_str, table, interval) is True
    #
    # mark_table_as_waiting(db_str, table.name)
    # assert should_be_created(db_str, table, interval) is False
    #
    # mark_table_as_not_waiting(db_str, table.name)
    # assert should_be_created(db_str, table, interval) is True
    #
    # old = arrow.now().replace(minutes=-180).timestamp
    # db_cursor.execute(
    #     """
    #     UPDATE tables SET waiting = ?
    #     WHERE table_name = ?
    # """,
    #     (old, table.name),
    # )
    # assert should_be_created(db_str, table, interval) is True

    start = arrow.now().replace(seconds=-180).timestamp
    log_start(db_str, table.name, start)
    assert should_be_created(db_str, table, interval) is False


def test_wait_till_finished(db_str, table):
    pass


def test_list_children_for_table():
    pass
