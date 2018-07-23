import arrow

from server.formatters import (
    print_log,
    format_as_human_date,
    format_as_date,
    format_as_ts,
    format_as_short_ts,
    format_delta,
    format_seconds,
    format_interval,
    format_average_time,
    skip_none,
    format_minutes,
    format_job,
    prepare_table_details,
)


def test_print_log():
    log = {
        "start": 1522151698,
        "connect": 1522151699,
        "select": 1522151773,
        "create_temp": 1522151783,
        "process": None,
        "csv": 1522151793,
        "s3": None,
        "insert": None,
        "clean_csv": None,
        "tests": 1522151799,
        "replace_old": 1522151825,
        "drop_old": 1522151865,
    }
    printed = print_log(log)
    assert printed == [
        "Tuesday, March 27, 14:54:58",
        "14:54:59: Connected to Redshift (1s)",
        "14:56:13: Selected data from Redshift (1m 14s)",
        "14:56:23: Created temporary table (10s)",
        "14:56:33: Exported processed data to CSV (10s)",
        "14:56:39: Run tests (6s)",
        "14:57:05: Replaced old table (26s)",
        "14:57:45: Dropped old table (40s)",
    ]
    assert len(printed) == 8


def test_prepare_table_details():
    log = {
        "start": None,
        "connect": 1522151699,
        "select": 1522151773,
        "create_temp": 1522151783,
        "process": None,
        "csv": 1522151793,
        "s3": None,
        "insert": None,
        "clean_csv": None,
        "tests": 1522151799,
        "replace_old": 1522151825,
        "drop_old": 1522151865,
        "finish": 1522151865,
    }
    assert prepare_table_details([log]) == ([], [])

    log["start"] = 1522151698
    assert prepare_table_details([log]) == (
        [
            [
                "Tuesday, March 27, 14:54:58",
                "14:54:59: Connected to Redshift (1s)",
                "14:56:13: Selected data from Redshift (1m 14s)",
                "14:56:23: Created temporary table (10s)",
                "14:56:33: Exported processed data to CSV (10s)",
                "14:56:39: Run tests (6s)",
                "14:57:05: Replaced old table (26s)",
                "14:57:45: Dropped old table (40s)",
            ]
        ],
        [{"date": "2018-03-27 11:54:58+00:00", "duration": 167}],
    )


def test_format_as_human_date():
    assert format_as_human_date(None) == ""
    now = arrow.utcnow()
    assert format_as_human_date(now) == "just now"
    assert format_as_human_date(now.shift(hours=-2)) == "2 hours ago"


def test_format_as_date():
    assert format_as_date(None) == ""
    assert format_as_date(1522151698) == "2018-03-27T14:54:58+03:00"


def test_format_as_short_ts():
    assert format_as_short_ts(None) == ""
    assert format_as_short_ts(1522151698) == "14:54:58"


def test_format_as_ts():
    assert format_as_ts(None) == ""
    assert format_as_ts(1522151698) == "Tuesday, March 27, 14:54:58"


def test_format_delta():
    assert format_delta(1000, 1005) == "5s"
    assert format_delta(1522151698, 1522157726) == "1h 40m 28s"


def test_format_minutes():
    assert format_minutes(0) == ""
    assert format_minutes(None) == ""
    assert format_minutes(15) == "15s"
    assert format_minutes(85) == "1m 25s"
    assert format_minutes(1273) == "21m 13s"
    assert format_minutes(3685) == "61m 25s"


def test_format_seconds():
    assert format_seconds(None) == ""
    assert format_seconds(0) == "0s"
    assert format_seconds(15) == "15s"
    assert format_seconds(85) == "1m 25s"
    assert format_seconds(660) == "11m"
    assert format_seconds(3600) == "1h"
    assert format_seconds(3625) == "1h 25s"
    assert format_seconds(3685) == "1h 1m 25s"
    assert format_seconds(172_800) == "2d"
    assert format_seconds(93_600) == "1d 2h"
    assert format_seconds(94_260) == "1d 2h 11m"
    assert format_seconds(93_655) == "1d 2h 55s"
    assert format_seconds(94_273) == "1d 2h 11m 13s"


def test_format_interval():
    assert format_interval(None) == ""
    assert format_interval(15) == "15m"
    assert format_interval(75) == "1h 15m"
    assert format_interval(1440) == "24h"


def test_format_average_time():
    assert format_average_time(None) == ""
    assert format_average_time(2.6) == "3s"
    assert format_average_time(12.2) == "12s"
    assert format_average_time(1440.1) == "24m"


def test_skip_none():
    assert skip_none("text") == "text"
    assert skip_none("") == ""
    assert skip_none(None) == ""


def test_format_job():
    job = {"table": "schema.table", "start": 1522151698, "finish": 1522151865}
    formatted = format_job(job)
    assert formatted == {
        "table": "schema.table",
        "start": "2018-03-27 11:54:58+00:00",
        "finish": "2018-03-27 11:57:45+00:00",
    }
