import sqlite3
from typing import List, Dict, Tuple
import json

import arrow
from flask import Flask, g, render_template, request, jsonify

from create.timestamps import events

DATABASE = './duro.db'

app = Flask(__name__)
app.config.update({'DATABASE': DATABASE})


def before_request():
    app.jinja_env.cache = {}


app.before_request(before_request)
app.config.update(
    DEBUG=True,
    TESTING=True,
    TEMPLATES_AUTO_RELOAD=True
)


def get_db(use_rowfactory=True):
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = connect_db(use_rowfactory)
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


def connect_db(use_rowfactory=True):
    rv = sqlite3.connect(DATABASE)
    if use_rowfactory:
        rv.row_factory = sqlite3.Row
    return rv


@app.route('/tables')
def show_list():
    db = get_db()
    tables = db.execute('''
        SELECT table_name, interval, last_created, mean
        FROM tables 
        ORDER BY last_created DESC''').fetchall()

    return render_template('list_tables.html', tables=tables)


@app.route('/')
def show_current():
    return render_template('current_jobs.html', jobs=jobs)


@app.route('/jobs')
def jobs():
    db = get_db(use_rowfactory=False)
    floor = arrow.get(request.args.get('from')).timestamp
    ceiling = arrow.get(request.args.get('to')).timestamp
    return jsonify(get_jobs(floor, ceiling, db))


def get_jobs(floor, ceiling, db):
    return db.execute('''
                    SELECT "table", "start", drop_old
                    FROM timestamps
                    WHERE "start" BETWEEN ? and ?
                    ORDER BY start DESC''', (floor, ceiling)).fetchall()


@app.route('/tables/<table>')
def show_table_details(table: str):
    db = get_db()
    cursor = db.execute('''
            SELECT t.table_name, t.interval,
                ts.start, ts.connect, ts."select", ts.create_temp,
                ts.process, ts.csv, ts.s3, ts."insert", ts.clean_csv,
                ts.tests, ts.replace_old, ts.drop_old, ts.finish
            FROM tables t
            LEFT JOIN timestamps ts ON t.table_name = ts."table"
            WHERE t.table_name = ?
            ORDER BY ts.drop_old DESC''', (table,))
    logs, graph_data = prepare_table_details(cursor.fetchall())
    return render_template('table_details.html', table=table,
                           logs=logs,
                           graph_data=graph_data)


def prepare_table_details(details: List) -> Tuple[List, List]:
    if len(details) == 1 and details[0]['start'] is None:
        return [], []
    return ([print_log(d) for d in details],
            [{'date': arrow.get(d['start']).format(),
              'duration': d['finish'] - d['start']}
             for d in details])


def print_log(log: Dict) -> List:
    result = [f'{format_as_ts(log["start"])}']
    prev_ts = log['start']
    for key in events.keys():
        if log[key] is not None and key != 'start':
            next_ts = log[key]
            result.append(f'{format_as_short_ts(log[key])}: {events[key]} ({format_delta(prev_ts, next_ts)})')
            prev_ts = next_ts
    return result


@app.template_filter('format_int_as_human_date')
def format_as_human_date(date: int) -> str:
    return arrow.get(date).to(
        'local').humanize() if date is not None else ''


@app.template_filter('format_int_as_date')
def format_as_date(date: int) -> str:
    return str(arrow.get(date).to('local')) if date is not None else ''


def format_as_ts(date: int) -> str:
    return arrow.get(date).to('local').strftime('%A, %B %d, %H:%M:%S') if date is not None else ''


def format_as_short_ts(date: int) -> str:
    return arrow.get(date).to('local').strftime('%H:%M:%S') if date is not None else ''


def format_delta(prev_ts: int, next_ts: int) -> str:
    delta = next_ts - prev_ts
    return format_seconds(delta)


def format_seconds(time: int) -> str:
    def _format_minutes(remainder: int):
        if remainder == 0:
            return ''
        return f'{remainder // 60}m ' + (f'{remainder % 60}s' if remainder % 60 != 0 else '')

    if time < 60:
        return f'{time}s'
    if time < 3600:
        return _format_minutes(time)
    if time <= 86400:
        hours, remainder = time // 3600, time % 3600
        return f'{hours}h ' + _format_minutes(remainder)
    if time > 86400:
        days, remainder = time // 86400, time % 86400
        hours, remainder = remainder // 3600, remainder % 3600
        return f'{days}d ' + (f'{hours}h ' if hours else ' ') + \
               _format_minutes(remainder)


@app.template_filter('format_interval')
def format_interval(interval: int) -> str:
    # return interval
    return format_seconds(interval*60) if interval is not None else ''


@app.template_filter('format_avg_time')
def format_average_time(num: float) -> str:
    return format_seconds(round(num)) if num is not None else ''


def main():
    app.run(debug=True)

if __name__ == '__main__':
    main()
    # print(format_interval(10080))
    # print(format_interval(60))
    # print(format_interval(40))
