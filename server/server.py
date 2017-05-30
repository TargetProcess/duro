import sqlite3
from typing import List, Dict, Tuple

import arrow
from flask import Flask, g, render_template

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


def get_db():
    db = getattr(g, '_database', None)
    if db is None:
        db = g._database = connect_db()
    return db


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


def connect_db():
    rv = sqlite3.connect(DATABASE)
    rv.row_factory = sqlite3.Row
    return rv


@app.route('/list')
def show_list():
    db = get_db()
    cursor = db.execute('''
        SELECT table_name, interval, last_created 
        FROM tables 
        ORDER BY last_created DESC''')
    tables = cursor.fetchall()
    return render_template('list_tables.html', tables=tables)


@app.route('/')
def show_current():
    db = get_db()
    interval = 60 * 61
    cursor = db.execute('''
            SELECT "table", "start", drop_old
            FROM timestamps
            WHERE (strftime('%s', 'now') -  "start")
                BETWEEN 0 and ?
            ORDER BY start DESC''', (interval,))
    jobs = cursor.fetchall()
    return render_template('current_jobs.html', jobs=jobs)


@app.route('/tables/<table>')
def show_table_details(table: str):
    db = get_db()
    cursor = db.execute('''
            SELECT t.table_name, t.interval,
                ts.start, ts.connect, ts."select", ts.create_temp,
                ts.process, ts.csv, ts.s3, ts."insert", ts.clean_csv,
                ts.tests, ts.replace_old, ts.drop_old
            FROM tables t
            LEFT JOIN timestamps ts on t.table_name = ts."table"
            WHERE t.table_name = ?
            ORDER BY ts.drop_old DESC''', (table,))
    logs, graph_data = prepare_table_details(cursor.fetchall())
    return render_template('table_details.html', table=table,
                           logs=logs,
                           graph_data=graph_data)


def prepare_table_details(details: List) -> Tuple[List, List]:
    if len(details) == 0:
        return [], []
    return ([print_log(d) for d in details],
            [{'date': arrow.get(d['start']).format(),
              'duration': d['drop_old'] - d['start']}
             for d in details])


def print_log(log: Dict) -> List:
    result = [f'{format_as_ts(log["start"])}']
    prev_ts = log['start']
    for key in events.keys():
        if log[key] is not None and key != 'start':
            next_ts = log[key]
            result.append(f'{format_as_short_ts(log[key])}: {events[key]} ({format_seconds(prev_ts, next_ts)})')
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


def format_seconds(prev_ts: int, next_ts: int) -> str:
    delta = next_ts - prev_ts
    if delta < 60:
        return f'{delta}s'
    if delta < 3600:
        return f'{delta // 60} min {delta % 60}s'
    if delta < 86400:
        hours, remainder = delta // 3600, delta % 3600
        return f'{hours} h {remainder // 60} min {remainder % 60}s'
    if delta >= 86400:
        days, remainder = delta // 86400, delta % 86400
        hours, remainder = delta // 3600, delta % 3600
        return f'{days} d {hours} h {remainder // 60} min {remainder % 60}s'


@app.template_filter('format_interval')
def format_interval(interval: int) -> str:
    return f'({interval} minutes)' if interval is not None else ''

def main():
    app.run(debug=True)

if __name__ == '__main__':
    main()
