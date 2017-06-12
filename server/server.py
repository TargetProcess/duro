import sqlite3
from typing import List, Dict, Tuple

import arrow
from flask import Flask, g, render_template, request, jsonify

from server.formatters import (format_average_time, format_as_human_date,
                               format_as_date, format_interval,
                               print_log, skip_none)
from server.sqlite import get_all_tables, get_jobs, get_table_details

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


def connect_db(use_rowfactory=True):
    rv = sqlite3.connect(DATABASE)
    if use_rowfactory:
        rv.row_factory = sqlite3.Row
    return rv


@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, '_database', None)
    if db is not None:
        db.close()


@app.route('/tables/')
def show_list():
    return render_template('list_tables.html', tables=get_all_tables(get_db()))


@app.route('/')
@app.route('/<from_date>/')
@app.route('/<from_date>/<to_date>')
def show_current(from_date=None, to_date=None):
    if from_date:
        floor = arrow.get(from_date).format()
    else:
        floor = arrow.utcnow().replace(minutes=-30).format()
    if to_date:
        ceiling = arrow.get(to_date).format()
    else:
        ceiling = arrow.utcnow().format()
    return render_template('current_jobs.html',
                           floor=floor,
                           ceiling=ceiling)


@app.route('/jobs')
def jobs():
    db = get_db()
    print(request.args.get('from'), type(request.args.get('from')))
    floor = arrow.get(request.args.get('from')).timestamp
    ceiling = arrow.get(request.args.get('to')).timestamp
    return jsonify([{'table': job['table'],
                     'start': arrow.get(job['start']).format(),
                     'finish': arrow.get(job['finish']).format()}
                    for job in get_jobs(floor, ceiling, db)
                    ])


@app.route('/tables/<table>')
def show_table_details(table: str):
    db = get_db()
    logs, graph_data = prepare_table_details(get_table_details(db, table))
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


def main():
    app.add_template_filter(format_average_time, 'format_avg_time')
    app.add_template_filter(format_as_human_date, 'format_int_as_human_date')
    app.add_template_filter(format_as_date, 'format_int_as_date')
    app.add_template_filter(format_interval)
    app.add_template_filter(skip_none)
    app.run(debug=True)


if __name__ == '__main__':
    main()
