import sqlite3

import arrow
from flask import Flask, g, render_template, request, jsonify

from create.sqlite import is_running
from server.formatters import (
    format_average_time,
    format_as_human_date,
    format_as_date,
    format_interval,
    skip_none,
    format_job,
    prepare_table_details,
)
from server.sqlite import (
    get_all_tables,
    get_jobs,
    get_table_details,
    set_table_for_update,
    get_overview_stats,
)
from utils.global_config import load_global_config

DATABASE = load_global_config().db_path

app = Flask(__name__)
app.config.update({"DATABASE": DATABASE})


def before_request():
    app.jinja_env.cache = {}


app.before_request(before_request)
app.config.update(DEBUG=True, TESTING=True, TEMPLATES_AUTO_RELOAD=True)


def get_db(use_rowfactory: bool = True):
    db = getattr(g, "_database", None)
    if db is None:
        db = g._database = connect_db(use_rowfactory)
    return db


def connect_db(use_rowfactory: bool = True):
    rv = sqlite3.connect(DATABASE)
    if use_rowfactory:
        rv.row_factory = sqlite3.Row
    return rv


# pylint: disable=unused-argument
@app.teardown_appcontext
def close_connection(exception):
    db = getattr(g, "_database", None)
    if db is not None:
        db.close()


@app.route("/tables/")
def show_list():
    return render_template("list_tables.html")


@app.route("/api/tables/")
def get_tables():
    return jsonify([dict(table) for table in get_all_tables(get_db())])


@app.route("/")
@app.route("/<from_date>/")
@app.route("/<from_date>/<to_date>")
def show_current(from_date: str = None, to_date: str = None):
    if from_date:
        floor = arrow.get(from_date)
    else:
        floor = arrow.utcnow().replace(minutes=-30)
    if to_date:
        ceiling = arrow.get(to_date)
    else:
        ceiling = arrow.utcnow()
    return render_template("current_jobs.html", floor=floor, ceiling=ceiling)


@app.route("/api/jobs")
def jobs():
    db = get_db()
    floor = arrow.get(request.args.get("from").replace(' ', '+')).timestamp
    ceiling = arrow.get(request.args.get("to").replace(' ', '+')).timestamp
    jobs_dict = [format_job(job) for job in get_jobs(floor, ceiling, db)]
    return jsonify(jobs_dict)


@app.route("/tables/<table>")
def show_table_details(table: str):
    db = get_db()
    logs, graph_data = prepare_table_details(get_table_details(db, table))
    return render_template(
        "table_details.html", table=table, logs=logs, graph_data=graph_data
    )


@app.route("/update", methods=["POST"])
def register_update_request():
    table = request.form["table"]
    force_tree_update = int(request.form.get("tree", 0))
    if is_running(DATABASE, table):
        return jsonify({"message": "Already running"})

    set_table_for_update(get_db(), table, force_tree_update)
    return jsonify({"message": f"Scheduled {table} for update", "table": table})


@app.route("/api/stats")
def stats():
    db = get_db(False)
    overview_stats = get_overview_stats(db, 24)
    return jsonify(overview_stats)


def start_server():
    app.add_template_filter(format_average_time, "format_avg_time")
    app.add_template_filter(format_as_human_date, "format_int_as_human_date")
    app.add_template_filter(format_as_date, "format_int_as_date")
    app.add_template_filter(format_interval)
    app.add_template_filter(skip_none)
    app.run(debug=True)


if __name__ == "__main__":
    start_server()
