import sqlite3

ddl = """
create table commits
(
    hash text,
    processed integer
);

create table tables
(
    table_name text,
    query text,
    interval integer,
    config text,
    last_created integer,
    mean real,
    times_run integer,
    force integer,
    started integer,
    deleted integer,
    waiting integer
);

create table timestamps
(
    "table" text,
    start int,
    connect int,
    "select" int,
    create_temp int,
    process int,
    csv int,
    s3 int,
    "insert" int,
    clean_csv int,
    tests int,
    replace_old int,
    drop_old int,
    make_snapshot int,
    finish int
);

create table version
(
    major INTEGER,
    minor INTEGER
);
"""

inserts = """
INSERT INTO tables (table_name, query, interval, config, last_created, mean, times_run, force, started, deleted, waiting) 
VALUES ('first.cities', 'select city, country
from first.cities_raw', 1440, 
'{"grant_select": "jane, john"}', 
null, 0, 0, null, null, null, null);

INSERT INTO tables  
VALUES ('first.countries', 'select country, continent
from first.countries_raw;', 60, 
'{"grant_select": "joan, john"}',
null, 0, 0, null, null, null, null);

INSERT INTO tables 
VALUES ('second.child', 'select city, country from first.cities', null, 
'{"diststyle": "all", "distkey": "city", "snapshots_interval": "24d", "snapshots_stored_for": "90d"}', 
null, 0, 0, null, null, null, null);

INSERT INTO tables  
VALUES ('second.parent', 'select * from second.child limit 10', 24, 
'{"diststyle": "even"}', null, 0, 0, null, null, null, null);

INSERT INTO timestamps ("table", start, connect, "select", create_temp, 
process, csv, s3, "insert", clean_csv, tests, replace_old, drop_old, make_snapshot,
finish)
VALUES ('first.cities', 1522151698, 1522151699, 1522151773, 1522151783, null, 
1522151793, null, null, null, 1522151799, 1522151825, 1522151825, null, 1522151825);

INSERT INTO timestamps ("table", start, connect, "select", create_temp, 
process, csv, s3, "insert", clean_csv, tests, replace_old, drop_old, make_snapshot,
finish)
VALUES ('first.cities', 1522151835, 1522151849, 1522152053, 1522152063, null, 
1522152073, null, null, null, 1522152155, 1522152202, 1522152202, null, 1522152202);

INSERT INTO timestamps ("table", start, connect, "select", create_temp, 
process, csv, s3, "insert", clean_csv, tests, replace_old, drop_old, make_snapshot,
finish)
VALUES ('first.cities', 1523544406, null, null, null, null, null, null, null, 
null, null, null, null, null, null);

INSERT INTO version (major, minor) VALUES (1, 0);
"""
