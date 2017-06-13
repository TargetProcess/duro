def get_all_tables(db):
    return db.execute('''
            SELECT table_name, interval, last_created, mean
            FROM tables 
            ORDER BY last_created DESC''').fetchall()


def get_jobs(floor: int, ceiling: int, db):
    return db.execute('''
                    SELECT "table", 
                        "start",
                        COALESCE(drop_old, "insert") AS "finish"
                    FROM timestamps
                    WHERE "start" BETWEEN ? AND ?
                    
                    UNION ALL
                    
                    SELECT table_name AS "table",
                        started AS "start",
                        NULL AS "finish"
                    FROM tables
                    WHERE started IS NOT NULL
                    
                    ''', (floor, ceiling)).fetchall()


def get_table_details(db, table: str):
    return db.execute('''
                SELECT t.table_name, t.interval,
                    ts.start, ts.connect, ts."select", ts.create_temp,
                    ts.process, ts.csv, ts.s3, ts."insert", ts.clean_csv,
                    ts.tests, ts.replace_old, ts.drop_old, ts.finish
                FROM tables t
                LEFT JOIN timestamps ts ON t.table_name = ts."table"
                WHERE t.table_name = ?
                ORDER BY ts.drop_old DESC''',
                      (table,)).fetchall()


def set_table_for_update(db, table: str, force_tree: int):
    db.execute('''UPDATE tables
                SET force = 1, force_tree = ?
                WHERE table_name = ? ''', (force_tree, table))
    db.commit()
