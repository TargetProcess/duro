import sqlite3

from utils.global_config import load_global_config
from utils.logger import setup_logger

updates = [(101, "ALTER TABLE tables ADD COLUMN waiting integer")]


def get_version(connection) -> int:
    cursor = connection.cursor()
    try:
        cursor.execute("SELECT major, minor FROM version")
        major, minor = cursor.fetchone()
        return major * 100 + minor
    except sqlite3.OperationalError as e:
        if str(e).startswith("no such table"):
            cursor.execute(
                """CREATE TABLE version (
                        major INTEGER,
                        minor INTEGER)"""
            )
            return get_version(connection)
    except TypeError:
        cursor.execute(
            """INSERT INTO version
                        VALUES (1, 0)"""
        )
        connection.commit()
        return get_version(connection)


def apply_update(connection, update):
    connection.execute(update)
    connection.commit()


def update_version(connection, new_version: int):
    connection.execute(
        """UPDATE version
                        SET major = ?, minor = ?""",
        (new_version // 100, new_version % 100),
    )
    connection.commit()


def get_connection(db: str):
    return sqlite3.connect(db)


def update_db(db: str):
    logger = setup_logger("update_db")
    connection = get_connection(db)
    version = get_version(connection)
    logger.info(f"Current db version: {version // 100}.{version % 100}")
    new_updates = [upd for upd in updates if upd[0] > version]
    for update in new_updates:
        logger.info(f"Updating to {update[0] // 100}.{update[0] % 100}")
        apply_update(connection, update[1])
        update_version(connection, update[0])
        logger.info(f"Updated to {update[0] // 100}.{update[0] % 100}")
    logger.info("Table schema is up-to-date")


if __name__ == "__main__":
    update_db(load_global_config().db_path)
