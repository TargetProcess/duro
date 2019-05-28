import time
from datetime import datetime

from create.sqlite import get_tables_to_create, reset_all_starts
from create.tree import create_tree
from utils.errors import CreationError
from notifications.slack import send_slack_notification
from utils.global_config import load_global_config


def create(root_table: str):
    try:
        create_tree(root_table, load_global_config())
    except CreationError as e:
        send_slack_notification(e.message, f"Error while creating {e.table}")
    except Exception as e:
        send_slack_notification(str(e))


if __name__ == "__main__":
    db_path = load_global_config().db_path
    reset_all_starts(db_path)
    while True:
        new_tables = get_tables_to_create(db_path)
        print(datetime.now(), len(new_tables), "new tables")
        for t in new_tables:
            create(t[0])
        time.sleep(30)
