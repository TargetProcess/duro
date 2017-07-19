import time
from datetime import datetime

from create.sqlite import get_tables_to_create
from create.tree import create_tree
from utils.global_config import load_global_config


def create(root_table: str):
    create_tree(root_table, load_global_config())


if __name__ == '__main__':
    while True:
        new_tables = get_tables_to_create('./duro.db')
        print(datetime.now(), len(new_tables), 'new tables')
        for t in new_tables:
            create(t[0])
        time.sleep(30)
