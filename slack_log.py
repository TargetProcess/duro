import sqlite3

from server.sqlite import get_overview_stats
from notifications.slack import send_slack_notification
from utils.global_config import load_global_config


if __name__ == '__main__':
    hours = 24
    db = sqlite3.connect(load_global_config().db_path)
    tables, updates, pct = get_overview_stats(db, hours)
    db.close()
    message = f'I’m working! ' \
              f'{tables} tables updated. ' \
              f'{updates} updates run during last {hours} hours. ' \
              f'{pct}% of time spent recreating views.'
    send_slack_notification(message, title='Duro', message_type='log')
