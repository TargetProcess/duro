from create.sqlite import get_overview_stats
from notifications.slack import send_slack_notification
from utils.global_config import load_global_config


if __name__ == '__main__':
    hours = 24
    tables, updates = get_overview_stats(load_global_config().db_path, hours)
    message = f'''I’m working!
{tables} tables updated, {updates} updates run during last {hours} hours.'''
    send_slack_notification(message, title='Duro', message_type='log')
