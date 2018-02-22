from collections import defaultdict
from datetime import datetime as dt, timedelta
from functools import wraps
from urllib.error import HTTPError

import slackweb

from utils.global_config import load_slack_config
from utils.logger import setup_logger


def delay_duplicates(timeout=10):
    def wrap(f):
        calls = defaultdict(lambda: dt.min)

        @wraps(f)
        def wrapper(*args, **kwargs):
            key = ''.join(str(arg)
                          for arg in args + tuple(kwargs.values()))
            previous_call = calls[key]
            now = dt.now()
            if now - previous_call >= timedelta(minutes=timeout):
                calls[key] = now
                return f(*args, **kwargs)
        return wrapper
    return wrap


@delay_duplicates()
def send_slack_notification(message: str, title: str = None,
                            message_type: str = None):
    slack_config = load_slack_config()
    logger = setup_logger('slack')

    channels = {'success': (slack_config.success_channel,
                            ':white_check_mark:'),
                'failure': (slack_config.failure_channel,
                            ':scream_cat:'),
                'log': (slack_config.log_channel,
                        ':white_check_mark:')}

    channel, emoji = channels.get(message_type,
                                  (slack_config.failure_channel,
                                   ':scream_cat:'))

    if slack_config is None:
        logger.info(f'Couldn’t load Slack config to send “{message}”')
        return

    slack = slackweb.Slack(slack_config.url)
    username = title if title is not None else 'Duro notification'
    try:
        slack.notify(text=message, channel=channel,
                     username=username, icon_emoji=emoji)
    except HTTPError:
        logger.info(f'Slack URL or channel name is incorrect.')
