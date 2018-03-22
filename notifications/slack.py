from collections import defaultdict
from datetime import datetime as dt, timedelta
from functools import wraps
from typing import Tuple, Dict, Optional
from urllib.error import HTTPError


import slackweb

from utils.global_config import load_slack_config, SlackConfig
from utils.logger import setup_logger


# pylint: disable=inconsistent-return-statements
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
    logger = setup_logger('slack')
    slack_config = load_slack_config()

    if slack_config is None:
        logger.info(f'Couldn’t load Slack config to send “{message}”')
        return

    url, message_dict = build_message(slack_config, message, title, message_type)
    slack = slackweb.Slack(url)

    try:
        slack.notify(**message_dict)
    except HTTPError:
        logger.info(f'Slack URL or channel name is incorrect.')


def choose_channel_and_emoji(slack_config: SlackConfig,
                             message_type: str) -> Tuple:

    channels = {'success': (slack_config.success_channel,
                            ':white_check_mark:'),
                'failure': (slack_config.failure_channel,
                            ':scream_cat:'),
                'log': (slack_config.log_channel,
                        ':white_check_mark:')}

    return channels.get(message_type,
                        (slack_config.failure_channel,
                         ':scream_cat:'))


def build_message(slack_config: SlackConfig,
                  message, title=None,
                  message_type=None) -> Tuple[Optional[str], Optional[Dict]]:

    channel, emoji = choose_channel_and_emoji(slack_config, message_type)
    username = title if title is not None else 'Duro notification'

    message_dict = {
        'text': message,
        'channel': channel,
        'icon_emoji': emoji,
        'username': username
    }

    return slack_config.url, message_dict
