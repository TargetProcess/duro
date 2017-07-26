import slackweb

from utils.global_config import load_slack_config
from utils.logger import setup_logger


def send_slack_notification(message: str, title: str = None,
                            success: bool = False):
    slack_config = load_slack_config()
    logger = setup_logger('slack')

    if slack_config is None:
        logger.info(f'Couldn’t load Slack config to send “{message}”')
        return

    slack = slackweb.Slack(slack_config.url)
    channel = slack_config.success_channel if success else slack_config.failure_channel
    emoji = ':white_check_mark:' if success else ':scream_cat:'
    username = title if title is not None else 'Duro notification'
    slack.notify(text=message, channel=channel,
                 username=username, icon_emoji=emoji)
