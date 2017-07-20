import slackweb

from utils.global_config import load_slack_config


def send_slack_notification(message: str):
    slack_config = load_slack_config()
    if slack_config is None:
        print('Couldn’t load Slack config')
        return
    slack = slackweb.Slack(slack_config.url)
    print("MESSAGE:", message)
    slack.notify(text=message,
                 channel=slack_config.channel)
