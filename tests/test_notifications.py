from time import sleep

from duro.notifications.slack import (
    choose_channel_and_emoji,
    build_message,
    delay_duplicates,
)
from duro.utils.global_config import load_slack_config


def test_choose_channel_and_emoji(full_config):
    full_slack_config = load_slack_config(full_config)

    channel, emoji = choose_channel_and_emoji(full_slack_config, "success")
    assert channel == "success"
    assert emoji == ":white_check_mark:"

    channel, emoji = choose_channel_and_emoji(full_slack_config, "log")
    assert channel == "log-channel"
    assert emoji == ":white_check_mark:"

    channel, emoji = choose_channel_and_emoji(full_slack_config, "failure")
    assert channel == "failure"
    assert emoji == ":scream_cat:"

    channel, emoji = choose_channel_and_emoji(full_slack_config, "something else")
    assert channel == "failure"
    assert emoji == ":scream_cat:"


def test_build_message(full_config):
    full_slack_config = load_slack_config(full_config)
    url, message_dict = build_message(full_slack_config, "Text", "Title", "failure")
    assert url == "https://hooks.slack.com/services/"
    assert message_dict["text"] == "Text"
    assert message_dict["channel"] == "failure"
    assert message_dict["icon_emoji"] == ":scream_cat:"
    assert message_dict["username"] == "Title"

    url, message_dict = build_message(
        full_slack_config, "Text", title=None, message_type="success"
    )
    assert url == "https://hooks.slack.com/services/"
    assert message_dict["text"] == "Text"
    assert message_dict["channel"] == "success"
    assert message_dict["icon_emoji"] == ":white_check_mark:"
    assert message_dict["username"] == "Duro notification"

    url, message_dict = build_message(full_slack_config, "Text")
    assert url == "https://hooks.slack.com/services/"
    assert message_dict["text"] == "Text"
    assert message_dict["channel"] == "failure"
    assert message_dict["icon_emoji"] == ":scream_cat:"
    assert message_dict["username"] == "Duro notification"


def test_delay_duplicates():
    @delay_duplicates(1)
    def one():
        return 1

    no_sleep = []
    for _ in range(5):
        no_sleep.append(one())
    assert no_sleep == [1, None, None, None, None]

    with_sleep = []
    for _ in range(5):
        sleep(1.1)
        with_sleep.append(one())

    assert with_sleep == [1, 1, 1, 1, 1]
