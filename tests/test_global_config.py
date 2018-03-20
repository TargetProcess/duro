import pytest

from utils.global_config import load_global_config, load_slack_config


def test_load_global_config(full_config, partial_config, empty_config):
    full_global_config = load_global_config(full_config)
    assert full_global_config.views_path == 'views_folder'
    assert full_global_config.db_path == 'db.db'
    assert full_global_config.logs_path == 'logs_folder'
    assert full_global_config.graph.name == 'test-graph'

    empty_global_config = load_global_config(empty_config)
    assert empty_global_config.views_path == './views'
    assert empty_global_config.db_path == './duro.db'
    assert empty_global_config.logs_path == './logs'
    assert empty_global_config.graph.name == 'dependencies'

    partial_global_config = load_global_config(partial_config)
    assert partial_global_config.views_path == './views'
    assert partial_global_config.db_path == 'db.db'
    assert partial_global_config.logs_path == 'logs_folder'
    assert partial_global_config.graph.name == 'dependencies'

    with pytest.raises(ValueError):
        load_global_config('non-existent file')


def test_load_slack_config(full_config, partial_config, empty_config):
    full_slack_config = load_slack_config(full_config)
    assert full_slack_config.success_channel == 'success'
    assert full_slack_config.log_channel == 'log-channel'
    assert full_slack_config.failure_channel == 'failure'
    assert full_slack_config.url == 'https://hooks.slack.com/services/'

    empty_slack_config = load_slack_config(empty_config)
    assert empty_slack_config is None

    partial_slack_config = load_slack_config(partial_config)
    assert partial_slack_config.success_channel == 'channel'
    assert partial_slack_config.log_channel == 'channel'
    assert partial_slack_config.failure_channel == 'channel'
    assert partial_slack_config.url == 'https://hooks.slack.com/services/'
