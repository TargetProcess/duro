from typing import Dict
import configparser

config = configparser.ConfigParser()
config.read('config.conf')


def redshift_credentials() -> Dict:
    return dict(config['redshift'])


def slack_credentials() -> Dict:
    return dict(config['slack'])
