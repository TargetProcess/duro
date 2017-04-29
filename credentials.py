from typing import Dict
from functools import lru_cache
import configparser

config = configparser.ConfigParser()
config.read('config.conf')


@lru_cache()
def redshift_credentials() -> Dict:
    return dict(config['redshift'])


@lru_cache()
def slack_credentials() -> Dict:
    return dict(config['slack'])


@lru_cache()
def s3_credentials() -> Dict:
    return dict(config['s3'])
