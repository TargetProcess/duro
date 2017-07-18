import logging
from logging import Logger

import logzero

from utils.global_config import load_global_config


def setup_logger(name: str, stdout: bool = False) -> Logger:
    if stdout:
        return logzero.setup_logger(name=name, level=logging.INFO)
    else:
        logfile = f'{load_global_config().logs_path}/{name}.log'
        return logzero.setup_logger(name=name, logfile=logfile,
                            level=logging.INFO, maxBytes=1_000_000)
