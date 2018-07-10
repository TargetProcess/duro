import logging
from functools import wraps
from logging import Logger
from typing import Callable
from inspect import signature
from os import makedirs

import logzero

from utils.global_config import load_global_config


def setup_logger(name: str = 'duro', stdout: bool = False) -> Logger:
    if stdout:
        return logzero.setup_logger(name=name, level=logging.INFO)

    path = load_global_config().logs_path
    makedirs(path, exist_ok=True)

    logfile = f'{path}/{name}.log'
    return logzero.setup_logger(name=name, logfile=logfile,
                                level=logging.INFO, maxBytes=1_000_000)


def log_action(action: str, argument_name: str = 'table'):
    def outer(f: Callable) -> Callable:
        logger = setup_logger()

        @wraps(f)
        def wrapper(*args, **kwargs):
            bound = signature(f).bind(*args, **kwargs)
            arg_value = bound.arguments.get(argument_name)
            prefix = f'{arg_value}: ' if arg_value else ''

            try:
                logger.info(f'{prefix}{action}: starting')
                result = f(*args, **kwargs)
            except Exception:
                logger.error(f'{prefix}{action}: failed')
                raise
            else:
                logger.info(f'{prefix}{action}: succeeded')
                return result

        return wrapper

    return outer
