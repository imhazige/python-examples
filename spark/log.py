'''
the format is using str.format see https://docs.python.org/3/library/stdtypes.html#str.format
'''


import logging
import time
import sys
import os

from logging.handlers import TimedRotatingFileHandler


def create_timed_rotating_log(logger, path='app.log'):
    """
    """

    # :level from env
    level = os.environ.get('LOG_LEVEL', '')
    level = level.upper()
    if 'DEBUG' == level:
        level = logging.DEBUG
    elif 'INFO' == level:
        level = logging.INFO
    elif 'WARN' == level:
        level = logging.WARN
    elif 'ERROR' == level:
        level = logging.ERROR
    elif '' == level:
        level = logging.INFO
    else:
        raise Exception('unkonwn log level' + level)
    logger.setLevel(level)

    handler = TimedRotatingFileHandler(path,
                                       when="D",
                                       interval=1,
                                       backupCount=10)
    formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s')

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # add console
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(formatter)
    logger.addHandler(ch)


# Get an instance of a logger
logger = logging.getLogger("app")
logpath = os.environ.get('LOG_PATH', 'app.log')
create_timed_rotating_log(logger, logpath)


def format(msg, *args, **kwargs):
    if args is None or 0 == len(args):
        return msg

    return msg.format(*args, **kwargs)


def debug(msg, *args, **kwargs):
    logger.debug(format(msg, *args, **kwargs))


def info(msg, *args, **kwargs):
    logger.info(format(msg, *args, **kwargs))


def warn(msg, *args, **kwargs):
    logger.warn(format(msg, *args, **kwargs))


def error(msg, *args, **kwargs):
    logger.error(format(msg, *args, **kwargs))


def exception(msg, *args, **kwargs):
    logger.exception(msg.format(*args, **kwargs))


# ----------------------------------------------------------------------


# ----------------------------------------------------------------------
if __name__ == "__main__":
    # TODO:level from env
    debug('HIII')
    info('xxxxx')
