#!/usr/bin/env python
import sys
import logging
from logging.handlers import RotatingFileHandler

from systools.system import popen, get_package_modules

from media import settings, get_factory


WORKERS_DIR = 'workers'
CMDS = ['mongod', 'transmission-daemon', 'mediainfo',
    'unzip', 'unrar', 'xvfb-run']


def check_requirements():
    res = True
    for cmd in CMDS:
        if popen('which %s' % cmd)[-1] != 0:
            res = False
            print '%s is missing' % cmd

    return res

def main():
    if not check_requirements():
        sys.exit(1)

    factory = get_factory()
    factory.remove(daemon=True)

    formatter = logging.Formatter(settings.LOG_FORMAT)

    # Standard file logging
    fh = RotatingFileHandler(settings.LOG_DEFAULT, 'a', settings.LOG_SIZE,
            settings.LOG_COUNT)
    fh.setFormatter(formatter)

    # Errors file logging
    eh = RotatingFileHandler(settings.LOG_ERRORS, 'a', settings.LOG_SIZE,
            settings.LOG_COUNT)
    eh.setFormatter(formatter)
    eh.setLevel(logging.ERROR)

    factory.logging_handlers = (fh, eh)

    for module in get_package_modules(WORKERS_DIR):
        if module != '__init__':
            target = '%s.%s.%s.run' % (settings.PACKAGE_NAME, WORKERS_DIR, module)
            factory.add(target=target, daemon=True)

    factory.run()


if __name__ == '__main__':
    main()
