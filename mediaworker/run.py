#!/usr/bin/env python
import logging
from logging.handlers import RotatingFileHandler

from mediaworker import env, settings, get_factory

from systools.system import get_package_modules


WORKERS_DIR = 'workers'


def main():
    factory = get_factory()
    factory.drop()

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
            target = '%s.%s.%s.main' % (settings.PACKAGE_NAME, WORKERS_DIR, module)
            factory.add(target=target, daemon=True)

    factory.run()


if __name__ == '__main__':
    main()
