import logging

from mediaworker import settings

from mediacore.util.db import connect

from factory import Factory


logging.basicConfig(level=logging.DEBUG)
connect(settings.DB_NAME)


def get_factory():
    return Factory(collection=settings.PACKAGE_NAME)
