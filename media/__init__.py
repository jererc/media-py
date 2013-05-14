from factory import Factory

from mediacore.utils.db import connect

from media import settings


connect(settings.DB_NAME)


def get_factory():
    return Factory(collection=settings.PACKAGE_NAME)
