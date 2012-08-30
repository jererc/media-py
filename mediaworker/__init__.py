from mediaworker import settings

from factory import Factory


def get_factory():
    return Factory(collection=settings.PACKAGE_NAME)
