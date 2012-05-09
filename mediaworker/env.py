import logging

from mediaworker.settings import DB_NAME

from mediacore.util.db import connect


logging.basicConfig(level=logging.DEBUG)

connect(DB_NAME)
