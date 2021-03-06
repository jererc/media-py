PACKAGE_NAME = 'media'
DB_NAME = 'media'
API_PORT = 9000
FILES_COUNT_MIN = {'music': 3}

# Logging
LOG_FILE = '/home/user/log/media.log'
LOG_FORMAT = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
LOG_SIZE = 100000   # bytes
LOG_COUNT = 100


# Import local settings
try:
    from local_settings import *
except ImportError:
    pass


# Check directories
import os

path = os.path.dirname(LOG_FILE)
if not os.path.exists(path):
    try:
        os.makedirs(path)
    except Exception, e:
        raise Exception('failed to create %s: %s' % (path, str(e)))
    print 'created %s' % path
