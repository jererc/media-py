from datetime import datetime


DB_NAME = 'mediaworker'

# Data
PATH_MEDIA_ROOT = '/home/user'
PATHS_FINISHED = {
    'transmission': '/home/user/.transmission/finished', # must be different than the download dir
    }
PATHS_MEDIA_NEW = {
    'audio': '/home/user/audio/new',
    'video': '/home/user/video/new',
    'image': '/home/user/image/new',
    None: '/home/user/misc/new',    # other media types
    }
PATH_TMP = '/tmp'

# New media filters
IMDB_DATE_MIN = datetime.utcnow().year - 1
IMDB_RATING_MIN = 6.0   # / 10
SPUTNIKMUSIC_RATING_MIN = 3.5   # / 5
TVRAGE_STYLES = ['scripted', 'mini-series']     # list of authorized tvshow styles

# Credentials
OPENSUBTITLES_USERNAME = ''
OPENSUBTITLES_PASSWORD = ''

# Transmission
TRANSMISSION_USERNAME = None
TRANSMISSION_PASSWORD = None
TRANSMISSION_PORT = 9091

# Logging
LOG_FORMAT = '%(asctime)s [%(levelname)s] %(name)s: %(message)s'
LOG_DEFAULT = '/home/user/log/mediaworker.log'
LOG_ERRORS = '/home/user/log/mediaworker-errors.log'
LOG_SIZE = 100000   # Bytes
LOG_COUNT = 10


# Import local settings
try:
    from local_settings import *
except ImportError:
    pass


# Check directories
import os

paths = [PATH_MEDIA_ROOT] + PATHS_MEDIA_NEW.values() + PATHS_FINISHED.values() + [os.path.dirname(LOG_DEFAULT), os.path.dirname(LOG_ERRORS), PATH_TMP]
for path in set(paths):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except Exception, e:
            raise Exception('failed to create %s: %s' % (path, e))
        print 'created %s' % path