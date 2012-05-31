from datetime import datetime
import re


DB_NAME = 'mediaworker'


# Data
PATH_MEDIA_ROOT = '/home/user'
PATHS_FINISHED = {
    'transmission': '/home/user/.transmission/finished', # must be different than the download dir
    }
PATH_INVALID_DOWNLOAD = '/home/user/misc/invalid'
PATHS_MEDIA_NEW = {
    'audio': '/home/user/audio/new',
    'video': '/home/user/video/new',
    'image': '/home/user/image/new',
    None: '/home/user/misc/new',    # other media types
    }
PATH_TMP = '/tmp'


# Releases filters
IMDB_DATE_MIN = datetime.utcnow().year - 1  # greater or equal
IMDB_RATING_MIN = 6.0   # / 10, greater or equal
SPUTNIKMUSIC_RATING_MIN = 3.5   # / 5, greater or equal
TVRAGE_STYLES = ['scripted', 'mini-series']


# Search results filters
re_incl_movies = re.compile(r'\b(br|bd|dvd|hd)rip\b', re.I)
re_incl_tv = re.compile(r'\b([hp]dtv|dsr(ip)?)\b', re.I)
re_excl_video = re.compile(r'\b(720|1080)p\b', re.I)
re_excl_anime = re.compile(r'\b(720|1080)p\b', re.I)
FILTER_DEF = {    # size ranges in MB, title inclusions and exclusions
    'anime': {'size_min': 100, 'size_max': 1000, 're_excl': re_excl_anime},
    'apps': {},
    'books': {},
    'games': {},
    'movies': {'size_min': 500, 'size_max': 2500, 're_incl': re_incl_movies, 're_excl': re_excl_video},
    'music': {'size_min': 30, 'size_max': 300},
    'tv': {'size_min': 100, 'size_max': 1000, 're_incl': re_incl_tv, 're_excl': re_excl_video},
    }


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
LOG_COUNT = 100


# Import local settings
try:
    from local_settings import *
except ImportError:
    pass


# Check directories
import os

paths = [PATH_MEDIA_ROOT] + PATHS_MEDIA_NEW.values() \
    + PATHS_FINISHED.values() \
    + [os.path.dirname(LOG_DEFAULT), os.path.dirname(LOG_ERRORS), PATH_TMP]
for path in set(paths):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except Exception, e:
            raise Exception('failed to create %s: %s' % (path, e))
        print 'created %s' % path
