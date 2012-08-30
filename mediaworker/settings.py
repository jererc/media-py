import re
from datetime import timedelta


PACKAGE_NAME = 'mediaworker'

DB_NAME = 'mediaworker'

# Data
PATH_MEDIA_ROOT = '/home/user'
PATHS_MEDIA_NEW = {
    'video': '/home/user/video/new',
    'audio': '/home/user/audio/new',
    'image': '/home/user/image/new',
    None: '/home/user/misc/new',    # other media types
    }
PATHS_FINISHED = {
    'transmission': '/home/user/.transmission/finished', # must be different than the download dir
    }
PATH_INVALID_DOWNLOAD = '/home/user/misc/invalid'
PATHS_EXCLUDE = []
PATH_TMP = '/tmp'

# Search results filters
re_incl_movies = re.compile(r'\b(br|bd|dvd|hd)rip\b', re.I)
re_incl_tv = re.compile(r'\b([hp]dtv|dsr(ip)?)\b', re.I)
re_excl_video = re.compile(r'\b(720|1080)p\b', re.I)
re_excl_anime = re.compile(r'\b(720|1080)p\b', re.I)
SEARCH_FILTERS = {    # size ranges in MB, title inclusions and exclusions
    'anime': {'size_min': 100, 'size_max': 1000, 're_excl_raw': re_excl_anime},
    'apps': {},
    'books': {},
    'games': {},
    'movies': {'size_min': 500, 'size_max': 2500, 're_incl_raw': re_incl_movies, 're_excl_raw': re_excl_video},
    'music': {'size_min': 30, 'size_max': 300},
    'tv': {'size_min': 100, 'size_max': 1000, 're_incl_raw': re_incl_tv, 're_excl_raw': re_excl_video},
    }

# Search langs
MOVIES_SEARCH_LANGS = ['en']
TV_SEARCH_LANGS = ['en']
SUBTITLES_SEARCH_LANGS = ['en']

# Media filters
MEDIA_FILTERS = {
    'imdb': {
        'genre': {'exclude': r'\bhorror\b'},
        'rating': {'min': 6.5},
        },
    'tvrage': {
        'genre': {'exclude': r'\bteens|soaps\b'},
        'classification': {'include': r'\bscripted\b'},
        },
    'sputnikmusic': {
        'genre': {'exclude': r'\b(black metal|death metal|hip hop)\b'},
        'rating': {'min': 3.5},
        },
    'lastfm': {
        'genre': {'exclude': r'\b(black metal|death metal|hip hop)\b'},
        },
    }

# Similar search
SIMILAR_DELTA = {
    'movies': timedelta(days=2),
    'tv': timedelta(days=7),
    'music': timedelta(days=2),
    }

# Credentials
OPENSUBTITLES_USERNAME = ''
OPENSUBTITLES_PASSWORD = ''

# Transmission
TRANSMISSION_HOST = 'localhost'
TRANSMISSION_PORT = 9091
TRANSMISSION_USERNAME = None
TRANSMISSION_PASSWORD = None

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
