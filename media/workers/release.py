import os.path
from datetime import datetime, timedelta
import logging

from systools.system import loop, timer

from filetools.title import clean

from mediacore.model.release import Release
from mediacore.model.work import Work
from mediacore.web.google import Google
from mediacore.web.imdb import Imdb
from mediacore.web.metacritic import Metacritic
from mediacore.web.rottentomatoes import Rottentomatoes
from mediacore.web.vcdquality import Vcdquality
from mediacore.web.tvrage import Tvrage
from mediacore.web.sputnikmusic import Sputnikmusic

from media import settings, get_factory


NAME = os.path.splitext(os.path.basename(__file__))[0]
TIMEOUT_IMPORT = 600    # seconds
DELTA_IMPORT = timedelta(hours=2)
DELTA_RELEASE = timedelta(days=90)
VCDQUALITY_PAGES_MAX = 10
TV_EPISODE_MAX = 20  # maximum episode number for new releases

logger = logging.getLogger(__name__)


def _import_imdb():
    for res in Imdb().releases():
        name = res['title']
        if not Release.find_one({
                'name': name,
                'type': 'video',
                'info.subtype': 'movies',
                }):
            Release.insert({
                    'name': name,
                    'type': 'video',
                    'src': {'web': 'imdb'},
                    'info': {'subtype': 'movies'},
                    'url': res['url'],
                    'date': datetime.utcnow(),
                    'created': datetime.utcnow(),
                    'processed': False,
                    }, safe=True)
            logger.info('added movies release "%s"', name)

def _import_metacritic():
    for res in Metacritic().releases('movies_dvd'):
        if res['date'] < datetime.utcnow() - DELTA_RELEASE:
            continue

        name = res['title']
        if not Release.find_one({
                'name': name,
                'type': 'video',
                'info.subtype': 'movies',
                }):
            Release.insert({
                    'name': name,
                    'type': 'video',
                    'src': {'web': 'metacritic'},
                    'info': {'subtype': 'movies'},
                    'url': res['url'],
                    'date': res['date'],
                    'created': datetime.utcnow(),
                    'processed': False,
                    }, safe=True)
            logger.info('added movies release "%s"', name)

def _import_rottentomatoes():
    for res in Rottentomatoes().releases('dvd_new'):
        name = res['title']
        if not Release.find_one({
                'name': name,
                'type': 'video',
                'info.subtype': 'movies',
                }):
            Release.insert({
                    'name': name,
                    'type': 'video',
                    'src': {'web': 'rottentomatoes'},
                    'info': {'subtype': 'movies'},
                    'url': res['url'],
                    'date': datetime.utcnow(),
                    'created': datetime.utcnow(),
                    'processed': False,
                    }, safe=True)
            logger.info('added movies release "%s"', name)

def _import_vcdquality():
    for res in Vcdquality().releases(pages_max=VCDQUALITY_PAGES_MAX):
        if res['date'] < datetime.utcnow() - DELTA_RELEASE:
            continue

        name = clean(res['release'], 7)
        if not Release.find_one({
                'name': name,
                'type': 'video',
                'info.subtype': 'movies',
                }):
            Release.insert({
                    'name': name,
                    'type': 'video',
                    'src': {'web': 'vcdquality'},
                    'info': {'subtype': 'movies'},
                    'release': res['release'],
                    'date': res['date'],
                    'created': datetime.utcnow(),
                    'processed': False,
                    }, safe=True)
            logger.info('added movies release "%s"', name)

def _import_tvrage():
    for res in Tvrage().scheduled_shows():
        if not res.get('url') or not res.get('season') or not res.get('episode'):
            continue
        if res['season'] > 1 or res['episode'] > TV_EPISODE_MAX:
            continue

        name = clean(res['title'], 7)
        if not Release.find_one({
                'name': name,
                'type': 'video',
                'info.subtype': 'tv',
                }):
            Release.insert({
                    'name': name,
                    'type': 'video',
                    'src': {'web': 'tvrage'},
                    'info': {'subtype': 'tv'},
                    'url': res['url'],
                    'date': datetime.utcnow(),  # release date is the date we discovered the show
                    'created': datetime.utcnow(),
                    'processed': False,
                    }, safe=True)
            logger.info('added tv release "%s"', name)

def _import_sputnikmusic():
    for res in Sputnikmusic().reviews():
        if not res.get('artist') or not res.get('album') or not res.get('rating'):
            continue
        if not res.get('date') or res['date'] < datetime.utcnow() - DELTA_RELEASE:
            continue

        name = '%s - %s' % (res['artist'], res['album'])
        if not Release.find_one({
                'artist': res['artist'],
                'album': res['album'],
                'type': 'audio',
                'info.subtype': 'music',
                }):
            Release.insert({
                    'name': name,
                    'artist': res['artist'],
                    'album': res['album'],
                    'type': 'audio',
                    'src': {'web': 'sputnikmusic'},
                    'info': {'subtype': 'music'},
                    'date': res['date'],    # datetime
                    'created': datetime.utcnow(),
                    'processed': False,
                    }, safe=True)
            logger.info('added music release "%s"', name)

@timer()
def import_releases(type):
    res = Work.get_info(NAME, type)
    if not res or res < datetime.utcnow() - DELTA_IMPORT:
        globals().get('_import_%s' % type)()
        Work.set_info(NAME, type, datetime.utcnow())

@loop(minutes=5)
def run():
    if Google().accessible:
        factory = get_factory()

        for type in ('imdb', 'metacritic', 'rottentomatoes', 'vcdquality',
                'tvrage', 'sputnikmusic'):
            target = '%s.workers.release.import_releases' % settings.PACKAGE_NAME
            factory.add(target=target, args=(type,), timeout=TIMEOUT_IMPORT)

        Release.remove({'date': {'$lt': datetime.utcnow() - DELTA_RELEASE}},
                safe=True)
