import os.path
from datetime import datetime, timedelta
import logging

from mediaworker import settings, get_factory

from systools.system import loop, timer

from mediacore.model.release import Release
from mediacore.model.worker import Worker
from mediacore.web.google import Google
from mediacore.web.vcdquality import Vcdquality
from mediacore.web.tvrage import Tvrage
from mediacore.web.sputnikmusic import Sputnikmusic
from mediacore.util.title import clean


NAME = os.path.splitext(os.path.basename(__file__))[0]
TIMEOUT_IMPORT = 600    # seconds
DELTA_IMPORT = timedelta(hours=2)
DELTA_RELEASE = timedelta(days=90)
VCDQUALITY_PAGES_MAX = 10
TV_EPISODE_MAX = 20  # maximum episode number for new releases


logger = logging.getLogger(__name__)


def _import_vcdquality():
    for res in Vcdquality().results(pages_max=VCDQUALITY_PAGES_MAX):
        if res['date'] < datetime.utcnow() - DELTA_RELEASE:
            continue

        name = clean(res['release'], 7)
        if not Release().find_one({
                'name': name,
                'type': 'video',
                'info.subtype': 'movies',
                }):
            Release().insert({
                    'name': name,
                    'type': 'video',
                    'info': {'subtype': 'movies'},
                    'release': res['release'],
                    'date': res['date'],    # datetime
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

        name = clean(res['name'], 7)
        if not Release().find_one({
                'name': name,
                'type': 'video',
                'info.subtype': 'tv',
                }):
            Release().insert({
                    'name': name,
                    'type': 'video',
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
        if not Release().find_one({
                'artist': res['artist'],
                'album': res['album'],
                'type': 'audio',
                'info.subtype': 'music',
                }):
            Release().insert({
                    'name': name,
                    'artist': res['artist'],
                    'album': res['album'],
                    'type': 'audio',
                    'info': {'subtype': 'music'},
                    'date': res['date'],    # datetime
                    'created': datetime.utcnow(),
                    'processed': False,
                    }, safe=True)
            logger.info('added music release "%s"', name)

@timer()
def import_releases(type):
    res = Worker().get_attr(NAME, type)
    if not res or res < datetime.utcnow() - DELTA_IMPORT:
        globals().get('_import_%s' % type)()
        Worker().set_attr(NAME, type, datetime.utcnow())

@loop(minutes=5)
def run():
    if Google().accessible:
        factory = get_factory()

        for type in ('vcdquality', 'tvrage', 'sputnikmusic'):
            target = '%s.workers.release.import_releases' % settings.PACKAGE_NAME
            factory.add(target=target, args=(type,), timeout=TIMEOUT_IMPORT)

        Release().remove({'date': {'$lt': datetime.utcnow() - DELTA_RELEASE}},
                safe=True)
