#!/usr/bin/env python
import os.path
from datetime import datetime, timedelta
import logging

from pymongo import ASCENDING

from mediaworker import env, settings

from systools.system import loop, timeout, timer

from mediacore.model.release import Release
from mediacore.model.search import Search
from mediacore.model.file import File
from mediacore.model.worker import Worker
from mediacore.web.google import Google
from mediacore.web.imdb import Imdb
from mediacore.web.vcdquality import Vcdquality
from mediacore.web.sputnikmusic import Sputnikmusic
from mediacore.web.tvrage import Tvrage
from mediacore.web.youtube import Youtube
from mediacore.util.title import Title, clean
from mediacore.util.util import prefix_dict


NAME = os.path.splitext(os.path.basename(__file__))[0]
DELTA_IMPORT = timedelta(hours=2)
DELTA_UPDATE = timedelta(hours=24)
DELTA_RELEASE = timedelta(days=90)
VCDQUALITY_PAGES_MAX = 10
TV_EPISODE_MAX = 20  # maximum episode number for new releases
UPDATE_LIMIT = 20
SEARCH_LANGS_DEF = {
    'movies': ['en', 'fr'],
    'tv': ['en'],
    'music': None,
    }
NB_FILES_MIN = {
    'video': 1,
    'audio': 2,
    }


logger = logging.getLogger(__name__)


def import_vcdquality(pages_max, age_max):
    for res in Vcdquality().results(pages_max=pages_max):
        if res['date'] < datetime.utcnow() - age_max:
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
                    'date': res['date'],
                    'processed': False,
                    }, safe=True)
            logger.info('added video/movies release "%s"', name)

def import_tvrage(age_max):
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
                    'processed': False,
                    }, safe=True)
            logger.info('added video/tv release "%s"', name)

def import_sputnikmusic(age_max):
    for res in Sputnikmusic().reviews():
        if not res.get('artist') or not res.get('album') or not res.get('rating'):
            continue
        if not res.get('date') or res['date'] < datetime.utcnow() - age_max:
            continue

        name = '%s %s' % (res['artist'], res['album'])
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
                    'date': res['date'],
                    'processed': False,
                    }, safe=True)
            logger.info('added audio release "%s"', name)

def _get_extra(release):
    res = {}
    subtype = release['info'].get('subtype')

    if release['type'] == 'video':

        if subtype == 'movies':
            date = Title(release['release']).date
            info = Imdb().get_info(release['name'], year=date)
            if info:
                res.update(prefix_dict(info, 'imdb_'))

        elif subtype == 'tv':
            date = release['date'].year
            info = Tvrage().get_info(release['url'])
            if info:
                res.update(prefix_dict(info, 'tvrage_'))
                if info.get('date'):
                    date = info['date']

        info = Youtube().get_trailer(release['name'], date)
        if info:
            res.update(prefix_dict(info, 'youtube_'))

    elif release['type'] == 'audio':
        info = Sputnikmusic().get_album_info(release['artist'], release['album'])
        if info:
            res.update(prefix_dict(info, 'sputnikmusic_'))
        info = Youtube().get_track(release['artist'], release['album'])
        if info:
            res.update(prefix_dict(info, 'youtube_'))

    return res

def update_extra():
    '''Update the releases extra info.
    '''
    for release in Release().find({
            '$or': [
                {'updated': {'$exists': False}},
                {'updated': {'$lt': datetime.utcnow() - DELTA_UPDATE}},
                ],
            },
            limit=UPDATE_LIMIT,
            sort=[('updated', ASCENDING)],
            timeout=False):
        Release().update({'_id': release['_id']}, {'$set': {
                'extra': _get_extra(release),
                'updated': datetime.utcnow(),
                }}, safe=True)

        logger.info('updated %s release "%s"', release['info'].get('subtype'), release['name'])

def add_search(release, url_info):
    if release_exists(release):
        return

    category = release['info'].get('subtype')
    if release['name'] in Search().list_names().get(category, []):
        return

    if category == 'tv':
        query = '%s 1x01' % release['name']
        mode = 'inc'
    else:
        query = release['name']
        mode = 'once'

    Search().add(query,
            category=category,
            mode=mode,
            langs=SEARCH_LANGS_DEF[category],
            release_id=release['_id'],
            url_info=url_info)

    logger.info('added %s search "%s"', category, query)

def process_releases():
    for release in Release().find({
            'processed': False,
            'updated': {'$exists': True},
            }):
        extra = release['extra']
        subtype = release['info'].get('subtype')

        if subtype == 'movies':
            rating = extra.get('imdb_rating')
            if rating is None:
                continue
            date = extra.get('imdb_date')
            if not date:
                continue

            if rating >= settings.IMDB_RATING_MIN \
                    and date >= settings.IMDB_DATE_MIN:
                add_search(release, url_info=extra.get('imdb_url'))

        elif subtype == 'tv':
            style = extra.get('tvrage_style')
            if style is None:
                continue

            if style in settings.TVRAGE_STYLES:
                add_search(release,
                        url_info=extra.get('tvrage_url') or extra.get('imdb_url'))

        elif subtype == 'music':
            rating = extra.get('sputnikmusic_rating')
            if rating is None:
                continue

            if rating >= settings.SPUTNIKMUSIC_RATING_MIN \
                    or artist_exists(release['artist']):
                add_search(release, url_info=extra.get('sputnikmusic_url'))

        Release().update({'_id': release['_id']}, {'$set': {'processed': datetime.utcnow()}}, safe=True)

def release_exists(release):
    files = File().search(release['name'], release['type'])
    if len(files) >= NB_FILES_MIN[release['type']]:
        return True

def artist_exists(artist):
    res = File().find({'type': 'audio', 'info.artist': clean(artist, 1)})
    if res.count() >= NB_FILES_MIN['audio']:
        return True

def validate_import():
    res = Worker().get_attr(NAME, 'imported')
    if not res or res < datetime.utcnow() - DELTA_IMPORT:
        return True

@loop(minutes=2)
@timeout(hours=2)
@timer()
def main():
    if Google().accessible:

        if validate_import():
            import_vcdquality(VCDQUALITY_PAGES_MAX, DELTA_RELEASE)
            import_tvrage(DELTA_RELEASE)
            import_sputnikmusic(DELTA_RELEASE)

            Worker().set_attr(NAME, 'imported', datetime.utcnow())

            Release().remove({'date': {'$lt': datetime.utcnow() - DELTA_RELEASE}}, safe=True)

        update_extra()

    process_releases()


if __name__ == '__main__':
    main()
