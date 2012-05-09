#!/usr/bin/env python
from datetime import datetime, timedelta
import logging

from mediaworker import env, settings

from systools.system import loop, timeout, timer

from mediacore.model.release import Release
from mediacore.model.search import Search
from mediacore.model.file import File
from mediacore.web.google import Google
from mediacore.web.imdb import Imdb
from mediacore.web.vcdquality import Vcdquality
from mediacore.web.sputnikmusic import Sputnikmusic
from mediacore.web.tvrage import Tvrage
from mediacore.web.youtube import Youtube
from mediacore.util.title import Title, clean
from mediacore.util.util import prefix_dict


VCDQUALITY_PAGES_MAX = 10
TV_EPISODE_MAX = 20  # maximum episode number of new releases
RELEASE_AGE_MAX = timedelta(days=90)
UPDATE_RECURRENCE = timedelta(hours=24)
UPDATE_LIMIT = 100
SEARCH_LANGS_DEF = {
    'movies': ['en', 'fr'],
    'tv': ['en'],
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

        doc = {
            'type': 'video',
            'subtype': 'movies',
            'name': clean(res['release'], 7),
            }
        if not Release().find_one(doc):
            doc.update({
                    'release': res['release'],
                    'date': res['date'],
                    'processed': False,
                    })
            Release().insert(doc, safe=True)
            logger.info('added video/movies release "%s"', doc['name'])

def import_tvrage(age_max):
    for res in Tvrage().scheduled_shows():
        if not res.get('url') or not res.get('season') or not res.get('episode'):
            continue
        if res['season'] > 1 or res['episode'] > TV_EPISODE_MAX:
            continue

        doc = {
            'type': 'video',
            'subtype': 'tv',
            'name': clean(res['name'], 7),
            }
        if not Release().find_one(doc):
            doc.update({
                    'url': res['url'],
                    'date': datetime.utcnow(),  # the date we discovered the show
                    'processed': False,
                    })
            Release().insert(doc, safe=True)
            logger.info('added video/tv release "%s"', doc['name'])

def import_sputnikmusic(age_max):
    for res in Sputnikmusic().reviews():
        if not res.get('artist') or not res.get('album') or not res.get('rating'):
            continue
        if not res.get('date') or res['date'] < datetime.utcnow() - age_max:
            continue

        doc = {
            'type': 'audio',
            'artist': res['artist'],
            'album': res['album'],
            }
        if not Release().find_one(doc):
            doc.update({
                    'name': '%s %s' % (res['artist'], res['album']),
                    'date': res['date'],
                    'processed': False,
                    })
            Release().insert(doc, safe=True)
            logger.info('added audio release "%s"', doc['name'])

def get_extra(release):
    res = {}

    if release['type'] == 'video':

        if release['subtype'] == 'movies':
            date = Title(release['release']).date
            info = Imdb().get_info(release['name'], date)
            if info:
                res.update(prefix_dict(info, 'imdb_'))

        elif release['subtype'] == 'tv':
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
    for i in range(UPDATE_LIMIT):
        release = Release().find_one({
            '$or': [
                {'updated': {'$exists': False}},
                {'updated': {'$lt': datetime.utcnow() - UPDATE_RECURRENCE}},
                ],
            })
        if not release:
            break

        Release().update({'_id': release['_id']}, {'$set': {
                'extra': get_extra(release),
                'updated': datetime.utcnow(),
                }}, safe=True)
        subtype_str = '/%s' % release['subtype'] if release.get('subtype') else ''
        logger.info('updated %s%s release "%s"', release['type'], subtype_str, release['name'])

def process_releases():
    for res in Release().find({
            'processed': False,
            'updated': {'$exists': True},
            }):
        if not release_exists(res):
            searches = Search().list_names()

            if res['type'] == 'video':

                if res['subtype'] == 'movies':
                    rating = res['extra'].get('imdb_rating')
                    if rating is None:
                        continue
                    date = res['extra'].get('imdb_date')
                    if not date:
                        continue

                    if rating >= settings.IMDB_RATING_MIN \
                            and date >= settings.IMDB_DATE_MIN \
                            and res['name'] not in searches.get('movies', []):
                        Search().add(res['name'],
                                category='movies',
                                mode='once',
                                langs=SEARCH_LANGS_DEF['movies'])
                        logger.info('added movies search "%s"', res['name'])

                elif res['subtype'] == 'tv':
                    style = res['extra'].get('tvrage_style')
                    if style is None:
                        continue

                    if style in settings.TVRAGE_STYLES \
                            and res['name'] not in searches.get('tv', []):
                        query = '%s 1x01' % res['name']
                        Search().add(query,
                                category='tv',
                                mode='inc',
                                langs=SEARCH_LANGS_DEF['tv'])
                        logger.info('added tv search "%s"', query)

            elif res['type'] == 'audio':
                rating = res['extra'].get('sputnikmusic_rating')
                if rating is None:
                    continue

                if rating >= settings.SPUTNIKMUSIC_RATING_MIN or artist_exists(res['artist']) \
                        and res['name'] not in searches.get('music', []):
                    Search().add(res['name'],
                            category='music',
                            mode='once')
                    logger.info('added music search "%s"', res['name'])

        Release().update({'_id': res['_id']}, {'$set': {'processed': datetime.utcnow()}}, safe=True)

def release_exists(release):
    files = File().search(release['name'], release['type'])
    if len(files) >= NB_FILES_MIN[release['type']]:
        return True

def artist_exists(artist):
    res = File().find({'type': 'audio', 'info.artist': clean(artist, 1)})
    if res.count() >= NB_FILES_MIN['audio']:
        return True

@loop(minutes=30)
@timeout(hours=2)
@timer
def main():
    if Google().accessible:
        import_vcdquality(VCDQUALITY_PAGES_MAX, RELEASE_AGE_MAX)
        import_tvrage(RELEASE_AGE_MAX)
        import_sputnikmusic(RELEASE_AGE_MAX)

        Release().remove({'date': {'$lt': datetime.utcnow() - RELEASE_AGE_MAX}}, safe=True)

        update_extra()

    process_releases()


if __name__ == '__main__':
    main()
