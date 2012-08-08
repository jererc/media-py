#!/usr/bin/env python
import re
from datetime import datetime, timedelta
import logging

from mediaworker import env, settings

from systools.system import loop, timeout, timer

from mediacore.model.release import Release
from mediacore.model.similar import Similar
from mediacore.model.media import Media
from mediacore.model.search import Search
from mediacore.web.google import Google
from mediacore.web.info import similar_movies, similar_music
from mediacore.util.util import randomize
from mediacore.util.filter import validate_extra


HISTORY_LIMIT = 100
SEARCH_LANGS = {
    'movies': settings.MOVIES_SEARCH_LANGS,
    'tv': settings.TV_SEARCH_LANGS,
    'music': None,
    }
FILES_COUNT_MIN = {'music': 3}


logger = logging.getLogger(__name__)


def _media_exists(**kwargs):
    files = Media().search_files(**kwargs)
    return len(files) >= FILES_COUNT_MIN.get(kwargs.get('category'), 1)

def _add_search(**search):
    if _media_exists(**search):
        return

    search['langs'] = SEARCH_LANGS.get(search['category'])
    if Search().add(**search):
        logger.info('added search %s', search)
        return True

def process_releases():
    for release in Release().find({
            'processed': False,
            'updated': {'$exists': True},
            }):
        subtype = release['info'].get('subtype')

        if subtype == 'music' and _media_exists(name=release['artist'], category='music'):
            valid = True
        else:
            valid = validate_extra(release['extra'], settings.MEDIA_FILTERS)
            if valid is None:
                continue

        if valid:
            _add_search(**Release().get_search(release))

        Release().update({'_id': release['_id']},
                {'$set': {'processed': datetime.utcnow()}}, safe=True)

def _get_files_pattern(bases):
    if not bases:
        return
    if not isinstance(bases, (tuple, list)):
        bases = [bases]
    return r'^(%s)' % '|'.join([re.escape(b) for b in bases])

def _get_movies(bases=None):
    '''Get a list of movies.

    :return: list of tuples (movie, Media id)
    '''
    movies = {}
    spec = {
        'info.subtype': 'movies',
        'extra.imdb': {'$exists': True},
        }
    if bases:
        spec['files'] = {'$regex': _get_files_pattern(bases)}

    for media in Media().find(spec):
        if media['name'] not in movies:
            movies[media['name']] = media['_id']
    return movies.items()

def _get_music(bases=None):
    '''Get a list of music bands.

    :return: list of tuples (band, Media id)
    '''
    bands = {}
    spec = {
        'info.subtype': 'music',
        'info.artist': {'$nin': ['', 'va', 'various']},
        }
    if bases:
        spec['files'] = {'$regex': _get_files_pattern(bases)}

    for media in Media().find(spec):
        band = media['info'].get('artist')
        if band:
            bands.setdefault(band, {'_id': media['_id'], 'count': 0})
            bands[band]['count'] += len(media['files'])

    res = []
    for band, info in bands.items():
        if info['count'] >= FILES_COUNT_MIN['music']:
            res.append((band, info['_id']))
    return res

def _validate_history(args, history):
    for info in history:
        if args.get('name') != info.get('name'):
            continue
        if args.get('album') != info.get('album'):
            continue
        if args.get('category') != info.get('category'):
            continue
        return False
    return True

def _process_media(search):
    search['history'] = search.get('history', [])

    def process(args):
        if _validate_history(args, search['history']) \
                and _add_search(**args):
            search['history'].insert(0, args)
            return True

    if search['category'] == 'movies':
        for movie, media_id in randomize(_get_movies(search['paths'])):
            logger.info('searching similar movies for "%s"', movie)

            for similar_movie in similar_movies(movie, type='title',
                    filters=settings.MEDIA_FILTERS):
                if process({
                        'name': similar_movie,
                        'category': 'movies',
                        'media_id': media_id,
                        }):
                    return True

    elif search['category'] == 'music':
        for band, media_id in randomize(_get_music(search['paths'])):
            logger.info('searching similar bands for "%s"', band)

            for similar_band, album in similar_music(band,
                    filters=settings.MEDIA_FILTERS):
                if process({
                        'name': similar_band,
                        'album': album,
                        'category': 'music',
                        'media_id': media_id,
                        }):
                    return True

def process_media():
    for res in Similar().find():
        date = res.get('processed')
        if date and date + timedelta(hours=res['recurrence']) > datetime.utcnow():
            continue

        logger.info('processing %s paths %s', res['category'], res['paths'])

        if not _process_media(res):
            logger.info('failed to find similar %s from media in %s', res['category'], res['paths'])

        res['history'] = res.get('history', [])[:HISTORY_LIMIT]
        res['processed'] = datetime.utcnow()
        Similar().save(res, safe=True)

@loop(minutes=5)
@timeout(hours=1)
@timer()
def main():
    if Google().accessible:
        process_media()

    process_releases()


if __name__ == '__main__':
    main()
