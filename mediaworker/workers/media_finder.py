#!/usr/bin/env python
import re
from datetime import datetime, timedelta
import logging

from mediaworker import env

from systools.system import loop, timeout, timer

from mediacore.model.media_finder import MediaFinder
from mediacore.model.file import File
from mediacore.model.search import Search
from mediacore.web.google import Google
from mediacore.web.imdb import Imdb
from mediacore.web.sputnikmusic import Sputnikmusic
from mediacore.util.title import clean
from mediacore.util.util import randomize


NB_TRACKS_MIN = 5
MOVIES_SEARCH_LANGS = ['en', 'fr']
HISTORY_LIMIT = 500


logger = logging.getLogger(__name__)


def get_directors(paths=None):
    '''Get a list of movies directors.
    '''
    spec = {
        'type': 'video',
        'extra.imdb_director': {'$exists': True},
        }
    if paths:
        if not isinstance(paths, (tuple, list)):
            paths = [paths]
        spec['file'] = {'$regex': '^(%s)/' % '|'.join([re.escape(p) for p in paths])}

    directors = []
    for res in File().find(spec):
        for director in res['extra']['imdb_director']:
            if not director in directors:
                directors.append(director)
    return directors

def get_bands(paths=None):
    '''Get a list of music bands.
    '''
    spec = {
        'info.artist': {'$nin': ['', 'va', 'various']},
        'type': 'audio',
        }
    if paths:
        if not isinstance(paths, (tuple, list)):
            paths = [paths]
        spec['file'] = {'$regex': '^(%s)/' % '|'.join([re.escape(p) for p in paths])}

    bands = {}
    for res in File().find(spec):
        band = res['info']['artist']
        bands.setdefault(band, 0)
        bands[band] += 1
    return [k for k, v in bands.items() if v > NB_TRACKS_MIN]

def get_director_info(director):
    info = Imdb().get_info(director, type='name')
    if not info:
        return {}
    return info

def get_band_info(band):
    info = Sputnikmusic().get_info(band)
    if not info:
        return {}
    return info

def movie_exists(movie):
    if File().find_one({
            'type': 'video',
            'info.full_name': clean(movie, 1),
            }):
        return True

def band_exists(band):
    res = File().find({
            'type': 'audio',
            'info.artist': clean(band, 1),
            }).count()
    if res >= NB_TRACKS_MIN:
        return True

def add_movies_search(movie):
    '''Add a movies search.
    '''
    query = clean(movie['title'], 1)
    if query and not Search().get(q=query, category='movies'):
        Search().add(query,
                category='movies',
                mode='once',
                langs=MOVIES_SEARCH_LANGS,
                url_info=movie.get('url'))
        logger.info('added movies search "%s"', query)
        return True

def add_music_search(band):
    '''Add a search for each band album.
    '''
    albums = get_band_info(band).get('albums')
    if albums:
        for album in albums:
            query = clean('%s %s' % (band, album['name']), 1)
            if query and not Search().get(q=query, category='music'):
                Search().add(query,
                        category='music',
                        mode='once',
                        url_info=album.get('url'))
                logger.info('added music search "%s"', query)
        return True

def process_movies(search):
    for director in randomize(get_directors(search['paths'])):
        logger.info('searching movies from director "%s"', director)

        info = get_director_info(director)
        for movie in info['titles']:
            history = search.get('history', [])
            if movie['title'] in history:
                continue
            if movie_exists(movie['title']):
                continue

            # Add search
            if add_movies_search(movie):
                history.insert(0, movie['title'])
                MediaFinder().update({'_id': search['_id']}, {'$set': {
                        'processed': datetime.utcnow(),
                        'history': history[:HISTORY_LIMIT],
                        }},
                        safe=True)
                return

def process_music(search):
    for band in randomize(get_bands(search['paths'])):
        logger.info('searching similar bands for "%s"', band)

        for similar_band in get_band_info(band).get('similar_bands', []):
            history = search.get('history', [])
            if similar_band in history:
                continue
            if band_exists(similar_band):
                continue

            # Add search
            if add_music_search(similar_band):
                history.insert(0, similar_band)
                MediaFinder().update({'_id': search['_id']}, {'$set': {
                        'processed': datetime.utcnow(),
                        'history': history[:HISTORY_LIMIT],
                        }},
                        safe=True)
                return

@loop(minutes=10)
@timeout(hours=1)
@timer()
def main():
    if not Google().accessible:
        return

    for search in MediaFinder().find():
        date = search.get('processed')
        if date and date + timedelta(hours=search['recurrence']) > datetime.utcnow():
            continue

        logger.info('processing %s paths %s', search['category'], search['paths'])

        if search['category'] == 'movies':
            process_movies(search)
        elif search['category'] == 'music':
            process_music(search)


if __name__ == '__main__':
    main()
