#!/usr/bin/env python
import re
from datetime import datetime, timedelta
from operator import itemgetter
import logging

from mediaworker import env, settings

from systools.system import loop, timeout, timer

from mediacore.model.media_finder import MediaFinder
from mediacore.model.file import File
from mediacore.model.search import Search
from mediacore.web.google import Google
from mediacore.web.imdb import Imdb
from mediacore.web.sputnikmusic import Sputnikmusic
from mediacore.util.title import clean
from mediacore.util.util import randomize


NB_TRACKS_MIN = 3
HISTORY_LIMIT = 100
SEARCH_LANGS = {
    'movies': ['en', 'fr'],
    'music': None,
    }


logger = logging.getLogger(__name__)


def get_movies_names(paths=None):
    '''Get a list of movies directors and actors.
    '''
    spec = {
        'type': 'video',
        '$or': [
            {'extra.imdb_director': {'$exists': True}},
            {'extra.imdb_stars': {'$exists': True}},
            ],
        }
    if paths:
        if not isinstance(paths, (tuple, list)):
            paths = [paths]
        spec['file'] = {'$regex': '^(%s)/' % '|'.join([re.escape(p) for p in paths])}

    names = []
    for res in File().find(spec):
        for name_type in ('imdb_director', 'imdb_stars'):
            for name in res['extra'].get(name_type, []):
                if name not in names:
                    names.append(name)
    return names

def get_music_bands(paths=None):
    '''Get a list of music bands.
    '''
    spec = {
        'type': 'audio',
        'info.artist': {'$nin': ['', 'va', 'various']},
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

def get_name_info(name):
    return Imdb().get_info(name, type='name') or {}

def get_band_info(band):
    return Sputnikmusic().get_info(band) or {}

def movie_exists(movie):
    if File().find_one({
            'type': 'video',
            'info.full_name': clean(movie, 1),
            }):
        return True

def album_exists(band, album):
    res = File().find({
            'type': 'audio',
            'info.artist': clean(band, 1),
            'info.album': clean(album, 1),
            }).count()
    if res >= NB_TRACKS_MIN:
        return True

def add_search(query, category, url_info):
    query = clean(query, 1)
    if query and not Search().get(q=query, category=category):
        Search().add(query,
                category=category,
                mode='once',
                langs=SEARCH_LANGS[category],
                url_info=url_info)
        logger.info('added %s search "%s"', category, query)
        return True

def process_movies(search):
    for name in randomize(get_movies_names(search['paths'])):
        logger.info('searching movies from "%s"', name)

        for movie in get_name_info(name).get('titles', []):
            history = search.get('history', [])
            if movie['title'] in history:
                continue

            info = Imdb().get_info(url=movie['url']) or {}
            rating = info.get('rating')
            if not rating or rating < settings.IMDB_RATING_MIN:
                continue
            if movie_exists(movie['title']):
                continue

            if add_search(movie['title'], 'movies', movie['url']):
                history.insert(0, movie['title'])
                MediaFinder().update({'_id': search['_id']}, {'$set': {
                        'processed': datetime.utcnow(),
                        'history': history[:HISTORY_LIMIT],
                        }},
                        safe=True)
                return

def process_music(search):
    for band in randomize(get_music_bands(search['paths'])):
        logger.info('searching similar bands for "%s"', band)

        for similar_band in randomize(get_band_info(band).get('similar_bands', [])):

            # Loop similar band albums by reversed rating order
            albums = get_band_info(similar_band).get('albums', [])
            for album in sorted(albums, key=itemgetter('rating'), reverse=True):
                name = '%s - %s' % (similar_band, album['name'])

                history = search.get('history', [])
                if name in history:
                    continue
                if not album.get('rating') or album['rating'] < settings.SPUTNIKMUSIC_RATING_MIN:
                    continue
                if album_exists(similar_band, album['name']):
                    continue

                if add_search(name, 'music', album['url']):
                    history.insert(0, name)
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
