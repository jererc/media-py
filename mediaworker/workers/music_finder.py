#!/usr/bin/env python
import re
from datetime import datetime, timedelta
import logging

from mediaworker import env

from systools.system import loop, timeout, timer

from mediacore.model.music_finder import MusicFinder
from mediacore.model.file import File
from mediacore.model.search import Search
from mediacore.web.google import Google
from mediacore.web.sputnikmusic import Sputnikmusic
from mediacore.util.title import clean
from mediacore.util.util import randomize


NB_TRACKS_MIN = 5
SEARCHES_COUNT = 100


logger = logging.getLogger(__name__)


def find_band(band):
    res = File().find({
            'info.artist': clean(band, 1),
            'type': 'audio',
            }).count()
    if res >= NB_TRACKS_MIN:
        return True

def get_bands(paths=None):
    '''Get a list of bands.
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

def get_band_info(band):
    info = Sputnikmusic().get_info(band)
    if not info:
        return {}
    return info

def add_search(band):
    '''Add a search for each band album.
    '''
    albums = get_band_info(band).get('albums')
    if albums:
        for album in albums:
            q = clean('%s %s' % (band, album['name']), 1)
            Search().add(q, category='music', mode='once')
            logger.info('added music search "%s"', q)
        return True

@loop(minutes=10)
@timeout(hours=1)
@timer()
def main():
    if not Google().accessible:
        return

    for search in MusicFinder().find():
        date = search.get('processed')
        if date and date + timedelta(hours=search['recurrence']) > datetime.utcnow():
            continue

        logger.info('processing paths %s', search['paths'])

        for band in randomize(get_bands(search['paths'])):
            logger.info('searching "%s" similar bands', band)

            for similar_band in get_band_info(band).get('similar_bands', []):
                bands_searched = search.get('bands_searched', [])
                if similar_band in bands_searched:
                    continue
                if find_band(similar_band):
                    continue

                # Add search
                if add_search(similar_band):
                    bands_searched.insert(0, similar_band)
                    MusicFinder().update({'_id': search['_id']}, {'$set': {
                            'processed': datetime.utcnow(),
                            'bands_searched': bands_searched[:SEARCHES_COUNT],
                            }},
                            safe=True)
                    return


if __name__ == '__main__':
    main()
