#!/usr/bin/env python
import re
from datetime import datetime, timedelta
import logging

from mediaworker import env, settings

from systools.system import loop, timeout, timer, dotdict

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


class SimilarSearch(dotdict):
    def __init__(self, doc):
        super(SimilarSearch, self).__init__(doc)
        self.category = self.get('category')
        self.history = self.get('history', [])
        self.processed = self.get('processed')

    def validate(self):
        if not self.processed:
            return True
        if self.processed < datetime.utcnow() - timedelta(hours=self.recurrence):
            return True

    def _validate_history(self, args):
        for info in self.history:
            if args.get('name') != info.get('name'):
                continue
            if args.get('album') != info.get('album'):
                continue
            if args.get('category') != info.get('category'):
                continue
            return False
        return True

    def _process_result(self, args):
        if self._validate_history(args) and add_search(**args):
            self.history.insert(0, args)
            return True

    def _get_similar_movies(self, media):
        for similar_movie in similar_movies(media['name'], type='title',
                filters=settings.MEDIA_FILTERS):
            if self._process_result({
                    'name': similar_movie,
                    'category': 'movies',
                    'media_id': media['_id'],
                    }):
                return True

        logger.info('failed to find similar movies from "%s"', media['name'])

    def _get_similar_music(self, media):
        for similar_band, album in similar_music(media['info'].get('artist'),
                filters=settings.MEDIA_FILTERS):
            if self._process_result({
                    'name': similar_band,
                    'album': album,
                    'category': 'music',
                    'media_id': media['_id'],
                    }):
                return True

        logger.info('failed to find similar music from "%s"', media['info'].get('artist'))

    def get_similar(self, media_id):
        media = Media().get(media_id)
        if not media:
            return

        category = media['info'].get('subtype')
        if category == 'movies':
            return self._get_similar_movies(media)
        elif category == 'music':
            return self._get_similar_music(media)

    def _get_files_pattern(self, bases):
        if not bases:
            return
        if not isinstance(bases, (tuple, list)):
            bases = [bases]
        return r'^(%s)' % '|'.join([re.escape(b) for b in bases])

    def _get_movies_ids(self, bases=None):
        '''Get a list of movies media ids.

        :return: list of tuples (movie, Media id)
        '''
        movies = {}
        spec = {
            'info.subtype': 'movies',
            'extra.imdb': {'$exists': True},
            }
        if bases:
            spec['files'] = {'$regex': self._get_files_pattern(bases)}

        for media in Media().find(spec):
            if media['name'] and media['name'] not in movies:
                movies[media['name']] = media['_id']
        return movies.values()

    def _get_music_ids(self, bases=None):
        '''Get a list of music media ids.

        :return: list of tuples (band, Media id)
        '''
        bands = {}
        spec = {
            'info.subtype': 'music',
            'info.artist': {'$nin': ['', 'va', 'various']},
            }
        if bases:
            spec['files'] = {'$regex': self._get_files_pattern(bases)}

        for media in Media().find(spec):
            band = media['info'].get('artist')
            if band:
                bands.setdefault(band, {'_id': media['_id'], 'count': 0})
                bands[band]['count'] += len(media['files'])

        res = []
        for band, info in bands.items():
            if info['count'] >= FILES_COUNT_MIN['music']:
                res.append(info['_id'])
        return res

    def _get_media_ids(self):
        res = []
        if self.category == 'movies':
            res = self._get_movies_ids(self.paths)
        elif self.category == 'music':
            res = self._get_music_ids(self.paths)
        return res

    @timeout(minutes=30)
    def process(self):
        '''Process a similar search.
        '''
        if self.get('media_id'):
            media_ids = [self.media_id]
        else:
            media_ids = randomize(self._get_media_ids())

        for media_id in media_ids:
            if self.get_similar(media_id):
                return True

    def save(self):
        self.history = self.history[:HISTORY_LIMIT]
        self.processed = datetime.utcnow()
        Similar().save(self, safe=True)


def _media_exists(**kwargs):
    files = Media().search_files(**kwargs)
    return len(files) >= FILES_COUNT_MIN.get(kwargs.get('category'), 1)

def add_search(**search):
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
            add_search(**Release().get_search(release))

        Release().update({'_id': release['_id']},
                {'$set': {'processed': datetime.utcnow()}}, safe=True)

def process_media():
    for res in Similar().find():
        search = SimilarSearch(res)
        if search.validate():
            search.process()
            search.save()

@loop(minutes=5)
@timeout(hours=1)
@timer()
def main():
    if Google().accessible:
        process_media()

    process_releases()


if __name__ == '__main__':
    main()
