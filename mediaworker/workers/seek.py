#!/usr/bin/env python
import os.path
from datetime import datetime, timedelta
import logging

from pymongo import ASCENDING

from mediaworker import env, settings

from systools.system import loop, timeout, timer, dotdict

from mediacore.model.release import Release
from mediacore.model.similar import Similar
from mediacore.model.media import Media
from mediacore.model.search import Search
from mediacore.model.worker import Worker
from mediacore.web.google import Google
from mediacore.web.info import similar_movies, similar_music
from mediacore.util.filter import validate_extra


NAME = os.path.splitext(os.path.basename(__file__))[0]
DELTA_SIMILAR_SEARCH = timedelta(hours=24)
DELTA_SIMILAR_MAX = timedelta(days=365)
SEARCH_LANGS = {
    'movies': settings.MOVIES_SEARCH_LANGS,
    'tv': settings.TV_SEARCH_LANGS,
    'music': None,
    }
FILES_COUNT_MIN = {'music': 3}


logger = logging.getLogger(__name__)


class SimilarMedia(dotdict):
    def __init__(self, doc):
        super(SimilarMedia, self).__init__(doc)

    def _process_result(self, doc):
        doc['category'] = self.info.get('subtype')
        if Similar().find_one(doc):
            return

        doc['media_id'] = self._id
        if add_search(**doc):
            doc['created'] = datetime.utcnow()
            Similar().insert(doc, safe=True)

            self.last_similar_search = datetime.utcnow()
            Media().save(self, safe=True)
            return True

    def _get_similar_movies(self):
        for movie in similar_movies(self.name, type='title',
                filters=settings.MEDIA_FILTERS):
            if self._process_result({'name': movie}):
                return True

        logger.info('failed to find similar movies from "%s"', self.name)

    def _get_similar_music(self):
        for artist, album in similar_music(self.info.get('artist'),
                filters=settings.MEDIA_FILTERS):
            if self._process_result({'name': artist, 'album': album}):
                return True

        logger.info('failed to find similar music from "%s"', self.info.get('artist'))

    @timeout(minutes=30)
    def process(self):
        category = self.info.get('subtype')

        logger.info('searching similar %s for "%s"', category, self.name)

        if category == 'movies':
            return self._get_similar_movies()
        elif category == 'music':
            return self._get_similar_music()


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

def process_media():
    if not validate_similar():
        return
    for media in Media().find({'similar_search': True},
            sort=[('last_similar_search', ASCENDING)]):
        if SimilarMedia(media).process():
            break

    Worker().set_attr(NAME, 'similar_search', datetime.utcnow())

    Similar().remove({'created': {'$lt': datetime.utcnow() - DELTA_SIMILAR_MAX}},
            safe=True)

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

def validate_similar():
    res = Worker().get_attr(NAME, 'similar_search')
    if not res or res < datetime.utcnow() - DELTA_SIMILAR_SEARCH:
        return True

@loop(minutes=5)
@timeout(hours=1)
@timer()
def main():
    if Google().accessible:
        process_media()

    process_releases()


if __name__ == '__main__':
    main()
