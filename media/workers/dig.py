from datetime import datetime, timedelta
import logging

from pymongo import ASCENDING

from systools.system import loop, timer, dotdict

from mediacore.model.release import Release
from mediacore.model.similar import SimilarSearch, SimilarResult
from mediacore.model.media import Media
from mediacore.model.search import Search
from mediacore.model.settings import Settings
from mediacore.web.google import Google
from mediacore.web.info import similar_movies, similar_tv, similar_music

from media import settings, get_factory


WORKERS_LIMIT = 5
TIMEOUT_SEEK = 3600     # seconds
DEFAULT_RECURRENCE = 48   # hours
DELTA_SIMILAR_MAX = timedelta(days=365)
DELTA_DATA_MAX = timedelta(days=30)
DELTA_DATA = timedelta(days=1)
UPDATE_DATA_LIMIT = 100

logger = logging.getLogger(__name__)


class Similar(dotdict):

    def __init__(self, doc):
        super(Similar, self).__init__(doc)
        self.media_filters = Settings.get_settings('media_filters')

    def _process_result(self, doc):
        doc['category'] = self.category
        if SimilarResult.find_one(doc):
            return

        if doc['category'] == 'tv':
            doc['mode'] = 'inc'
            doc['season'] = 1
            doc['episode'] = 1
        doc['similar_id'] = self._id
        doc['langs'] = self.get('langs')
        if add_search(**doc):
            doc['created'] = datetime.utcnow()
            SimilarResult.insert(doc, safe=True)
            return True

    def _get_similar_movies(self):
        for movie in similar_movies(self.name,
                type='title', filters=self.media_filters):
            if self._process_result({'name': movie}):
                return True

    def _get_similar_tv(self):
        for tv in similar_tv(self.name, filters=self.media_filters):
            if self._process_result({'name': tv}):
                return True

    def _get_similar_music(self):
        for artist, album in similar_music(self.name,
                filters=self.media_filters):
            if self._process_result({'name': artist, 'album': album}):
                return True

    def process(self):
        logger.info('searching similar %s for "%s"' % (self.category, self.name))
        res = getattr(self, '_get_similar_%s' % self.category)()
        if not res:
            logger.info('failed to find similar %s from "%s"' % (self.category, self.name))
        return res


def _media_exists(**kwargs):
    files = Media.search_files(name=kwargs.get('name'),
            category=kwargs.get('category'),
            album=kwargs.get('album'))
    return len(files) >= settings.FILES_COUNT_MIN.get(kwargs.get('category'), 1)

def add_search(**search):
    if not _media_exists(**search) and Search.add(**search):
        logger.info('added search %s' % search)
        return True

@timer(300)
def process_similar(similar_id):
    search = SimilarSearch.get(similar_id)
    if search:
        Similar(search).process()
        search['processed'] = datetime.utcnow()
        SimilarSearch.save(search, safe=True)

def process_similars():
    count = 0

    for search in SimilarSearch.find(sort=[('processed', ASCENDING)]):
        processed = search.get('processed')
        delta = timedelta(hours=search.get('recurrence', DEFAULT_RECURRENCE))
        if processed and processed > datetime.utcnow() - delta:
            continue

        target = '%s.workers.dig.process_similar' % settings.PACKAGE_NAME
        get_factory().add(target=target,
                args=(search['_id'],), timeout=TIMEOUT_SEEK)

        count += 1
        if count == WORKERS_LIMIT:
            break

def process_releases():
    media_langs = Settings.get_settings('media_langs')
    for release in Release.find({
            'processed': False,
            'valid': {'$exists': True},
            }):
        valid = release['valid']
        subtype = release['info'].get('subtype')

        if not valid and subtype == 'music' \
                and _media_exists(name=release['artist'], category='music'):
            valid = True
        if valid is None:
            continue

        if valid:
            search = Release.get_search(release)
            search['langs'] = media_langs.get(subtype, [])
            add_search(**search)

        Release.update({'_id': release['_id']},
                {'$set': {'processed': datetime.utcnow()}}, safe=True)

@loop(minutes=5)
def run():
    if Google().accessible:
        process_similars()

    process_releases()

    SimilarResult.remove({'created': {
            '$lt': datetime.utcnow() - DELTA_SIMILAR_MAX,
            }}, safe=True)
