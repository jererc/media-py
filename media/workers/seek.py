from datetime import datetime, timedelta
import logging

from pymongo import ASCENDING

from systools.system import loop, timer, dotdict

from mediacore.model.release import Release
from mediacore.model.similar import SimilarSearch, SimilarResult
from mediacore.model.media import Media
from mediacore.model.search import Search
from mediacore.web.google import Google
from mediacore.web.info import similar_movies, similar_tv, similar_music
from mediacore.utils.filter import validate_extra

from media import settings, get_factory


WORKERS_LIMIT = 5
TIMEOUT_SEEK = 3600     # seconds
DEFAULT_RECURRENCE = 48   # hours
DELTA_SIMILAR_MAX = timedelta(days=365)
SEARCH_LANGS = {
    'movies': settings.MOVIES_SEARCH_LANGS,
    'tv': settings.TV_SEARCH_LANGS,
    'music': None,
    }
FILES_COUNT_MIN = {'music': 3}

logger = logging.getLogger(__name__)


class Similar(dotdict):

    def __init__(self, doc):
        super(Similar, self).__init__(doc)

    def _process_result(self, doc):
        doc['category'] = self.category
        if SimilarResult.find_one(doc):
            return

        if doc['category'] == 'tv':
            doc['mode'] = 'inc'
            doc['season'] = 1
            doc['episode'] = 1
        doc['similar_id'] = self._id
        if add_search(**doc):
            doc['created'] = datetime.utcnow()
            SimilarResult.insert(doc, safe=True)
            return True

    def _get_similar_movies(self):
        for movie in similar_movies(self.name, type='title',
                filters=settings.MEDIA_FILTERS):
            if self._process_result({'name': movie}):
                return True

    def _get_similar_tv(self):
        for tv in similar_tv(self.name,
                filters=settings.MEDIA_FILTERS):
            if self._process_result({'name': tv}):
                return True

    def _get_similar_music(self):
        for artist, album in similar_music(self.name,
                filters=settings.MEDIA_FILTERS):
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

    return len(files) >= FILES_COUNT_MIN.get(kwargs.get('category'), 1)

def add_search(**search):
    if _media_exists(**search):
        return

    search['langs'] = SEARCH_LANGS.get(search['category'])
    if Search.add(**search):
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

        target = '%s.workers.seek.process_similar' % settings.PACKAGE_NAME
        get_factory().add(target=target,
                args=(search['_id'],), timeout=TIMEOUT_SEEK)

        count += 1
        if count == WORKERS_LIMIT:
            break

def process_releases():
    for release in Release.find({
            'processed': False,
            'updated': {'$exists': True},
            }):
        subtype = release['info'].get('subtype')

        if subtype == 'music' and _media_exists(name=release['artist'],
                category='music'):
            valid = True
        else:
            valid = validate_extra(release['extra'], settings.MEDIA_FILTERS)
            if valid is None:
                continue

        if valid:
            add_search(**Release.get_search(release))
        Release.update({'_id': release['_id']},
                {'$set': {'processed': datetime.utcnow()}}, safe=True)

@loop(minutes=5)
def run():
    if Google().accessible:
        process_similars()

    process_releases()

    SimilarResult.remove({'created': {'$lt': datetime.utcnow() - DELTA_SIMILAR_MAX}},
            safe=True)
