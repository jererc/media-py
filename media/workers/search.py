import os.path
from datetime import datetime, timedelta
from copy import copy
import logging

from pymongo import ASCENDING

from systools.system import loop, timer, dotdict

from transfer import Transfer

from filetools.title import Title

from mediacore.model.search import Search as MSearch
from mediacore.model.media import Media
from mediacore.model.result import Result
from mediacore.model.settings import Settings
from mediacore.web.search import results
from mediacore.web.google import Google
from mediacore.web.netflix import Netflix, NETFLIX_CATEGORIES

from media import settings, get_factory


WORKERS_LIMIT = 5
TIMEOUT_SEARCH = 1800    # seconds
DELTA_SEARCH = {
    'inc': timedelta(hours=4),
    'once': timedelta(hours=4),
    'ever': timedelta(hours=6),
    }
DELTA_FILE_SEARCH = timedelta(hours=1)
DELTA_URL_SEARCH = timedelta(hours=24)
DELTA_RESULT = {
    'once': timedelta(hours=24),
    'inc': timedelta(hours=12),
    'ever': timedelta(hours=24),
    }
DELTA_IDLE = timedelta(days=10)
DELTA_IDLE_SEARCH = timedelta(days=2)
DELTA_OBSOLETE = timedelta(days=90)
DELTA_NEXT_SEASON = timedelta(days=60)
PAGES_MAX = 20
SEARCH_LIMIT = 10
NB_SEEDS_MIN = {
    'once': 0,
    'inc': 10,
    'ever': 1,
    }

logger = logging.getLogger(__name__)


class Search(dotdict):

    def __init__(self, doc):
        super(Search, self).__init__(doc)
        self.langs = self.get('langs') or []
        self.transfers = self.get('transfers', [])

        session = self.get('session', {})
        if session.get('nb_downloads') == 0 \
                and session.get('nb_errors') == 0 \
                and session.get('nb_pending') == 0:
            sort_results = 'date'
            pages_max = 1
        else:
            sort_results = 'popularity'
            pages_max = PAGES_MAX

        self.session = {
            'first_search': session.get('first_search'),
            'last_search': session.get('last_search'),
            'last_result': session.get('last_result'),
            'last_download': session.get('last_download'),
            'last_file_search': session.get('last_file_search'),
            'last_url_search': session.get('last_url_search'),
            'nb_processed': session.get('nb_processed', 0),
            'sort_results': sort_results,
            'pages_max': pages_max,
            'nb_results': 0,
            'nb_pending': 0,
            'nb_downloads': 0,
            'nb_errors': 0,
            }

    def _get_query(self):
        return MSearch.get_query(self)

    def _add_next(self, mode):
        '''Create a search for next episode or season.
        '''
        search = MSearch.get_next(self, mode=mode)
        if search and MSearch.add(**search):
            logger.info('added search %s', search)

    def _search_file(self):
        if self.mode == 'ever':
            return
        date = self.session['last_file_search']
        if date and date > datetime.utcnow() - DELTA_FILE_SEARCH:
            return

        files = Media.search_files(**self)
        if len(files) >= settings.FILES_COUNT_MIN.get(self.category, 1):
            if self.mode == 'inc':
                self._add_next('episode')
            MSearch.remove({'_id': self._id}, safe=True)
            logger.info('removed %s search "%s": found files %s', self.category, self._get_query(), files)
            return True

        MSearch.update({'_id': self._id},
                {'$set': {'session.last_file_search': datetime.utcnow()}},
                safe=True)

    def _is_obsolete(self):
        if self.mode in ('inc', 'ever'):
            return
        date = self.session['last_result'] or self.session['first_search']
        if date and date < datetime.utcnow() - DELTA_OBSOLETE:
            MSearch.remove({'_id': self._id}, safe=True)
            logger.info('removed search "%s" (no result for %d days)', self._get_query(), DELTA_OBSOLETE.days)
            return True

    def _validate_dates(self):
        date = self.session['last_search']
        if date and datetime.utcnow() < date + DELTA_SEARCH[self.mode]:
            return False
        return True

    def _check_episode(self):
        if self.get('season') and self.get('episode') and self.episode > 2 \
                and self.session['first_search'] \
                and self.session['first_search'] < datetime.utcnow() - DELTA_NEXT_SEASON:
            self._add_next('season')

    def validate(self):
        if self._search_file() or self._is_obsolete():
            return False
        if not self._validate_dates():
            return False
        self._check_episode()
        return True

    def _get_filters(self, query):
        filters = Settings.get_settings('search_filters')
        res = copy(filters.get(self.category, {}))
        res['include'] = Title(query).get_search_re(auto=True)
        res['langs'] = self.langs
        return res

    def _validate_result(self, result):
        '''Check result dynamic attributes.
        '''
        if result.type == 'torrent':
            date = result.get('date')
            if date and date > datetime.utcnow() - DELTA_RESULT[self.mode]:
                logger.info('filtered "%s" (%s): too recent (%s)', result.title, result.plugin, date)
                return False

            seeds = result.get('seeds')
            if seeds is not None and seeds < NB_SEEDS_MIN[self.mode]:
                logger.info('filtered "%s" (%s): not enough seeds (%s)', result.title, result.plugin, seeds)
                return False

        return True

    def _search_url(self):
        date = self.session['last_url_search']
        if date and date > datetime.utcnow() - DELTA_URL_SEARCH:
            return

        if self.category not in NETFLIX_CATEGORIES:
            return
        netflix_ = Settings.get_settings('netflix')
        if not netflix_['username'] or not netflix_['password']:
            return
        netflix = get_netflix_object(netflix_['username'], netflix_['password'])
        if not netflix:
            return

        res = netflix.get_info(self.name, self.category)
        if res:
            Media.add_url(url=res['url'], name=res['title'],
                    category=self.category)
            logger.info('found "%s" on netflix (%s)', res['title'], res['url'])
            if self.category == 'movies':
                MSearch.remove({'_id': self._id}, safe=True)
                logger.info('removed %s search "%s": found url %s', self.category, self.name, res['url'])
                return True

        MSearch.update({'_id': self._id},
                {'$set': {'session.last_url_search': datetime.utcnow()}},
                safe=True)

    def process(self):
        query = self._get_query()
        dst = Settings.get_settings('paths')['finished_download']

        logger.info('processing %s search "%s"', self.category, query)

        if self._search_url():
            return

        for result in results(query,
                category=self.category,
                sort=self.session['sort_results'],
                pages_max=self.session['pages_max'],
                **self._get_filters(query)):
            if not result:
                self.session['nb_errors'] += 1
                continue

            Result.add_result(result, search_id=self._id)

            if not result.auto:
                continue
            if self.safe and not result.safe:
                continue

            if result.get('hash'):
                spec = {'info.hash': result.hash}
            else:
                spec = {'src': result.url}
            if Transfer.find_one(spec):
                continue

            self.session['nb_results'] += 1
            if not self._validate_result(result):
                self.session['nb_pending'] += 1
                continue

            if self.mode == 'inc':
                self._add_next('episode')

            transfer_id = Transfer.add(result.url, dst, type=result.type)
            self.transfers.insert(0, transfer_id)

            self.session['nb_downloads'] += 1
            logger.info('found "%s" on %s (%s)', result.title, result.plugin, result.url)

            if self.mode != 'ever':
                break

    def save(self):
        now = datetime.utcnow()

        if not self.session['first_search']:
            self.session['first_search'] = now
        if self.session['nb_results']:
            self.session['last_result'] = now
        if self.session['nb_downloads']:
            self.session['last_download'] = now
        if self.session['nb_errors'] <= 1:
            self.session['last_search'] = now
            self.session['nb_processed'] += 1

        MSearch.save(self, safe=True)


def get_netflix_object(username, password):
    path_tmp = Settings.get_settings('paths')['tmp']
    res = Netflix(username, password,
            cookie_file=os.path.join(path_tmp, 'netflix_cookies.txt'))
    if res.logged:
        return res

@timer(300)
def process_search(search_id):
    search = MSearch.get(search_id)
    if search:
        search = Search(search)
        search.process()
        search.save()

def process_searches():
    count = 0

    for search in MSearch.find(
            sort=[('session.last_search', ASCENDING)]):
        search = Search(search)
        if not search.validate():
            continue

        target = '%s.workers.search.process_search' % settings.PACKAGE_NAME
        get_factory().add(target=target,
                args=(search._id,), timeout=TIMEOUT_SEARCH)

        count += 1
        if count == WORKERS_LIMIT:
            break

@loop(60)
def run():
    if Google().accessible:
        process_searches()
