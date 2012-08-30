from datetime import datetime, timedelta
import logging
from copy import copy

from pymongo import ASCENDING

from mediaworker import settings, get_factory

from systools.system import loop, timer, dotdict

from mediacore.model.search import Search as MSearch
from mediacore.model.media import Media
from mediacore.model.result import Result
from mediacore.web.torrent import results
from mediacore.web.google import Google
from mediacore.util.title import Title


WORKERS_LIMIT = 5
TIMEOUT_SEARCH = 1800    # seconds
DELTA_SEARCH = {
    'once': timedelta(hours=6),
    'inc': timedelta(hours=6),
    'ever': timedelta(hours=12),
    }
DELTA_FILES_SEARCH = timedelta(hours=1)
DELTA_RESULT = {
    'once': timedelta(hours=24),
    'inc': timedelta(hours=12),
    'ever': timedelta(hours=24),
    }
DELTA_IDLE = timedelta(days=10)
DELTA_IDLE_SEARCH = timedelta(days=2)
DELTA_OBSOLETE = timedelta(days=30)
DELTA_NEXT_SEASON = timedelta(days=60)
DELTA_RESULTS_MAX = timedelta(days=180)
PAGES_MAX = 20
SEARCH_LIMIT = 10
NB_SEEDS_MIN = {
    'once': 0,
    'inc': 10,
    'ever': 1,
    }
FILES_COUNT_MIN = {'music': 3}


logger = logging.getLogger(__name__)


class Search(dotdict):
    def __init__(self, doc):
        super(Search, self).__init__(doc)
        self.langs = self.get('langs') or []
        self.results = self.get('results', [])

        session = self.get('session', {})
        if session.get('nb_downloads') == 0 \
                and session.get('nb_errors') == 0 \
                and session.get('nb_pending') == 0:
            sort_results = 'age'
            pages_max = 1
        else:
            sort_results = 'seeds'
            pages_max = PAGES_MAX

        self.session = {
            'first_search': session.get('first_search'),
            'last_search': session.get('last_search'),
            'last_result': session.get('last_result'),
            'last_download': session.get('last_download'),
            'last_files_search': session.get('last_files_search'),
            'nb_processed': session.get('nb_processed', 0),
            'sort_results': sort_results,
            'pages_max': pages_max,
            'nb_results': 0,
            'nb_pending': 0,
            'nb_downloads': 0,
            'nb_errors': 0,
            }

    def _get_query(self):
        return MSearch().get_query(self)

    def _add_next(self, mode):
        '''Create a search for next episode or season.
        '''
        search = MSearch().get_next(self, mode=mode)
        if search and MSearch().add(**search):
            logger.info('added search %s', search)

    def _is_finished(self):
        if self.mode == 'ever':
            return
        date = self.session['last_files_search']
        if date and date > datetime.utcnow() - DELTA_FILES_SEARCH:
            return

        files = Media().search_files(**self)
        if len(files) >= FILES_COUNT_MIN.get(self.category, 1):
            if self.mode == 'inc':
                self._add_next('episode')
            MSearch().remove({'_id': self._id}, safe=True)
            logger.info('removed %s search "%s": found %s', self.category, self._get_query(), files)
            return True

        self.session['last_files_search'] = datetime.utcnow()
        MSearch().save(self, safe=True)

    def _is_obsolete(self):
        # TODO: handle obsolete episodes searches
        if self.mode in ('inc', 'ever'):
            return
        date = self.session['last_result'] or self.session['first_search']
        if date and date < datetime.utcnow() - DELTA_OBSOLETE:
            MSearch().remove({'_id': self._id}, safe=True)
            logger.info('removed search "%s" (no result for %d days)', self._get_query(), DELTA_OBSOLETE.days)
            return True

    def _validate_dates(self):
        now = datetime.utcnow()

        if self.session['last_search'] \
                and self.session['last_search'] > now - DELTA_SEARCH[self.mode]:
            return False
        if self.session['last_download'] \
                and self.session['last_download'] > now - DELTA_SEARCH[self.mode]:
            return False

        date = self.session['last_result'] or self.session['first_search']
        if date and date < now - DELTA_IDLE:
            if self.session['last_search'] \
                    and self.session['last_search'] > now - DELTA_IDLE_SEARCH:
                return False

        return True

    def _check_episode(self):
        if self.get('season') and self.get('episode') and self.episode > 2 \
                and self.session['first_search'] \
                and self.session['first_search'] < datetime.utcnow() - DELTA_NEXT_SEASON:
            self._add_next('season')

    def validate(self):
        if self._is_finished() or self._is_obsolete():
            return False
        if not self._validate_dates():
            return False
        self._check_episode()
        return True

    def _get_filters(self):
        res = copy(settings.SEARCH_FILTERS.get(self.category, {}))
        res['re_incl'] = Title(self._get_query()).get_search_re()
        res['langs'] = self.langs
        return res

    def _validate_result(self, result):
        '''Check result dynamic attributes.
        '''
        if result.date and result.date > datetime.utcnow() - DELTA_RESULT[self.mode]:
            logger.info('filtered "%s" (%s): too recent (%s)', result.title, result.net_name, result.date)
            return False

        seeds = result.get('seeds')
        if seeds is not None and seeds < NB_SEEDS_MIN[self.mode]:
            logger.info('filtered "%s" (%s): not enough seeds (%s)', result.title, result.net_name, seeds)
            return False

        return True

    def process(self):
        query = self._get_query()

        logger.info('processing %s search "%s"', self.category, query)

        for result in results(query,
                category=self.category,
                sort=self.session['sort_results'],
                pages_max=self.session['pages_max'],
                **self._get_filters()):
            if not result:
                self.session['nb_errors'] += 1
                continue
            if Result().find_one({'hash': result.hash}):
                continue

            self.session['nb_results'] += 1
            if not self._validate_result(result):
                self.session['nb_pending'] += 1
                continue

            if self.mode == 'inc':
                self._add_next('episode')

            result['search_id'] = self._id
            result_id = Result().insert(result, safe=True)
            self.results.insert(0, result_id)
            self.session['nb_downloads'] += 1
            logger.info('found "%s" on %s', result.title, result.net_name)

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

        MSearch().save(self, safe=True)


@timer(300)
def process_search(search_id):
    search = MSearch().get(search_id)
    if not search:
        return
    search = Search(search)
    search.process()
    search.save()

@loop(60)
def process_searches():
    if Google().accessible:
        count = 0

        for search in MSearch().find(
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

    Result().remove({'created': {'$lt': datetime.utcnow() - DELTA_RESULTS_MAX}},
            safe=True)

def main():
    process_searches()
