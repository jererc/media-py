#!/usr/bin/env python
from datetime import datetime, timedelta
import logging

import pymongo

from mediaworker import env

from systools.system import loop, timeout, timer

from mediacore.model.search import Search as MSearch
from mediacore.model.file import File
from mediacore.model.result import Result
from mediacore.web.torrent import results
from mediacore.web.google import Google
from mediacore.util.title import Title
from mediacore.util.util import list_in, in_range


DELTA_SEARCH_MIN = {
    'once': timedelta(hours=24),
    'inc': timedelta(hours=12),
    'ever': timedelta(hours=24),
    }
DELTA_IDLE_SEARCH_MIN = timedelta(days=2)
DELTA_RESULT_MIN = {
    'once': timedelta(hours=24),
    'inc': timedelta(hours=24),
    'ever': timedelta(hours=24),
    }
DELTA_IDLE = timedelta(days=10)
DELTA_OBSOLETE = timedelta(days=30)
DELTA_NEXT_SEASON = timedelta(days=60)
DELTA_RESULTS_MAX = timedelta(days=120)

PAGES_MAX = 20
NB_SEEDS_MIN = {
    'once': 0,
    'inc': 10,
    'ever': 1,
    }
NB_DOWNLOADS_MAX = 1

CAT_DEF = {     # local categories correspondances
    'anime': 'video',
    'apps': None,
    'books': None,
    'games': None,
    'movies': 'video',
    'music': 'audio',
    'tv': 'video',
    }
SIZE_DEF = {    # size ranges in MB
    'anime': {'min': 100, 'max': 1000},
    'apps': {'min': None, 'max': None},
    'books': {'min': None, 'max': None},
    'games': {'min': None, 'max': None},
    'movies': {'min': 600, 'max': 2000},
    'music': {'min': 40, 'max': 200},
    'tv': {'min': 100, 'max': 1000},
    }
NB_FILES_DEF = {    # min number of matching local files to find
    'anime': 1,
    'apps': 1,
    'books': 1,
    'games': 1,
    'movies': 1,
    'music': 3,
    'tv': 1,
    }


logger = logging.getLogger(__name__)


class Search(dict):
    def __init__(self, doc):
        doc['mode'] = doc.get('mode', 'once')
        doc['langs'] = doc.get('langs', [])

        session = doc.get('session', {})

        if session.get('nb_downloads') == 0 \
                and session.get('nb_errors') == 0 \
                and session.get('nb_pending') == 0:
            sort_results = 'age'
            pages_max = 1
        else:
            sort_results = 'seeds'
            pages_max = PAGES_MAX

        doc['session'] = {
            'first_search': session.get('first_search'),
            'last_search': session.get('last_search'),
            'last_result': session.get('last_result'),
            'last_download': session.get('last_download'),
            'sort_results': sort_results,
            'pages_max': pages_max,
            'nb_results': 0,
            'nb_pending': 0,
            'nb_downloads': 0,
            'nb_errors': 0,
            }

        super(Search, self).__init__(doc)

    __getattr__ = dict.__getitem__

    def __setattr__(self, attr_name, value):
        if hasattr(getattr(self.__class__, attr_name, None), '__set__'):
            return object.__setattr__(self, attr_name, value)
        else:
            return self.__setitem__(attr_name, value)

    def __str__(self):
        return '%s(%s)' % (self.__class__.__name__, dict(self))

    def __repr__(self):
        return self.__str__()

    def __delattr__(self, attr_name):
        if attr_name.startswith('_'):
            return object.__delattr__(self, attr_name)
        else:
            return self.__delitem__(attr_name)

    def _check_dates(self):
        now = datetime.utcnow()

        if self.session['last_search'] and self.session['last_search'] > now - DELTA_SEARCH_MIN[self.mode]:
            return False
        if self.session['last_download'] and self.session['last_download'] > now - DELTA_SEARCH_MIN[self.mode]:
            return False

        date_ = self.session['last_result'] or self.session['first_search']
        if date_ and date_ < now - DELTA_IDLE:
            if self.session['last_search'] and self.session['last_search'] > now - DELTA_IDLE_SEARCH_MIN:
                return False

        return True

    def _search_files(self):
        files = File().search(self.q, CAT_DEF[self.category])
        if len(files) >= NB_FILES_DEF[self.category]:
            MSearch().remove(id=self._id)
            logger.info('removed %s search "%s": found %s', self.category, self.q, files[0])
            return True

    def _check_result_dynamic(self, result):
        '''Check result dynamic attributes.
        '''
        if result.date and result.date > datetime.utcnow() - DELTA_RESULT_MIN[self.mode]:
            logger.info('filtered "%s" (%s): too recent (%s)', result.title, result.net_name, result.date)
            return False
        seeds = getattr(result, 'seeds', None)
        if seeds is not None and seeds < NB_SEEDS_MIN[self.mode]:
            logger.info('filtered "%s" (%s): not enough seeds (%s)', result.title, result.net_name, seeds)
            return False
        return True

    def _check_result(self, result):
        '''Check result fixed attributes.
        '''
        if getattr(result, 'private', False):
            logger.info('filtered "%s" (%s): private tracker', result.title, result.net_name)
            return False
        if self.langs and not list_in(self.langs, Title(result.title).langs, all=False):
            logger.info('filtered "%s" (%s): languages do not match', result.title, result.net_name)
            return False
        if result.size is not None and not in_range(result.size, SIZE_DEF[self.category]['min'], SIZE_DEF[self.category]['max']):
            logger.info('filtered "%s" (%s): size does not match (%s MB)', result.title, result.net_name, result.size)
            return False
        return True

    def _add_next(self, mode):
        '''Create a search for next episode or season.
        '''
        query = None
        if mode == 'episode':
            query = MSearch().get_next_episode(self.q)
        elif mode == 'season':
            query = MSearch().get_next_season(self.q)

        if query and not MSearch().get(q=query):
            MSearch().add(query,
                    category=self.category,
                    mode=self.mode,
                    langs=self.langs)

    def validate(self):
        if not self._check_dates():
            return False
        if self.mode == 'ever':
            return True
        if self._search_files():
            return False

        if self.mode == 'inc' and self.category in ('tv', 'anime'):
            # Episodes next season
            title = Title(self.q)
            if title.season and title.episode and int(title.episode) > 2 \
                    and self.session['first_search'] \
                    and self.session['first_search'] < datetime.utcnow() - DELTA_NEXT_SEASON:
                self._add_next('season')

            # TODO: remove obsolete episodes searches

        else:
            # Remove obsolete searches
            date_ = self.session['last_result'] or self.session['first_search']
            if date_ and date_ < datetime.utcnow() - DELTA_OBSOLETE:
                MSearch().remove(self._id)
                logger.info('removed search "%s" (no result for %d days)', self.q, DELTA_OBSOLETE.days)
                return False

        return True

    def process(self):
        logger.info('processing %s search "%s"', self.category, self.q)

        re_incl_query = Title(self.q).get_search_re('word3' if self.mode != 'inc' else None)
        for result in results(self.q,
                category=self.category,
                sort=self.session['sort_results'],
                pages_max=self.session['pages_max'],
                re_incl=re_incl_query):
            if not result:
                self.session['nb_errors'] += 1
                continue
            if Result().find_one({'hash': result.hash}):
                continue

            self.session['nb_results'] += 1
            if not self._check_result_dynamic(result):
                self.session['nb_pending'] += 1
                continue

            doc = {
                'hash': result.hash,
                'title': result.title,
                'net_name': result.net_name,
                'url_magnet': result.url_magnet,
                'search_id': self._id,
                'created': datetime.utcnow(),
                'processed': False,
                }

            if not self._check_result(result):
                doc['processed'] = datetime.utcnow()
                Result().insert(doc)
                continue

            if self.mode == 'inc':
                self._add_next('episode')

            Result().insert(doc)
            self.session['nb_downloads'] += 1
            logger.info('found "%s" on %s', result.title, result.net_name)

            if self.mode != 'ever' and self.session['nb_downloads'] >= NB_DOWNLOADS_MAX:
                break

    def save(self):
        now = datetime.utcnow()

        if not self.session['first_search']:
            self.session['first_search'] = now
        if self.session['nb_errors'] <= 1:
            self.session['last_search'] = now
        if self.session['nb_results']:
            self.session['last_result'] = now
        if self.session['nb_downloads']:
            self.session['last_download'] = now

        MSearch().save(self, safe=True)


def process():
    for res in MSearch().find(sort=[('last_search', pymongo.ASCENDING)],
            timeout=False):
        search = Search(res)
        if search.validate():
            search.process()
            search.save()

@loop(60)
@timeout(hours=2)
@timer
def main():
    if Google().accessible:
        process()

    Result().remove({'created': {'$lt': datetime.utcnow() - DELTA_RESULTS_MAX}}, safe=True)


if __name__ == '__main__':
    main()
