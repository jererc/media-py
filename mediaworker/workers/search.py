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


PAGES_MAX = 20
LOCAL_SEARCH_RECURRENCE = timedelta(minutes=30)
AGE_SEARCH_MIN = {
    'once': timedelta(hours=24),
    'ever': timedelta(hours=24),
    'inc': timedelta(hours=12),
    }
AGE_DOWNLOAD_MIN = {
    'once': timedelta(hours=24),
    'ever': timedelta(hours=24),
    'inc': timedelta(hours=24),
    }
AGE_RESULTS_MAX = timedelta(days=120)   # max results age
AGE_DOWNLOADS_MAX = timedelta(days=120)   # max downloads age
NB_SEEDS_MIN = {
    'once': 0,
    'ever': 1,
    'inc': 10,
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


class Search(object):
    def __init__(self, doc):
        self.id = doc['_id']
        self.q = doc.get('q')
        self.category = doc.get('category')
        self.mode = doc.get('mode', 'once')
        self.langs = doc.get('langs', [])
        self.first_search = doc.get('first_search')
        self.last_search = doc.get('last_search')
        self.last_activity = doc.get('last_activity')
        self.last_download = doc.get('last_download')
        self.last_search_local = doc.get('last_search_local')

        self.pages_max = 1 if doc.get('downloads') == 0 else PAGES_MAX
        self.nb_results = 0
        self.nb_errors = 0
        self.nb_downloads = 0

    def check_dates(self):
        '''Check search dates.
        '''
        now = datetime.utcnow()
        if self.last_search and self.last_search > now - AGE_SEARCH_MIN[self.mode]:
            return False
        if self.last_download and self.last_download > now - AGE_SEARCH_MIN[self.mode]:
            return False
        if self.last_activity and self.last_activity < now - timedelta(days=22) \
                and self.last_search and self.last_search > now - timedelta(days=2):
            return False
        return True

    def check_result_dynamic(self, result):
        '''Check result dynamic attributes.
        '''
        if result.date and result.date > datetime.utcnow() - AGE_DOWNLOAD_MIN[self.mode]:
            logger.info('filtered "%s" (%s): too recent (%s)', result.title, result.net_name, result.date)
            return False
        result.seeds = getattr(result, 'seeds', None)
        if result.seeds is not None and result.seeds < NB_SEEDS_MIN[self.mode]:
            logger.info('filtered "%s" (%s): not enough seeds (%s)', result.title, result.net_name, result.seeds)
            return False
        return True

    def check_result(self, result):
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

    def save(self):
        info = {}
        now = datetime.utcnow()

        if not self.first_search:
            info['first_search'] = now
        if self.nb_errors <= 1:
            info['last_search'] = now
        if not self.nb_errors:
            info['downloads'] = self.nb_downloads
        if self.nb_results > 0 or not self.last_activity:
            info['last_activity'] = now
        if self.nb_downloads:
            info['last_download'] = now

        if info:
            MSearch().update(id=self.id, info=info)

    def add_new(self):
        '''Create a new search for episodes.
        '''
        query = MSearch().get_next_episode(self.q)
        if query and not MSearch().get(q=query):
            MSearch().add(query,
                    category=self.category,
                    mode=self.mode,
                    langs=self.langs)

def search_files():
    for search in MSearch().find({
            'mode': {'$ne': 'ever'},
            '$or': [
                {'last_search_local': {'$exists': False}},
                {'last_search_local': {'$lt': datetime.utcnow() - LOCAL_SEARCH_RECURRENCE}},
                ],
            }):
        files = File().search(search['q'], CAT_DEF[search['category']])
        if len(files) >= NB_FILES_DEF[search['category']]:
            MSearch().remove(id=search['_id'])
            logger.info('removed %s search "%s": found %s', search['category'], search['q'], files[0])
            continue
        MSearch().update(id=search['_id'], info={'last_search_local': datetime.utcnow()})

def search_web():
    for res in MSearch().find(sort=[('last_activity', pymongo.ASCENDING)],
            timeout=False):
        search = Search(res)
        if not search.check_dates():
            continue

        logger.info('processing %s search "%s"', search.category, search.q)

        re_incl_query = Title(search.q).get_search_re('word3' if search.mode != 'inc' else None)
        for result in results(search.q,
                category=search.category,
                pages_max=search.pages_max,
                re_incl=re_incl_query):
            if not result:
                search.nb_errors += 1
                continue
            if Result().find_one({'hash': result.hash}):
                continue
            if not search.check_result_dynamic(result):
                continue

            search.nb_results += 1

            if not search.check_result(result):
                Result().insert({
                        'hash': result.hash,
                        'title': result.title,
                        'net_name': result.net_name,
                        'created': datetime.utcnow(),
                        'processed': datetime.utcnow(),
                        })
                continue

            Result().insert({
                    'hash': result.hash,
                    'title': result.title,
                    'net_name': result.net_name,
                    'url_magnet': result.url_magnet,
                    'search_id': search.id,
                    'created': datetime.utcnow(),
                    'processed': False,
                    })

            search.nb_downloads += 1
            logger.info('found "%s" on %s', result.title, result.net_name)
            if search.mode != 'ever' and search.nb_downloads >= NB_DOWNLOADS_MAX:
                break

        search.save()
        if search.nb_downloads and search.mode == 'inc':
            search.add_new()

@loop(60)
@timeout(hours=2)
@timer
def main():
    search_files()
    if Google().accessible:
        search_web()

    Result().remove({'created': {'$lt': datetime.utcnow() - AGE_RESULTS_MAX}}, safe=True)


if __name__ == '__main__':
    main()
