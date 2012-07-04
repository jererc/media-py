#!/usr/bin/env python
from datetime import datetime, timedelta
import logging

from pymongo import ASCENDING

from mediaworker import env

from mediacore.model.search import Search
from mediacore.web.google import Google
from mediacore.web.imdb import Imdb
from mediacore.web.tvrage import Tvrage
from mediacore.web.sputnikmusic import Sputnikmusic
from mediacore.util.title import Title

from systools.system import loop, timeout, timer


UPDATE_LIMIT = 10
DELTA_UPDATE = timedelta(days=7)


logger = logging.getLogger(__name__)


def get_info_url(query, category):
    info = None
    if category in ('movies', 'tv', 'anime'):
        name = Title(query).name
        if category in ('tv', 'anime'):
            info = Tvrage().get_info(name)
        if not info:
            info = Imdb().get_info(name)

    elif category == 'music':
        info = Sputnikmusic().get_query_info(query)

    if info:
        return info.get('url')

def update_info():
    for search in Search().find({
            '$or': [
                {'updated': {'$exists': False}},
                {'updated': {'$lt': datetime.utcnow() - DELTA_UPDATE}},
                ],
            },
            sort=[('updated', ASCENDING)],
            limit=UPDATE_LIMIT,
            timeout=False):
        url = get_info_url(search['q'], search['category'])
        if url:
            Search().update(id=search['_id'], info={
                    'url_info': url,
                    'updated': datetime.utcnow(),
                    })
            logger.info('updated url for %s search "%s"', search['category'], search['q'])

@loop(minutes=10)
@timeout(hours=1)
@timer()
def main():
    if Google().accessible:
        update_info()

if __name__ == '__main__':
    main()
