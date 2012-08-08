#!/usr/bin/env python
from datetime import datetime, timedelta
import logging

from pymongo import DESCENDING

from mediaworker import env

from systools.system import loop, timeout, timer

from mediacore.model.media import Media
from mediacore.model.release import Release
from mediacore.model.search import Search
from mediacore.web.google import Google
from mediacore.web.info import search_extra


UPDATE_LIMIT = 20
DELTA_UPDATE_DEF = [    # delta created, delta updated
    (timedelta(days=365), timedelta(days=30)),
    (timedelta(days=90), timedelta(days=15)),
    (timedelta(days=30), timedelta(days=7)),
    (timedelta(days=10), timedelta(days=4)),
    (timedelta(days=2), timedelta(days=2)),
    ]


logger = logging.getLogger(__name__)


def validate_times(created, updated):
    if not updated:
        return True

    delta_created = datetime.utcnow() - created
    delta_updated = datetime.utcnow() - updated
    for d_created, d_updated in DELTA_UPDATE_DEF:
        if delta_created > d_created and delta_updated > d_updated:
            return True

@timeout(hours=1)
@timer()
def update_extra(model):
    count = 0
    for obj in model().find({
            '$or': [
                {'updated': {'$exists': False}},
                {'updated': {'$lt': datetime.utcnow() - DELTA_UPDATE_DEF[-1][1]}},
                ],
            }, sort=[('created', DESCENDING)], timeout=False):
        # Reload the document in case it has been updated after the request
        obj = model().find_one({'_id': obj['_id']})
        if not obj:
            continue
        if not validate_times(obj['created'], obj.get('updated')):
            continue
        category = obj.get('info', {}).get('subtype') or obj.get('category')

        spec = {'_id': obj['_id']}
        doc = {'updated': datetime.utcnow()}
        extra = search_extra(obj)
        if extra:
            doc['extra'] = extra
            if category == 'tv':
                if model == Media:
                    spec = {'info.name': obj['info']['name']}
                else:
                    spec = {'name': obj['name']}

        model().update(spec, {'$set': doc}, multi=True, safe=True)

        name = model().get_query(obj) if model == Search else obj['name']
        logger.info('updated %s %s "%s"', category, model.__name__.lower(), name)

        count += 1
        if count == UPDATE_LIMIT:
            break

@loop(minutes=5)
def main():
    if Google().accessible:
        for model in (Media, Release, Search):
            update_extra(model)


if __name__ == '__main__':
    main()
