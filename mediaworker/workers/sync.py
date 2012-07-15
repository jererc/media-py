#!/usr/bin/env python
import os.path
from datetime import datetime, timedelta
import re
import logging

from pymongo import DESCENDING

from mediaworker import env, settings

from mediacore.model.sync import Sync
from mediacore.model.file import File
from mediacore.util.media import get_file

from syncd import get_host

from systools.system import loop, timeout, timer


DELTA_UPDATE = timedelta(hours=6)


logger = logging.getLogger(__name__)


def get_media(path, category, genre=None, count_max=None, size_max=None):
    '''Get most recent media.
    '''
    if not count_max and not size_max:
        raise Exception('missing parameter count_max or size_max')

    base_dirs = []
    files = []
    size = 0
    spec = {
        'info.subtype': category,
        'file': {'$regex': '^%s/' % re.escape(path)},
        }
    if genre:
        if category == 'movies':
            spec['extra.imdb_genre'] = {'$regex': '|'.join(genre)}
        elif category == 'music':
            spec['extra.sputnikmusic_genre'] = {'$regex': '|'.join(genre)}

    for res in File().find(spec, sort=[('added', DESCENDING)]):
        if res['file'] in files:
            continue

        base_dir = get_file(res['file']).get_base(path_root=path)
        if not os.path.isdir(base_dir) or base_dir in base_dirs:
            continue

        for res_ in File().find({
                'file': {'$regex': '^%s/' % re.escape(base_dir)},
                }):
            files.append(res_['file'])
            size += res_['size']

        if size_max and size >= size_max:
            break
        base_dirs.append(base_dir)
        if count_max and len(base_dirs) == count_max:
            break

    return base_dirs

@loop(minutes=10)
@timeout(hours=2)
@timer()
def main():
    for sync in Sync().find({'$or': [
            {'processed': {'$exists': False}},
            {'processed': {'$lt': datetime.utcnow() - DELTA_UPDATE}},
            ]}):
        host = get_host(username=sync['username'], password=sync['password'])
        if not host:
            continue

        src = get_media(settings.PATHS_MEDIA_NEW['audio'],
                category=sync['category'],
                genre=sync.get('genre'),
                count_max=sync.get('count_max'),
                size_max=sync.get('size_max'))

        # Delete obsolete
        src_basenames = [os.path.basename(s) for s in src]
        for path_dst in host.listdir(sync['path']):
            if os.path.basename(path_dst) not in src_basenames:
                host.remove(path_dst)
                logger.info('removed obsolete %s@%s:%s', host.username, host.host, path_dst)

        for path_src in src:
            started = datetime.utcnow()
            try:
                host.sftpsync(path_src, sync['path'], download=False, delete=True)
            except Exception, e:
                logger.info('failed to sync %s with %s@%s:%s: %s', path_src, host.username, host.host, sync['path'], e)
                continue
            logger.info('synced %s with %s@%s:%s in %s', path_src, host.username, host.host, sync['path'], datetime.utcnow() - started)

        Sync().update({'_id': sync['_id']},
                {'$set': {'processed': datetime.utcnow()}}, safe=True)


if __name__ == '__main__':
    main()
