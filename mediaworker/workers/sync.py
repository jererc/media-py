#!/usr/bin/env python
import os.path
from datetime import datetime, timedelta
import logging

from pymongo import DESCENDING

from mediaworker import env, settings, get_factory

from systools.system import loop, timer

from mediacore.model.sync import Sync
from mediacore.model.media import Media
from mediacore.util.media import iter_files, get_size

from syncd import get_host


WORKERS_LIMIT = 4
TIMEOUT_SYNC = 3600 * 6     # seconds
DELTA_UPDATE = timedelta(hours=6)


logger = logging.getLogger(__name__)


@timer()
def get_recent_media(category, genre=None, count_max=None, size_max=None):
    '''Get most recent media.
    '''
    dirs = []
    size = 0

    spec = {'info.subtype': category}
    if genre:
        val = {'$regex': r'\b%s\b' % '|'.join(genre), '$options': 'i'}
        if category == 'movies':
            spec['extra.imdb.genre'] = val
        elif category == 'music':
            spec['$or'] = [
                {'extra.sputnikmusic.genre': val},
                {'extra.lastfm.genre': val},
                ]

    for media in Media().find(spec, sort=[('created', DESCENDING)]):
        dirs_ = Media().get_bases(media['_id'], dirs_only=True)
        if not dirs_:
            continue

        for dir in dirs_:
            for file in iter_files(dir):
                size_ = get_size(file)
                if size_:
                    size += size_ / 1024
        if size_max and size >= size_max:
            break
        dirs.extend(dirs_)
        if count_max and len(dirs) > count_max:
            dirs = dirs[:count_max]
            break

    return dirs

@timer()
def _sync(host, src, dst):
    if not isinstance(src, (tuple, list)):
        src = [src]

    started = datetime.utcnow()
    for src_ in src:
        try:
            host.sftpsync(src_, dst, download=False, delete=True)
            logger.info('synced %s with %s@%s:%s in %s', src_, host.username, host.host, dst, datetime.utcnow() - started)
        except Exception, e:
            logger.info('failed to sync %s with %s@%s:%s: %s', src_, host.username, host.host, dst, e)
            return
    return True

@timer()
def process_sync(sync_id):
    sync = Sync().get(sync_id)
    if not sync:
        return
    # TODO: do not block the workers queue if no host
    host = get_host(username=sync['username'], password=sync['password'])
    if not host:
        return

    if sync.get('media_id'):
        src = Media().get_bases(sync['media_id'], dirs_only=True)
        if src and _sync(host, src, sync['dst']):
            Sync().remove({'_id': sync['_id']}, safe=True)

    else:
        sync['started'] = datetime.utcnow()
        Sync().save(sync, safe=True)

        src = get_recent_media(**sync['parameters'])

        # Delete obsolete destination files
        basenames = [os.path.basename(s) for s in src]
        for dst in host.listdir(sync['dst']):
            if os.path.basename(dst) not in basenames:
                host.remove(dst)
                logger.info('removed obsolete %s@%s:%s', host.username, host.host, dst)

        if _sync(host, src, sync['dst']):
            sync['media'] = src
            sync['processed'] = datetime.utcnow()
            Sync().save(sync, safe=True)

@loop(60)
def process_syncs():
    for sync in Sync().find({
            '$or': [
                {'processed': {'$exists': False}},
                {'processed': {'$lt': datetime.utcnow() - DELTA_UPDATE}},
                ],
            },
            sort=[('media_id', DESCENDING)],
            limit=WORKERS_LIMIT):
        target = '%s.workers.sync.process_sync' % settings.PACKAGE_NAME
        get_factory().add(target=target,
                args=(sync['_id'],), timeout=TIMEOUT_SYNC)

def main():
    process_syncs()


if __name__ == '__main__':
    main()
