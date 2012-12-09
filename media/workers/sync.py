import os.path
from datetime import datetime, timedelta
import logging

from pymongo import DESCENDING

from mist import get_host

from transfer import Transfer

from systools.system import loop, timer

from filetools.media import iter_files, get_size

from mediacore.model.sync import Sync
from mediacore.model.media import Media

from media import settings, get_factory


WORKERS_LIMIT = 4
TIMEOUT_SYNC = 600     # seconds
DELTA_RETRY = timedelta(minutes=30)

logger = logging.getLogger(__name__)


@timer()
def get_recent_media(category, genre=None, count_max=None, size_max=None):
    '''Get most recent media.
    '''
    dirs = []
    size = 0

    spec = {
        'info.subtype': category,
        'date': {'$exists': True},
        }
    if genre:
        val = {'$regex': r'\b%s\b' % '|'.join(genre), '$options': 'i'}
        if category == 'movies':
            spec['extra.imdb.genre'] = val
        elif category == 'music':
            spec['$or'] = [
                {'extra.sputnikmusic.genre': val},
                {'extra.lastfm.genre': val},
                ]

    for media in Media.find(spec, sort=[('date', DESCENDING)]):
        dirs_ = Media.get_bases(media['_id'], dirs_only=True)
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

def set_retry(sync):
    sync['reserved'] = datetime.utcnow() + DELTA_RETRY
    Sync.save(sync, safe=True)

@timer()
def process_sync(sync_id):
    sync = Sync.get(sync_id)
    if not sync:
        return
    if Transfer.find_one({'sync_id': sync['_id'], 'finished': None}):
        set_retry(sync)
        return
    host = get_host(user=sync['user'])
    if not host:
        set_retry(sync)
        return

    if sync.get('media_id'):
        src = Media.get_bases(sync['media_id'], dirs_only=True)
        if not src:
            logger.info('failed to find path for media %s', sync['media_id'])
        else:
            dst = 'sftp://%s:%s@%s%s:%s' % (host.username, host.password, host.host, sync['dst'], host.port)
            Transfer.add(src, dst, type='sftp', sync_id=sync['_id'])
            logger.info('added transfer %s to %s' % (src, dst))
        Sync.remove({'_id': sync['_id']}, safe=True)

    else:
        src = get_recent_media(**sync['parameters'])

        # Delete obsolete destination files
        basenames = [os.path.basename(s) for s in src]
        for dst in host.listdir(sync['dst']):
            if os.path.basename(dst) not in basenames:
                host.remove(dst)
                logger.info('removed obsolete %s@%s:%s', host.username, host.host, dst)

        dst = 'sftp://%s:%s@%s%s:%s' % (host.username, host.password, host.host, sync['dst'], host.port)
        transfer_id = Transfer.add(src, dst, type='sftp', sync_id=sync['_id'])
        logger.info('added transfer %s to %s' % (src, dst))

        sync['transfer_id'] = transfer_id
        sync['media'] = src
        sync['processed'] = datetime.utcnow()
        sync['reserved'] = datetime.utcnow() + settings.DELTA_SYNC
        Sync.save(sync, safe=True)

@loop(60)
def run():
    for sync in Sync.find({'$or': [
            {'reserved': {'$exists': False}},
            {'reserved': {'$lt': datetime.utcnow()}},
            ]},
            sort=[('media_id', DESCENDING)],
            limit=WORKERS_LIMIT):
        target = '%s.workers.sync.process_sync' % settings.PACKAGE_NAME
        get_factory().add(target=target,
                args=(sync['_id'],), timeout=TIMEOUT_SYNC)
