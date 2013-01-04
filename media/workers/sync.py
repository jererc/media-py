import os.path
from datetime import datetime, timedelta
import logging

from pymongo import DESCENDING

from mist import get_user, get_host

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
    user = get_user(sync['user'])
    if not user:
        Sync.remove({'_id': sync['_id']}, safe=True)
        logger.info('failed to find user %s' % sync['user'])
        return
    path_root = user.get('paths', {}).get(sync['category'])
    if not path_root:
        Sync.remove({'_id': sync['_id']}, safe=True)
        logger.info('failed to find %s path for user %s' % (sync['category'], sync['user']))
        return
    dst_path = os.path.join(path_root, sync['dst'].strip('/')).rstrip('/') + '/'

    host = get_host(user=sync['user'])
    if not host:
        set_retry(sync)
        return

    media_id = sync['parameters'].get('id')
    if media_id:
        src = Media.get_bases(media_id, dirs_only=True)
        if not src:
            logger.info('failed to find path for media %s' % media_id)
        else:
            dst = 'sftp://%s:%s@%s%s:%s' % (host.username, host.password, host.host, dst_path, host.port)
            Transfer.add(src, dst, sync_id=sync['_id'])
            logger.info('added transfer %s to %s' % (src, dst))
        Sync.remove({'_id': sync['_id']}, safe=True)

    else:
        src = get_recent_media(sync['category'], **sync['parameters'])

        src_dirs = dict([(os.path.basename(s), s) for s in src])
        # Check duplicates at user path
        for dir_ in host.listdir(path_root):
            src_dir = src_dirs.pop(os.path.basename(dir_), None)
            if src_dir:
                src.remove(src_dir)
        # Delete obsolete destination files
        for dir_ in host.listdir(dst_path):
            if os.path.basename(dir_) not in src_dirs:
                host.remove(dir_)
                logger.info('removed obsolete %s@%s:%s' % (host.username, host.host, dir_))

        dst = 'sftp://%s:%s@%s%s:%s' % (host.username, host.password, host.host, dst_path, host.port)
        transfer_id = Transfer.add(src, dst, sync_id=sync['_id'])
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
            sort=[('parameters.id', DESCENDING)],
            limit=WORKERS_LIMIT):
        target = '%s.workers.sync.process_sync' % settings.PACKAGE_NAME
        get_factory().add(target=target,
                args=(sync['_id'],), timeout=TIMEOUT_SYNC)
