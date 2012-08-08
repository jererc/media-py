#!/usr/bin/env python
import os.path
from datetime import datetime, timedelta
import logging

from pymongo import DESCENDING

from mediaworker import env

from systools.system import loop, timeout, timer, dotdict

from mediacore.model.sync import Sync as MSync
from mediacore.model.media import Media
from mediacore.util.media import iter_files, get_size

from syncd import get_host


DELTA_UPDATE = timedelta(hours=6)


logger = logging.getLogger(__name__)


class Sync(dotdict):
    def __init__(self, doc):
        super(Sync, self).__init__(doc)
        host = get_host(username=self.username, password=self.password)
        if host:
            if not self.src:
                if not self.get('started'):
                    self.started = datetime.utcnow()
                    self.save()
                self.process_recurrent(host)

            elif self.process(host, self.src, self.dst):
                MSync().remove({'_id': self._id}, safe=True)

    @timer()
    def process(self, host, src, dst):
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

    @timeout(hours=2)
    def process_recurrent(self, host):
        self.started = datetime.utcnow()
        self.save()

        src_paths = get_media(**self.parameters)

        # Delete obsolete destination files
        basenames = [os.path.basename(s) for s in src_paths]
        for dst in host.listdir(self.dst):
            if os.path.basename(dst) not in basenames:
                host.remove(dst)
                logger.info('removed obsolete %s@%s:%s', host.username, host.host, dst)

        if self.process(host, src_paths, self.dst):
            self.media = src_paths
            self.processed = datetime.utcnow()
            self.save()

    def save(self):
        MSync().save(self, safe=True)


@timer()
def get_media(category, genre=None, count_max=None, size_max=None):
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

def process_once():
    for sync in MSync().find({'src': {'$ne': None}}):
        sync = Sync(sync)

def process_recurrent():
    for sync in MSync().find({
            'src': None,
            '$or': [
                {'processed': {'$exists': False}},
                {'processed': {'$lt': datetime.utcnow() - DELTA_UPDATE}},
                ],
            }):
        sync = Sync(sync)

@loop(30)
def main():
    process_once()
    process_recurrent()


if __name__ == '__main__':
    main()
