#!/usr/bin/env python
import os
import re
from datetime import datetime, timedelta
import logging

from mediaworker import env, settings

from systools.system import loop, timeout, timer

from mediacore.model.path import Path
from mediacore.model.media import Media
from mediacore.util.media import iter_files


PATHS_DEF = { # path: recurrence (hours)
    settings.PATH_MEDIA_ROOT: 24,
    settings.PATHS_MEDIA_NEW['video']: 1,
    settings.PATHS_MEDIA_NEW['audio']: 1,
    }
RE_FILE_EXCL = re.compile(r'^(%s)/' % '|'.join([re.escape(p) for p in settings.PATHS_EXCLUDE]))


logger = logging.getLogger(__name__)


@timeout(hours=6)
@timer()
def update_path(path):
    logger.info('started to update path %s', path)

    Path().update({'path': path},
            {'$set': {'started': datetime.utcnow()}}, safe=True)

    for file in iter_files(path):
        if not RE_FILE_EXCL.search(file):
            Media().add(file)

    Path().update({'path': path},
            {'$set': {'processed': datetime.utcnow()}}, safe=True)

def update():
    for path, recurrence in PATHS_DEF.items():
        res = Path().find_one({'path': path})
        if not res:
            Path().insert({'path': path}, safe=True)
        else:
            date = res.get('processed')
            if date and date + timedelta(hours=recurrence) > datetime.utcnow():
                continue

        update_path(path)

@timer()
def clean():
    Path().remove({'path': {'$nin': PATHS_DEF.keys()}}, safe=True)

    for media in Media().find():
        files_orig = media['files'][:]
        for file in files_orig:
            if not os.path.exists(file) or RE_FILE_EXCL.search(file):
                media['files'].remove(file)

        if not media['files']:
            Media().remove({'_id': media['_id']}, safe=True)
        elif media['files'] != files_orig:
            Media().save(media, safe=True)

@loop(minutes=10)
def main():
    update()
    clean()


if __name__ == '__main__':
    main()
