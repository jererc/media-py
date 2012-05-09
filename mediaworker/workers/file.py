#!/usr/bin/env python
import os
from datetime import datetime, timedelta
import logging

from mediaworker import env, settings

from systools.system import loop, timeout, timer

from mediacore.model.file import File
from mediacore.model.path import Path
from mediacore.util.media import iter_files, get_file_type


PATHS_DEF = { # path: recurrence (hours)
    settings.PATH_MEDIA_ROOT: 24,
    settings.PATHS_MEDIA_NEW['audio']: 1,
    settings.PATHS_MEDIA_NEW['video']: 1,
    }
FILE_TYPES = ('audio', 'video', 'subtitles',)


logger = logging.getLogger(__name__)


@loop(minutes=10)
@timeout(hours=4)
@timer
def main():
    for path, recurrence in PATHS_DEF.items():
        res = Path().find_one({'path': path})
        if not res:
            Path().insert({'path': path}, safe=True)
        else:
            date = res.get('processed')
            if date and date + timedelta(hours=recurrence) > datetime.utcnow():
                continue

        logger.info('started to update path %s', path)

        started = datetime.utcnow()
        for file in iter_files(path):
            if get_file_type(file) in FILE_TYPES:
                File().add(file)

        duration = datetime.utcnow() - started
        Path().update({'path': path}, {'$set': {
                'processed': datetime.utcnow(),
                'duration': duration.total_seconds(),
                }}, safe=True)
        logger.info('updated %s in %s', path, duration)

    # Clean db
    Path().remove({'path': {'$nin': PATHS_DEF.keys()}}, safe=True, multi=True)
    for res in File().find():
        if not os.path.exists(res['file']):
            File().remove(res['_id'])


if __name__ == '__main__':
    main()
