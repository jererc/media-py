import os
import re
import time
from datetime import datetime, timedelta
import logging

from mediaworker import settings, get_factory

from systools.system import loop, timer

from mediacore.model.media import Media
from mediacore.model.worker import Worker
from mediacore.util.media import iter_files


NAME = os.path.splitext(os.path.basename(__file__))[0]
TIMEOUT_UPDATE = 3600 * 6   # seconds
DELTA_UPDATE = timedelta(hours=12)
RE_FILE_EXCL = re.compile(r'^(%s)/' % '|'.join([re.escape(p) for p in settings.PATHS_EXCLUDE]))

logger = logging.getLogger(__name__)


@timer()
def update_path(path):
    res = Worker().get_attr(NAME, 'updated')
    if res and res > datetime.utcnow() - DELTA_UPDATE:
        return

    for file in iter_files(path):
        if not RE_FILE_EXCL.search(file):
            Media().add(file)
        time.sleep(.05)

    for media in Media().find():
        files_orig = media['files'][:]
        for file in files_orig:
            if not os.path.exists(file) or RE_FILE_EXCL.search(file):
                media['files'].remove(file)

        if not media['files']:
            Media().remove({'_id': media['_id']}, safe=True)
        elif media['files'] != files_orig:
            Media().save(media, safe=True)

    Worker().set_attr(NAME, 'updated', datetime.utcnow())

def get_mtime(files):
    dates = []
    for file in files:
        try:
            date = os.stat(file).st_mtime
        except OSError:
            continue
        date = datetime.utcfromtimestamp(date)
        dates.append(date)
    if dates:
        return sorted(dates)[0]

@timer()
def update_media():
    for res in Media().find():
        mtime = get_mtime(res['files'])
        if mtime:
            Media().update({'_id': res['_id']},
                    {'$set': {'date': mtime}}, safe=True)

@loop(minutes=15)
def run():
    target = '%s.workers.media.update_path' % settings.PACKAGE_NAME
    get_factory().add(target=target,
            args=(settings.PATH_MEDIA_ROOT,), timeout=TIMEOUT_UPDATE)

    target = '%s.workers.media.update_media' % settings.PACKAGE_NAME
    get_factory().add(target=target, timeout=TIMEOUT_UPDATE)
