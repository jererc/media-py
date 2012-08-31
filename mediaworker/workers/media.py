import os.path
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
def update(path):
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

@loop(minutes=15)
def run():
    target = '%s.workers.media.update' % settings.PACKAGE_NAME
    get_factory().add(target=target,
            args=(settings.PATH_MEDIA_ROOT,), timeout=TIMEOUT_UPDATE)
