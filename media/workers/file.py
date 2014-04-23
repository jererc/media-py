import os
import re
import time
from datetime import datetime, timedelta
import logging

from systools.system import loop, timer

from filetools.media import iter_files

from mediacore.model.media import Media
from mediacore.model.work import Work
from mediacore.model.settings import Settings

from media import settings, get_factory


NAME = os.path.splitext(os.path.basename(__file__))[0]
TIME_RANGE = [4, 8]
DELTA_UPDATE = timedelta(hours=12)
TIMEOUT_UPDATE = 3600 * 6   # seconds

logger = logging.getLogger(__name__)


def validate_update_path():
    if datetime.now().hour not in range(*TIME_RANGE):
        return False
    res = Work.get_info(NAME, 'updated')
    if res and res > datetime.utcnow() - DELTA_UPDATE:
        return False
    return True

@timer()
def update_path():
    paths = Settings.get_settings('paths')

    excl = paths['media_root_exclude']
    re_excl = re.compile(r'^(%s)/' % '|'.join([re.escape(p.rstrip('/')) for p in excl]))

    for file in iter_files(str(paths['media_root'])):
        if not re_excl.search(file):
            Media.add_file(file)
        time.sleep(.05)

    for media in Media.find({'files': {'$exists': True}}, timeout=False):
        files_orig = media['files'][:]
        for file in files_orig:
            if not os.path.exists(file) or re_excl.search(file):
                media['files'].remove(file)

        if not media['files'] and not media.get('urls'):
            Media.remove({'_id': media['_id']}, safe=True)
        elif media['files'] != files_orig:
            Media.save(media, safe=True)

    Work.set_info(NAME, 'updated', datetime.utcnow())

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
    for res in Media.find({'files': {'$exists': True}}, timeout=False):
        mtime = get_mtime(res['files'])
        if mtime:
            Media.update({'_id': res['_id']},
                    {'$set': {'date': mtime}}, safe=True)

@loop(minutes=15)
def run():
    if validate_update_path():
        target = '%s.workers.file.update_path' % settings.PACKAGE_NAME
        get_factory().add(target=target, timeout=TIMEOUT_UPDATE)

    target = '%s.workers.file.update_media' % settings.PACKAGE_NAME
    get_factory().add(target=target, timeout=TIMEOUT_UPDATE)
