import os.path
from datetime import datetime, timedelta
import logging

from mediaworker import settings, get_factory

from systools.system import loop, timeout, timer

from mediacore.model.result import Result

from mediacore.web.search.plugins.filestube import Filestube
from mediacore.util import http


NAME = os.path.splitext(os.path.basename(__file__))[0]
PATH_FINISHED = settings.PATHS_FINISHED['grab']
DELTA_RETRY = timedelta(hours=6)
MAX_TRIES = 5
TIMEOUT_GRAB = 6 * 3600

logger = logging.getLogger(__name__)


def get_download_urls(urls):
    return Filestube().get_download_urls(urls)

def grab(result_id, dst):
    result = Result().find_one({'_id': result_id})
    if not result:
        return

    urls = get_download_urls(result['url'])
    if not urls:
        Result().update({'_id': result_id}, {
                '$set': {'retry': datetime.utcnow() + DELTA_RETRY},
                '$inc': {'tries': 1},
                }, safe=True)
        return

    files = http.download(urls, dst, temp_dir=settings.PATH_TMP)
    if files:
        logger.info('downloaded "%s" (%s) to %s' % (result['title'], urls, files))
        Result().update({'_id': result_id},
                {'$set': {'processed': datetime.utcnow()}}, safe=True)

@loop(60)
@timeout(minutes=30)
@timer()
def run():
    for res in Result().find({
            'processed': False,
            'type': 'filestube',
            '$nor': [
                {'retry': {'$gte': datetime.utcnow()}},
                {'tries': {'$gte': MAX_TRIES}},
                ],
            }):
        target = '%s.workers.grab.grab' % settings.PACKAGE_NAME
        get_factory().add(target=target,
                args=(res['_id'], PATH_FINISHED), timeout=TIMEOUT_GRAB)
