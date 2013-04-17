from datetime import datetime, timedelta
import logging

from systools.system import loop, timeout, timer

from transfer import Transfer

from mediacore.model.result import Result
from mediacore.model.settings import Settings
from mediacore.web.search.plugins.filestube import Filestube

from media import settings


DELTA_RETRY = timedelta(hours=6)
MAX_TRIES = 5

logger = logging.getLogger(__name__)


@loop(60)
@timeout(minutes=30)
@timer()
def run():
    dst = Settings.get_settings('paths')['finished_download']
    for result in Result.find({
            'processed': False,
            '$nor': [
                {'retry': {'$gte': datetime.utcnow()}},
                {'tries': {'$gte': MAX_TRIES}},
                ],
            }):
        if result.get('transfer_id'):
            transfer = Transfer.find_one({'_id': result['transfer_id']})
            if transfer and transfer.get('finished'):
                Result.update({'_id': result['_id']}, {'$set': {
                        'processed': datetime.utcnow(),
                        }}, safe=True)
            continue

        src = result['url']
        type = result['type']

        if result['type'] == 'filestube':
            src = Filestube().get_download_urls(result['url'])
            if not src:
                Result.update({'_id': result['_id']}, {
                        '$set': {'retry': datetime.utcnow() + DELTA_RETRY},
                        '$inc': {'tries': 1},
                        }, safe=True)
                continue
            type = 'http'

        transfer_id = Transfer.add(src, dst, type=type,
                temp_dir=settings.PATH_TMP)
        Result.update({'_id': result['_id']}, {'$set': {
                'transfer_id': transfer_id,
                }}, safe=True)
        logger.info('added %s transfer %s to %s' % (type, src, dst))
