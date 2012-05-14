#!/usr/bin/env python
from datetime import datetime, timedelta
import logging

from mediaworker import env, settings

from systools.system import loop, timeout, timer

from mediacore.model.result import Result
from mediacore.model.search import Search
from mediacore.util.transmission import Transmission, TransmissionError, TorrentExists


PATH_FINISHED = settings.PATHS_FINISHED['transmission']
MAX_TORRENT_AGE = timedelta(days=15)


logger = logging.getLogger(__name__)


@loop(30)
@timeout(minutes=10)
# @timer
def main():
    transmission = Transmission()
    if not transmission.logged:
        return

    transmission.watch(PATH_FINISHED, max_torrent_age=MAX_TORRENT_AGE)

    for res in Result().find({
            'processed': False,
            'url_magnet': {'$ne': None},
            }):
        try:
            transmission.add(res['url_magnet'])

            Search().col.update({'_id': res['search_id']},
                    {'$addToSet': {'hashes': res['hash']}}, safe=True)

            logger.info('added torrent %s to transmission', res['title'].encode('utf-8'))
        except TorrentExists, e:
            logger.info('torrent %s (%s) already exists: %s', res['title'].encode('utf-8'), res['hash'], e)
        except TransmissionError, e:
            logger.error('failed to add torrent %s (%s): %s', res['title'].encode('utf-8'), res['hash'], e)
            continue

        Result().update({'_id': res['_id']},
                {'$set': {'processed': datetime.utcnow()}}, safe=True)


if __name__ == '__main__':
    main()
