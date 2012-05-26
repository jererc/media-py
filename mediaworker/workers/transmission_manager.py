#!/usr/bin/env python
import os.path
from datetime import datetime, timedelta
import logging

from mediaworker import env, settings

from systools.system import loop, timeout, timer

from mediacore.model.result import Result
from mediacore.model.search import Search
from mediacore.model.worker import Worker
from mediacore.util.transmission import Transmission, TransmissionError, TorrentExists


NAME = os.path.basename(__file__)
PATH_FINISHED = settings.PATHS_FINISHED['transmission']
PATH_INVALID = settings.PATH_INVALID_DOWNLOAD
AGE_TORRENT_MAX = timedelta(days=15)
AGE_CLEAN = timedelta(hours=24)


logger = logging.getLogger(__name__)


def process_download(torrent):
    # Remove search
    search = Search().find_one({'hashes': torrent['hash'], 'mode': 'once'})
    if search:
        Search().remove(id=search['_id'])
        logger.info('removed %s search "%s": download finished', search['category'], search['q'])

    return True

@loop(30)
@timeout(minutes=30)
# @timer
def main():
    transmission = Transmission()
    if not transmission.logged:
        return

    transmission.watch(PATH_FINISHED, dst_invalid=PATH_INVALID, age_max=AGE_TORRENT_MAX)

    # Add new torrents
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

    # Clean download dir
    cleaned = Worker().get_attr(NAME, 'cleaned')
    if not cleaned or cleaned < datetime.utcnow() - AGE_CLEAN:
        transmission.clean_download_directory()
        Worker().set_attr(NAME, 'cleaned', datetime.utcnow())


if __name__ == '__main__':
    main()
