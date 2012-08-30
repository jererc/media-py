#!/usr/bin/env python
import os.path
from datetime import datetime, timedelta
import logging

from mediaworker import env, settings, get_factory

from systools.system import loop, timeout, timer

from mediacore.model.result import Result
from mediacore.model.search import Search
from mediacore.model.worker import Worker
from mediacore.util.transmission import Transmission, TransmissionError, TorrentExists


NAME = os.path.splitext(os.path.basename(__file__))[0]
TIMEOUT_MANAGE = 3600   # seconds
PATH_FINISHED = settings.PATHS_FINISHED['transmission']
PATH_INVALID = settings.PATH_INVALID_DOWNLOAD
DELTA_TORRENT_ACTIVE = 24 * 4   # hours
DELTA_TORRENT_ADDED = 24 * 15   # hours
DELTA_CLEAN = timedelta(hours=24)


logger = logging.getLogger(__name__)


def get_client():
    return Transmission(host=settings.TRANSMISSION_HOST,
            port=settings.TRANSMISSION_PORT,
            username=settings.TRANSMISSION_USERNAME,
            password=settings.TRANSMISSION_PASSWORD)

def validate_clean():
    res = Worker().get_attr(NAME, 'cleaned')
    if not res or res < datetime.utcnow() - DELTA_CLEAN:
        return True

@timer(30)
def manage_torrent(id, dst, dst_invalid, delta_active, delta_added):
    client = get_client()
    if client.logged:
        client.manage(id, dst=dst, dst_invalid=dst_invalid,
                delta_active=delta_active, delta_added=delta_added)

@loop(30)
@timeout(minutes=30)
@timer()
def manage_transmission():
    client = get_client()
    if not client.logged:
        return

    # Manage torrents
    for torrent_id in client.client.list():
        target = '%s.workers.transmission.manage_torrent' % settings.PACKAGE_NAME
        kwargs = {
            'id': torrent_id,
            'dst': PATH_FINISHED,
            'dst_invalid': PATH_INVALID,
            'delta_active': DELTA_TORRENT_ACTIVE,
            'delta_added': DELTA_TORRENT_ADDED,
            }
        get_factory().add(target=target,
                kwargs=kwargs, timeout=TIMEOUT_MANAGE)

    # Add new torrents
    for res in Result().find({
            'processed': False,
            'url_magnet': {'$ne': None},
            }):
        try:
            client.add(res['url_magnet'])

            Search().update({'_id': res['search_id']},
                    {'$addToSet': {'hashes': res['hash']}}, safe=True)

            logger.info('added torrent %s to transmission', res['title'].encode('utf-8'))
        except TorrentExists, e:
            logger.info('torrent %s (%s) already exists: %s', res['title'].encode('utf-8'), res['hash'], e)
        except TransmissionError, e:
            logger.error('failed to add torrent %s (%s): %s', res['title'].encode('utf-8'), res['hash'], e)
            continue

        Result().update({'_id': res['_id']},
                {'$set': {'processed': datetime.utcnow()}},
                safe=True)

    # Clean download directory
    if validate_clean():
        client.clean_download_directory()
        Worker().set_attr(NAME, 'cleaned', datetime.utcnow())

def main():
    manage_transmission()


if __name__ == '__main__':
    main()
