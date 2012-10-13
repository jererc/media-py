import os.path
from datetime import datetime, timedelta
import logging

from mediaworker import settings, get_factory

from systools.system import loop, timeout, timer

from mediacore.model.result import Result
from mediacore.model.search import Search
from mediacore.model.worker import Worker
from mediacore.util.transmission import Transmission, TransmissionError, TorrentExists


NAME = os.path.splitext(os.path.basename(__file__))[0]
TIMEOUT_MANAGE = 3600   # seconds
DELTA_TORRENT_ACTIVE = 24 * 4   # hours
DELTA_TORRENT_ADDED = 24 * 15   # hours
DELTA_CLEAN = timedelta(hours=24)
TRANSMISSION_INFO = {
    'host': settings.TRANSMISSION_HOST,
    'port': settings.TRANSMISSION_PORT,
    'username': settings.TRANSMISSION_USERNAME,
    'password': settings.TRANSMISSION_PASSWORD
    }

logger = logging.getLogger(__name__)


class TransmissionManager(Transmission):

    def manage_torrents(self):
        for torrent_id in self.client.list():
            target = '%s.workers.transmission.manage_torrent' % settings.PACKAGE_NAME
            kwargs = {
                'id': torrent_id,
                'dst': settings.PATHS_FINISHED['transmission'],
                'dst_invalid': settings.PATH_INVALID_DOWNLOAD,
                'delta_active': DELTA_TORRENT_ACTIVE,
                'delta_added': DELTA_TORRENT_ADDED,
                }
            get_factory().add(target=target,
                    kwargs=kwargs, timeout=TIMEOUT_MANAGE)

    def add_torrents(self):
        for res in Result().find({'processed': False, 'type': 'magnet'}):
            try:
                self.add(res['url'])
                Search().update({'_id': res['search_id']},
                        {'$addToSet': {'hashes': res['hash']}}, safe=True)
                logger.info('added torrent "%s" to transmission', res['title'].encode('utf-8'))
            except TorrentExists, e:
                logger.info('torrent "%s" (%s) already exists: %s', res['title'].encode('utf-8'), res['hash'], str(e))
            except TransmissionError, e:
                logger.error('failed to add torrent "%s" (%s): %s', res['title'].encode('utf-8'), res['hash'], str(e))
                continue

            Result().update({'_id': res['_id']},
                    {'$set': {'processed': datetime.utcnow()}}, safe=True)

    def clean(self):
        res = Worker().get_attr(NAME, 'cleaned')
        if not res or res < datetime.utcnow() - DELTA_CLEAN:
            self.clean_download_directory()
            Worker().set_attr(NAME, 'cleaned', datetime.utcnow())


@timer(30)
def manage_torrent(id, dst, dst_invalid, delta_active, delta_added):
    client = Transmission(**TRANSMISSION_INFO)
    if client.logged:
        client.manage(id, dst=dst, dst_invalid=dst_invalid,
                delta_active=delta_active, delta_added=delta_added)

@loop(30)
@timeout(minutes=30)
@timer()
def run():
    transmission = TransmissionManager(**TRANSMISSION_INFO)
    if transmission.logged:
        transmission.manage_torrents()
        transmission.add_torrents()
        transmission.clean()
