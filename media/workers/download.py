import os.path
from datetime import datetime
import logging

from systools.system import loop, timeout, timer

from filetools.download import downloads, check_download
from filetools.media import remove_file, move_file

from mediacore.model.download import Download
from mediacore.model.notification import Notification
from mediacore.model.media import Media
from mediacore.model.settings import Settings


logger = logging.getLogger(__name__)


@loop(30)
@timeout(hours=4)
@timer()
def run():
    path = Settings.get_settings('paths')['finished_download']
    if os.path.exists(path):
        media_paths = Settings.get_settings('paths')['media']

        for download in downloads(path):
            if not check_download(download.file):
                if remove_file(download.file):
                    logger.info('removed %s (bad download)' % download.filename)
                continue

            # Move the download
            if download.type not in media_paths:
                download.type = 'misc'
            dst = media_paths[download.type]
            res = move_file(download.file, dst)
            if res:
                Media.add(res)
                Download.insert({
                        'name': download.filename,
                        'category': download.type,
                        'path': dst,
                        'created': datetime.utcnow(),
                        }, safe=True)
                Notification.add('new media "%s"' % download.filename)
                logger.info('moved %s to %s' % (download.filename, dst))
