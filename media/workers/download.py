import os.path
from datetime import datetime
import logging

from systools.system import loop, timeout, timer

from filetools.download import downloads, check_download
from filetools.media import remove_file, move_file

from mediacore.model.download import Download
from mediacore.model.notification import Notification
from mediacore.model.media import Media

from media.settings import PATHS_FINISHED, PATHS_MEDIA_NEW


logger = logging.getLogger(__name__)


@loop(30)
@timeout(hours=4)
@timer()
def run():
    for path in PATHS_FINISHED.values():
        if not os.path.exists(path):
            continue

        for download in downloads(path):
            if not check_download(download.file):
                if remove_file(download.file):
                    logger.info('removed %s (bad download)' % download.filename)
                continue

            # Move the download
            if download.type not in PATHS_MEDIA_NEW:
                download.type = None
            res = move_file(download.file, PATHS_MEDIA_NEW[download.type])
            if res:
                Media.add(res)
                Download.insert({
                        'name': download.filename,
                        'category': download.type,
                        'path': PATHS_MEDIA_NEW[download.type],
                        'created': datetime.utcnow(),
                        }, safe=True)
                Notification.add('new media "%s"' % download.filename)
                logger.info('moved %s to %s' % (download.filename, PATHS_MEDIA_NEW[download.type]))
