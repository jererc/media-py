#!/usr/bin/env python
import os
import logging

from mediaworker import env
from mediaworker.settings import PATHS_FINISHED, PATHS_MEDIA_NEW

from systools.system import loop, timeout, timer

from mediacore.model.file import File
from mediacore.util.download import downloads, check_download
from mediacore.util.media import remove_file, move_file


logger = logging.getLogger(__name__)


@loop(30)
@timeout(hours=4)
@timer()
def main():
    for path in PATHS_FINISHED.values():
        if not os.path.exists(path):
            continue

        for download in downloads(path):
            if not check_download(download.file):
                if remove_file(download.file):
                    logger.info('removed %s (bad download)', download.filename)
                continue

            # Move the download
            if download.type not in PATHS_MEDIA_NEW:
                download.type = None
            res = move_file(download.file, PATHS_MEDIA_NEW[download.type])
            if res:
                File().add(res)
                logger.info('moved %s to %s', download.filename, PATHS_MEDIA_NEW[download.type])


if __name__ == '__main__':
    main()
