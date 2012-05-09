#!/usr/bin/env python
import os
import shutil
import re
import filecmp
import logging

from mediaworker import env, settings

from systools.system import loop, timeout, timer

from mediacore.model.file import File
from mediacore.util.media import get_file


PATH_VIDEO = settings.PATHS_MEDIA_NEW['video']
LANGS = ['en', 'fr']     # priority according to the list index


logger = logging.getLogger(__name__)


@loop(minutes=30)
@timeout(hours=1)
@timer
def main():
    for res in File().find({
            'type': 'video',
            'file': {'$regex': '^%s/' % re.escape(PATH_VIDEO)},
            'size': {'$gte': 100},
            }):
        if not os.path.exists(res['file']):
            continue

        video = get_file(res['file'])
        subtitles = []
        for index, lang in enumerate(LANGS):
            file_sub = video.get_subtitles(lang)
            if not file_sub:
                continue

            subtitles.append(lang)

            # Set the destination file name
            file_dst = os.path.join(video.path, '%s-%s%s' % (video.filename, index, os.path.splitext(file_sub)[1]))
            if file_dst == file_sub:
                continue
            # Check the destination file
            if os.path.exists(file_dst) and filecmp.cmp(file_sub, file_dst):
                continue
            # Copy the selected subtitles file
            try:
                shutil.copy(file_sub, file_dst)
            except Exception:
                logger.exception('exception')
                continue

            File().add(file_dst)
            logger.info('found %s subtitles for %s: %s', lang, video.filename, os.path.basename(file_sub))

        # Update file subtitles langs
        if res.get('subtitles', []) != subtitles:
            File().update(res['_id'], info={'subtitles': subtitles})


if __name__ == '__main__':
    main()
