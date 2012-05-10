#!/usr/bin/env python
import os.path
from datetime import datetime, timedelta
import re
import logging

from mediaworker import env, settings

from systools.system import loop, timeout, timer

from mediacore.model.file import File
from mediacore.web.google import Google
from mediacore.web.opensubtitles import Opensubtitles, OpensubtitlesError, DownloadQuotaReached
from mediacore.util.media import get_file, get_clean_filename
from mediacore.util.db import get_db


COL = 'subtitles'
PATH_VIDEO = settings.PATHS_MEDIA_NEW['video']
OPENSUBTITLES_LANG = 'eng'
AGE_SEARCH_MIN = timedelta(hours=6)
LANG_DEF = {
    'eng': 'en',
    'fre': 'fr',
    }
SEARCH_LIMIT = 50
QUOTA_REACHED_DELAY = timedelta(hours=12)


logger = logging.getLogger(__name__)


def get_sub_filename(filename_video, filename_sub, lang):
    filename, ext = os.path.splitext(filename_sub)
    return '%s.%s.(%s)%s' % (filename_video, lang, get_clean_filename(filename), ext)

def search_opensubtitles(video_file, name, season, episode, date=None):
    video = get_file(video_file)
    opensubtitles = Opensubtitles(settings.OPENSUBTITLES_USERNAME, settings.OPENSUBTITLES_PASSWORD)
    try:
        for sub in opensubtitles.results(name, season=season, episode=episode, date=date, lang=OPENSUBTITLES_LANG):
            filename_sub = get_sub_filename(video.filename, sub['filename'], LANG_DEF[OPENSUBTITLES_LANG])
            file_dst = os.path.join(video.path, filename_sub)
            if os.path.exists(file_dst):
                continue

            try:
                if opensubtitles.save(sub['url'], file_dst):
                    File().add(file_dst)
                    logger.info('saved %s', file_dst)
            except DownloadQuotaReached, e:
                update_quota()
                logger.info(e)
                raise Exception

    except OpensubtitlesError:
        return

    return True

def search_subtitles():
    for file in File().find({
            'type': 'video',
            'file': {'$regex': '^%s/' % re.escape(PATH_VIDEO)},
            'size': {'$gte': 100},
            '$or': [
                {'last_sub_search': {'$exists': False}},
                {'last_sub_search': {'$lt': datetime.utcnow() - AGE_SEARCH_MIN}},
                ],
            },
            limit=SEARCH_LIMIT,
            timeout=False):
        if not os.path.exists(file['file']):
            continue
        info = file.get('info')
        if not info:
            continue

        if info['subtype'] == 'tv':
            name = info.get('name')
            season = info.get('season')
            episode = info.get('episode')
            date = None
        else:
            name = info.get('full_name')
            season = None
            episode = None
            date = file.get('extra', {}).get('imdb_date')

        logger.info('searching subtitles for "%s" (%s)', info.get('display_name'), os.path.basename(file['file']))
        try:
            if search_opensubtitles(file['file'], name, season, episode, date):
                File().update(file['_id'], info={'last_sub_search': datetime.utcnow()})
        except Exception:
            return

def update_quota():
    if not get_db()[COL].find_one({'quota_reached': {'$exists': True}}):
        get_db()[COL].insert({'quota_reached': datetime.utcnow()}, safe=True)

@loop(minutes=10)
@timeout(hours=2)
@timer
def main():
    if not get_db()[COL].find_one({'quota_reached': {
            '$gte': datetime.utcnow() - QUOTA_REACHED_DELAY,
            }}):
        if Google().accessible:
            search_subtitles()


if __name__ == '__main__':
    main()
