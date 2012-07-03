#!/usr/bin/env python
import os.path
from datetime import datetime, timedelta
import re
import shutil
import filecmp
import logging

from pymongo import ASCENDING

from mediaworker import env, settings

from systools.system import loop, timeout, timer

from mediacore.model.file import File
from mediacore.model.worker import Worker
from mediacore.web.google import Google
from mediacore.web.opensubtitles import Opensubtitles, OpensubtitlesError, DownloadQuotaReached
from mediacore.util.media import get_file, get_clean_filename


NAME = os.path.splitext(os.path.basename(__file__))[0]
PATH_VIDEO = settings.PATHS_MEDIA_NEW['video']
OPENSUBTITLES_LANG = 'eng'
DELTA_SEARCH = timedelta(hours=12)
LANG_DEF = {
    'eng': 'en',
    'fre': 'fr',
    }
LANGS = ['en', 'fr']     # priority according to the index
SEARCH_LIMIT = 20
DELTA_QUOTA_REACHED = timedelta(hours=12)


logger = logging.getLogger(__name__)


def _get_sub_filename(filename_video, filename_sub, lang):
    filename, ext = os.path.splitext(filename_sub)
    return '%s.%s.(%s)%s' % (filename_video, lang, get_clean_filename(filename), ext)

def _search_opensubtitles(video_file, name, season, episode, date=None):
    video = get_file(video_file)
    opensubtitles = Opensubtitles(settings.OPENSUBTITLES_USERNAME, settings.OPENSUBTITLES_PASSWORD)
    try:
        for sub in opensubtitles.results(name, season=season, episode=episode, date=date, lang=OPENSUBTITLES_LANG):
            filename_sub = _get_sub_filename(video.filename, sub['filename'], LANG_DEF[OPENSUBTITLES_LANG])
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

def _update_subtitles(video_file):
    subtitles = []
    video = get_file(video_file)

    for index, lang in enumerate(sorted(LANGS)):
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

    return subtitles

def search_subtitles():
    for file in File().find({
            'type': 'video',
            'file': {'$regex': '^%s/' % re.escape(PATH_VIDEO)},
            'size': {'$gte': 100},
            '$or': [
                {'last_sub_search': {'$exists': False}},
                {'last_sub_search': {'$lt': datetime.utcnow() - DELTA_SEARCH}},
                ],
            },
            limit=SEARCH_LIMIT,
            sort=[('last_sub_search', ASCENDING)],
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
            if _search_opensubtitles(file['file'], name, season, episode, date):
                info_ = {'last_sub_search': datetime.utcnow()}
                subtitles = _update_subtitles(file['file'])
                if subtitles != file.get('subtitles', []):
                    info_['subtitles'] = subtitles
                File().update(file['_id'], info=info_)

        except Exception:
            return

def update_quota():
    if not Worker().get_attr(NAME, 'quota_reached'):
        Worker().set_attr(NAME, 'quota_reached', datetime.utcnow())

def validate_quota():
    res = Worker().get_attr(NAME, 'quota_reached')
    if not res or res < datetime.utcnow() - DELTA_QUOTA_REACHED:
        return True

@loop(minutes=2)
@timeout(hours=2)
@timer()
def main():
    if validate_quota() and Google().accessible:
        search_subtitles()


if __name__ == '__main__':
    main()
