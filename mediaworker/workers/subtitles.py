import os.path
from datetime import datetime, timedelta
import shutil
import filecmp
import logging

from pymongo import ASCENDING

from mediaworker import settings, get_factory

from systools.system import loop, timer

from mediacore.model.media import Media
from mediacore.model.worker import Worker
from mediacore.web.google import Google
from mediacore.web.opensubtitles import (Opensubtitles,
        OpensubtitlesError, DownloadQuotaReached)
from mediacore.util.media import get_file, get_clean_filename, get_size
from mediacore.util.title import clean


NAME = os.path.splitext(os.path.basename(__file__))[0]
WORKERS_LIMIT = 10
TIMEOUT_SEARCH = 600   # seconds
DELTA_SEARCH = timedelta(hours=12)
PATH_ROOT = settings.PATHS_MEDIA_NEW['video'].rstrip('/') + '/'
VIDEO_SIZE_MIN = 100    # MB
OPENSUBTITLES_LANGS = {
    'en': 'eng',
    'fr': 'fre',
    }
DELTA_QUOTA_REACHED = timedelta(hours=12)


logger = logging.getLogger(__name__)


def _get_sub_filename(filename_video, filename_sub, lang):
    filename, ext = os.path.splitext(filename_sub)
    return '%s.%s.(%s)%s' % (filename_video, lang, get_clean_filename(filename), ext)

def _search_opensubtitles(video_file, name, season, episode, date=None):
    video = get_file(video_file)
    opensubtitles = Opensubtitles(settings.OPENSUBTITLES_USERNAME,
            settings.OPENSUBTITLES_PASSWORD)

    for lang in settings.SUBTITLES_SEARCH_LANGS:
        lang_opensubtitles = OPENSUBTITLES_LANGS.get(lang)
        if not lang_opensubtitles:
            continue

        try:
            for sub in opensubtitles.results(name, season=season,
                    episode=episode, date=date, lang=lang_opensubtitles):
                filename_sub = _get_sub_filename(video.filename,
                        sub['filename'], lang)
                file_dst = os.path.join(video.path, filename_sub)
                if os.path.exists(file_dst):
                    continue

                try:
                    if opensubtitles.save(sub['url'], file_dst):
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

    for index, lang in enumerate(settings.SUBTITLES_SEARCH_LANGS):
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

        logger.info('found %s subtitles for %s: %s', lang, video.filename, os.path.basename(file_sub))

    return subtitles

@timer(30)
def search_subtitles(media_id):
    media = Media().get(media_id)
    if not media:
        return

    info = media['info']
    success = True
    subtitles = []

    for file in media['files']:
        if not file.startswith(PATH_ROOT):
            continue
        if not os.path.exists(file):
            continue
        if get_size(file) / 1024 < VIDEO_SIZE_MIN:
            continue

        if info['subtype'] == 'tv':
            name = clean(info.get('name'), 6)
            season = info.get('season')
            episode = info.get('episode')
            date = None
        else:
            name = info.get('full_name')
            season = None
            episode = None
            date = media.get('extra', {}).get('imdb', {}).get('date')

        logger.info('searching subtitles for "%s" (%s)', media['name'], file)
        try:
            if _search_opensubtitles(file, name, season, episode, date):
                subtitles_ = _update_subtitles(file)
                subtitles.extend(subtitles_)
            else:
                success = False

        except Exception:
            return

    if success:
        media['last_sub_search'] = datetime.utcnow()
    media['subtitles'] = sorted(list(set(subtitles)))
    Media().save(media, safe=True)

def process_media():
    count = 0

    for media in Media().find({
            'type': 'video',
            '$or': [
                {'last_sub_search': {'$exists': False}},
                {'last_sub_search': {'$lt': datetime.utcnow() - DELTA_SEARCH}},
                ],
            },
            sort=[('last_sub_search', ASCENDING)]):
        if [f for f in media['files'] if f.startswith(PATH_ROOT)]:
            target = '%s.workers.subtitles.search_subtitles' % settings.PACKAGE_NAME
            get_factory().add(target=target,
                    args=(media['_id'],), timeout=TIMEOUT_SEARCH)

            count += 1
            if count == WORKERS_LIMIT:
                return

def validate_quota():
    res = Worker().get_attr(NAME, 'quota_reached')
    if not res or res < datetime.utcnow() - DELTA_QUOTA_REACHED:
        return True

def update_quota():
    if not Worker().get_attr(NAME, 'quota_reached'):
        Worker().set_attr(NAME, 'quota_reached', datetime.utcnow())

@loop(minutes=2)
def run():
    if validate_quota() and Google().accessible:
        process_media()
