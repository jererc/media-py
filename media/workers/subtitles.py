import os.path
from datetime import datetime, timedelta
import logging

from pymongo import ASCENDING

from systools.system import loop, timer

from filetools.media import get_file, get_size
from filetools.title import clean

from mediacore.model.media import Media
from mediacore.model.subtitles import Subtitles
from mediacore.model.worker import Worker
from mediacore.web.google import Google
from mediacore.web.opensubtitles import Opensubtitles, DownloadQuotaReached
from mediacore.web.subscene import Subscene

from media import settings, get_factory


NAME = os.path.splitext(os.path.basename(__file__))[0]
WORKERS_LIMIT = 10
TIMEOUT_SEARCH = 1200   # seconds
DELTA_SEARCH = timedelta(hours=12)
PATH_ROOT = settings.PATHS_MEDIA_NEW['video'].rstrip('/') + '/'
VIDEO_SIZE_MIN = 100    # MB
DELTA_OPENSUBTITLES_QUOTA = timedelta(hours=12)
LANGS_DEF = {
    'opensubtitles': {
        'en': 'eng',
        'fr': 'fre',
        },
    'subscene': {
        'en': 'english',
        'fr': 'french',
        },
    }

logger = logging.getLogger(__name__)


def get_plugins():
    return {
        'subscene': Subscene(),
        'opensubtitles': Opensubtitles(settings.OPENSUBTITLES_USERNAME,
                    settings.OPENSUBTITLES_PASSWORD),
        }

def validate_file(file):
    if not file.startswith(PATH_ROOT):
        return
    if not os.path.exists(file):
        return
    if get_size(file) / 1024 < VIDEO_SIZE_MIN:
        return
    return True

@timer(300)
def search_subtitles(media_id):
    media = Media.get(media_id)
    if not media:
        return

    info = media['info']
    subtitles_langs = []
    success = True
    plugins = get_plugins()

    for file in media['files']:
        if not validate_file(file):
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

        file_ = get_file(file)
        dst = file_.get_subtitles_path()

        for lang in settings.SUBTITLES_SEARCH_LANGS:
            for obj_name, obj in plugins.items():
                if not obj.accessible:
                    success = False
                    continue
                if obj_name == 'opensubtitles' and not validate_quota():
                    success = False
                    continue
                lang_ = LANGS_DEF[obj_name].get(lang)
                if not lang_:
                    continue

                logger.debug('searching subtitles for "%s" (%s) on %s' % (media['name'], file, obj_name))

                for res in obj.results(name, season, episode, date, lang_):
                    doc = {
                        'url': res['url'],
                        'file': file_.file,
                        }
                    if Subtitles.find_one(doc):
                        continue
                    try:
                        files_dst = obj.download(res['url'], dst, settings.PATH_TMP)
                    except DownloadQuotaReached, e:
                        update_quota()
                        logger.info(str(e))
                        break
                    if not files_dst:
                        continue
                    for file_dst in files_dst:
                        logger.info('downloaded %s on %s' % (file_dst, obj_name))

                    doc['created'] = datetime.utcnow()
                    Subtitles.insert(doc, safe=True)

        for lang in settings.SUBTITLES_SEARCH_LANGS:
            if file_.set_subtitles(lang):
                subtitles_langs.append(lang)

    if success:
        media['subtitles_search'] = datetime.utcnow()
    media['subtitles'] = sorted(list(set(subtitles_langs)))
    Media.save(media, safe=True)

def process_media():
    count = 0

    for media in Media.find({
            'type': 'video',
            '$or': [
                {'subtitles_search': {'$exists': False}},
                {'subtitles_search': {'$lt': datetime.utcnow() - DELTA_SEARCH}},
                ],
            },
            sort=[('subtitles_search', ASCENDING)]):
        if [f for f in media['files'] if f.startswith(PATH_ROOT)]:
            target = '%s.workers.subtitles.search_subtitles' % settings.PACKAGE_NAME
            get_factory().add(target=target,
                    args=(media['_id'],), timeout=TIMEOUT_SEARCH)

            count += 1
            if count == WORKERS_LIMIT:
                return

def validate_quota():
    res = Worker.get_attr(NAME, 'opensubtitles_quota_reached')
    if not res or res < datetime.utcnow() - DELTA_OPENSUBTITLES_QUOTA:
        return True

def update_quota():
    if not Worker.get_attr(NAME, 'opensubtitles_quota_reached'):
        Worker.set_attr(NAME, 'opensubtitles_quota_reached',
                datetime.utcnow())

@loop(minutes=2)
def run():
    if Google().accessible:
        process_media()

    for res in Subtitles.find():
        if not os.path.exists(res['file']):
            Subtitles.remove({'_id': res['_id']}, safe=True)
