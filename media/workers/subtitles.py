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
PATH_ROOT = settings.PATHS_MEDIA_NEW['video'].rstrip('/') + '/'
WORKERS_LIMIT = 4
DELTA_UPDATE_DEF = [    # delta created, delta updated
    (timedelta(days=365), timedelta(days=30)),
    (timedelta(days=90), timedelta(days=15)),
    (timedelta(days=30), timedelta(days=7)),
    (timedelta(days=10), timedelta(days=2)),
    (timedelta(days=3), timedelta(hours=6)),
    ]
DELTA_OPENSUBTITLES_QUOTA = timedelta(hours=12)
TIMEOUT_SEARCH = 1200   # seconds
VIDEO_SIZE_MIN = 100    # MB
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


def validate_media(media):
    if not media['info'].get('name'):
        return
    if not media.get('updated_subs'):
        return True
    now = datetime.utcnow()
    delta_created = now - media['created']
    delta_updated = now - media['updated_subs']
    for d_created, d_updated in DELTA_UPDATE_DEF:
        if delta_created > d_created and delta_updated > d_updated:
            return True

def validate_file(file):
    if not file.startswith(PATH_ROOT):
        return
    if not os.path.exists(file):
        return
    if get_size(file) / 1024 < VIDEO_SIZE_MIN:
        return
    return True

def get_plugins():
    return {
        'subscene': Subscene(),
        'opensubtitles': Opensubtitles(settings.OPENSUBTITLES_USERNAME,
                    settings.OPENSUBTITLES_PASSWORD),
        }

@timer(300)
def search_subtitles(media_id):
    media = Media.get(media_id)
    if not media:
        return

    info = media['info']
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

    subtitles_langs = []
    plugins = get_plugins()

    stat = []
    for file in media['files']:
        if not validate_file(file):
            continue

        file_ = get_file(file)
        dst = file_.get_subtitles_path()

        processed = False
        for lang in settings.SUBTITLES_SEARCH_LANGS:
            logger.debug('searching %s subtitles for "%s" (%s)' % (lang, media['name'], file))

            for obj_name, obj in plugins.items():
                if not obj.accessible:
                    continue
                if obj_name == 'opensubtitles' and not validate_quota():
                    continue
                processed = True
                lang_ = LANGS_DEF[obj_name].get(lang)
                if not lang_:
                    continue

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

        stat.append(processed)

    if False not in stat:
        media['updated_subs'] = datetime.utcnow()
    media['subtitles'] = sorted(list(set(subtitles_langs)))
    Media.save(media, safe=True)

def process_media():
    count = 0

    for media in Media.find({
            'type': 'video',
            '$or': [
                {'updated_subs': {'$exists': False}},
                {'updated_subs': {'$lt': datetime.utcnow() - DELTA_UPDATE_DEF[-1][1]}},
                ],
            },
            sort=[('updated_subs', ASCENDING)]):
        if not [f for f in media['files'] if f.startswith(PATH_ROOT)]:
            continue
        if not validate_media(media):
            continue

        target = '%s.workers.subtitles.search_subtitles' % settings.PACKAGE_NAME
        get_factory().add(target=target,
                args=(media['_id'],), timeout=TIMEOUT_SEARCH)

        count += 1
        if count == WORKERS_LIMIT:
            return

def validate_quota():
    res = Worker.get_attr(NAME, 'opensubtitles_quota_reached')
    if not res:
        return True
    if res + DELTA_OPENSUBTITLES_QUOTA < datetime.utcnow():
        res = Worker.set_attr(NAME, 'opensubtitles_quota_reached', None)
        return True
    return False

def update_quota():
    if not Worker.get_attr(NAME, 'opensubtitles_quota_reached'):
        Worker.set_attr(NAME, 'opensubtitles_quota_reached',
                datetime.utcnow())

@loop(minutes=2)
def run():
    if settings.SUBTITLES_SEARCH_LANGS and Google().accessible:
        process_media()

    for res in Subtitles.find():
        if not os.path.exists(res['file']):
            Subtitles.remove({'_id': res['_id']}, safe=True)
