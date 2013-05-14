from datetime import datetime, timedelta
import logging

from pymongo import DESCENDING

from systools.system import loop, timer

from mediacore.model.settings import Settings
from mediacore.web.google import Google
from mediacore.web.info import search_extra
from mediacore.utils.filter import validate_extra

from media import settings, get_factory


WORKERS_LIMIT = 10
TIMEOUT_UPDATE = 300    # seconds
DELTA_UPDATE_DEF = [    # delta created, delta updated
    (timedelta(days=365), timedelta(days=60)),
    (timedelta(days=90), timedelta(days=30)),
    (timedelta(days=30), timedelta(days=15)),
    (timedelta(days=10), timedelta(days=7)),
    (timedelta(days=0), timedelta(days=2)),
    ]

logger = logging.getLogger(__name__)


def get_model(objtype, objmodel):
    try:
        module = __import__('mediacore.model.%s' % objtype, globals(), locals(), [objtype], -1)
        return getattr(module, objmodel)
    except (AttributeError, ImportError), e:
        logger.error('failed to import model %s: %s' % (objmodel, str(e)))

def validate_object(created, updated):
    if not updated:
        return True
    now = datetime.utcnow()
    delta_created = now - created
    delta_updated = now - updated
    for d_created, d_updated in DELTA_UPDATE_DEF:
        if delta_created > d_created and delta_updated > d_updated:
            return True

def _get_rating(extra, category):
    ratings = []

    if category in ('movies', 'tv', 'anime'):
        rating = extra.get('rottentomatoes', {}).get('rating')
        if rating:
            ratings.append(rating)
        rating = extra.get('metacritic', {}).get('rating')
        if rating:
            ratings.append(rating)
        rating = extra.get('imdb', {}).get('rating')
        if rating:
            ratings.append(rating * 100 / 10)

    elif category == 'music':
        rating = extra.get('sputnikmusic', {}).get('rating')
        if rating:
            ratings.append(rating * 100 / 5)

    if ratings:
        return int(sum(ratings) / len(ratings))

@timer(30)
def update_obj_extra(objtype, objmodel, objid):
    model = get_model(objtype, objmodel)
    if not model:
        return
    obj = model.find_one({'_id': objid})
    if not obj:
        return
    # Check dates in case the object has been updated by another worker
    if not validate_object(obj['created'], obj.get('updated')):
        return

    category = obj.get('info', {}).get('subtype') or obj.get('category')

    spec = {'_id': obj['_id']}
    doc = {'updated': datetime.utcnow()}
    extra = search_extra(obj)
    if extra:
        doc['extra'] = extra
        doc['rating'] = _get_rating(extra, category)
        if category == 'tv':
            if objtype == 'media':
                spec = {'info.name': obj['info']['name']}
            else:
                spec = {'name': obj['name']}

    media_filters = Settings.get_settings('media_filters')
    doc['valid'] = validate_extra(extra or {}, media_filters)

    model.update(spec, {'$set': doc}, multi=True, safe=True)

    name = model.get_query(obj) if objtype == 'search' else obj['name']
    logger.info('updated %s %s "%s"' % (category, objtype, name))

def update_extra(objtype, objmodel):
    count = 0

    sort = [('date', DESCENDING)] if objtype == 'release' else [('created', DESCENDING)]
    model = get_model(objtype, objmodel)
    if not model:
        return
    for obj in model.find({
            '$or': [
                {'updated': {'$exists': False}},
                {'updated': {'$lt': datetime.utcnow() - DELTA_UPDATE_DEF[-1][1]}},
                ],
            }, sort=sort):
        if not validate_object(obj['created'], obj.get('updated')):
            continue

        target = '%s.workers.extra.update_obj_extra' % settings.PACKAGE_NAME
        get_factory().add(target=target,
                args=(objtype, objmodel, obj['_id']), timeout=TIMEOUT_UPDATE)

        count += 1
        if count == WORKERS_LIMIT:
            break

@loop(minutes=2)
def run():
    if Google().accessible:
        for type, model in [
                ('media', 'Media'),
                ('release', 'Release'),
                ('search', 'Search'),
                ('similar', 'SimilarSearch'),
                ]:
            update_extra(type, model)
