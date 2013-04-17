from datetime import datetime, timedelta
import logging

from pymongo import DESCENDING

from systools.system import loop, timer

from mediacore.web.google import Google
from mediacore.web.info import search_extra

from media import settings, get_factory


WORKERS_LIMIT = 10
TIMEOUT_UPDATE = 300    # seconds
DELTA_UPDATE_DEF = [    # delta created, delta updated
    (timedelta(days=365), timedelta(days=30)),
    (timedelta(days=90), timedelta(days=15)),
    (timedelta(days=30), timedelta(days=7)),
    (timedelta(days=10), timedelta(days=4)),
    (timedelta(days=2), timedelta(days=2)),
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
        if category == 'tv':
            if objtype == 'media':
                spec = {'info.name': obj['info']['name']}
            else:
                spec = {'name': obj['name']}

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
