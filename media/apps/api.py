import os.path
from datetime import datetime, timedelta
from urlparse import urlparse, parse_qs
import logging

from flask import jsonify, request

from bson.objectid import ObjectId
from pymongo import ASCENDING, DESCENDING

from systools.system.webapp import crossdomain, serialize

from mist import get_users, get_user

from transfer import Transfer

from filetools.media import remove_file
from filetools.title import clean

from mediacore.model.media import Media
from mediacore.model.release import Release
from mediacore.model.search import Search
from mediacore.model.result import Result
from mediacore.model.sync import Sync
from mediacore.model.similar import SimilarSearch
from mediacore.model.settings import Settings

from media import settings, get_factory
from media.apps import app


EXTRA_FIELDS = ('date', 'rating', 'classification', 'genre', 'country',
    'network', 'next_episode', 'director', 'stars', 'airs',
    'runtime', 'title', 'url')
SEARCH_FIELDS = ('name', 'files', 'extra.imdb.director', 'extra.imdb.stars',
    'extra.imdb.genre', 'extra.tvrage.genre', 'extra.lastfm.genre',
    'extra.sputnikmusic.genre', 'extra.tvrage.classification')

logger = logging.getLogger(__name__)


class SyncError(Exception): pass


@app.route('/status', methods=['GET'])
@crossdomain(origin='*')
def check_status():
    return jsonify(result='media')

#
# Media
#
def _get_object_search(id, type):
    obj = None
    search = None

    if type == 'media':
        obj = Media.get(id)
        if obj:
            search = Media.get_search(obj)

    elif type == 'release':
        obj = Release.get(id)
        if obj:
            search = Release.get_search(obj)

    elif type == 'search':
        obj = Search.get(id)
        if obj:
            search = {
                'name': obj['name'],
                'category': obj['category'],
                'search_id': id,
                }
            if obj.get('album'):
                search['album'] = obj['album']

    if obj and search:
        search['extra'] = obj.get('extra', {})
        return search

@app.route('/media/create/media', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def create_media():
    data = request.json
    type = data.get('type')
    if not type:
        return jsonify(error='missing type')
    langs = data.get('langs') or []

    if 'id' in data:
        if not data.get('mode'):
            return jsonify(error='missing mode')
        id = ObjectId(data['id'])
        search = _get_object_search(id, type)
        if not search:
            return jsonify(error='%s %s does not exist' % (type, id))
        search['langs'] = langs
        search['mode'] = data['mode']
        search['safe'] = False
        if not Search.add(**search):
            return jsonify(error='failed to create search %s' % search)
        return jsonify(result=True)

    name = data.get('name')
    if not name:
        return jsonify(error='missing name')

    if type == 'url':
        dst = Settings.get_settings('paths')['finished_download']
        try:
            Transfer.add(name, dst)
        except Exception, e:
            return jsonify(error='failed to create transfer: %s' % str(e))

    elif type == 'movies_artist':
        get_factory().add(target='mediacore.model.search.add_movies',
                args=(clean(name, 1), langs))

    elif type == 'music_artist':
        get_factory().add(target='mediacore.model.search.add_music',
                args=(clean(name, 1),))

    else:
        if not data.get('mode'):
            return jsonify(error='missing mode')
        search = {
            'name': clean(name, 1),
            'category': type,
            'mode': data['mode'],
            'langs': langs,
            'safe': False,
            }
        if type == 'music':
            search['album'] = data.get('album')
            if not search['album']:
                return jsonify(error='missing album')
        if type in ('tv', 'anime'):
            for attr in ('season', 'episode'):
                val = data.get(attr)
                search[attr] = int(val) if val else None
        if not Search.add(**search):
            return jsonify(error='failed to create search %s' % search)

    return jsonify(result=True)

@app.route('/media/create/similar', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def create_similar():
    data = request.json
    if not data.get('recurrence'):
        return jsonify(error='missing recurrence')

    if 'id' in data:
        id = ObjectId(data['id'])
        type = data.get('type')
        search = _get_object_search(id, type)
        if not search:
            return jsonify(error='%s %s does not exist' % (type, id))
        similar = {
            'name': search['name'],
            'category': search['category'],
            }
    else:
        if not data.get('name'):
            return jsonify(error='missing name')
        if not data.get('category'):
            return jsonify(error='missing category')
        similar = {
            'name': clean(data['name'], 1),
            'category': data['category'],
            }

    similar['recurrence'] = int(data['recurrence'])
    similar['langs'] = data.get('langs') or []
    if not SimilarSearch.add(**similar):
        return jsonify(error='failed to create similar %s' % similar)

    return jsonify(result=True)

def _get_search_spec(query):
    query_ = {
        '$regex': r'(^|[\W_]+)%s([\W_]+|$)' % '[\W_]+'.join(clean(query, 1).split()),
        '$options': 'i',
        }
    return {'$or': [{f: query_} for f in SEARCH_FIELDS]}

def _get_search_title(search):
    res = search['name']
    if search.get('episode'):
        suffix = '%02d' % int(search['episode'])
        if search.get('season'):
            suffix = '%sx%s' % (int(search['season']), suffix)
        res = '%s %s' % (res, suffix)
    elif search.get('album'):
        res = '%s - %s' % (res, search['album'])
    return res

def _has_search(cache, obj):
    if 'searches' not in cache:
        cache['searches'] = list(Search.find())
    for res in cache['searches']:
        if res['name'] == obj['name'] \
                and res['category'] == obj['category'] \
                and res.get('season') == obj.get('season') \
                and res.get('episode') == obj.get('episode') \
                and res.get('album') == obj.get('album'):
            return True
    return False

def _has_similar(cache, obj):
    if 'similar' not in cache:
        cache['similar'] = list(SimilarSearch.find())
    for res in cache['similar']:
        if res['name'] == obj['name'] \
                and res['category'] == obj['category']:
            return True
    return False

def _get_extra(extra):
    res = {}
    for section, info in extra.items():
        if not info:
            continue
        res.setdefault(section, {})
        for key in EXTRA_FIELDS:
            if key in info:
                res[section][key] = info[key]
    return res

def _get_thumbnail_url(extra, category):
    if category == 'music':
        for section in ('sputnikmusic', 'lastfm'):
            url = extra.get(section, {}).get('url_thumbnail')
            if url:
                return url

    elif category in ('movies', 'tv', 'anime'):
        for section in ('rottentomatoes', 'tvrage'):
            url = extra.get(section, {}).get('url_thumbnail')
            if url:
                return url

    return extra.get('youtube', {}).get('urls_thumbnails', [None])[0]

def _get_video_id(extra):
    url = extra.get('youtube', {}).get('url_watch')
    if url:
        qs = parse_qs(urlparse(url).query)
        return qs['v'][0]

def _get_object(obj, type, **kwargs):
    if type in ('search', 'similar'):
        category = obj.get('category')
        date = obj['created']
    else:
        category = obj['info']['subtype']
        date = obj['date']

    extra = obj.get('extra', {})
    res = {
        'id': obj['_id'],
        'type': type,
        'category': category,
        'source': obj.get('source'),
        'date': date,
        'extra': _get_extra(extra),
        'rating': obj.get('rating'),
        'url_thumbnail': _get_thumbnail_url(extra, category),
        'video_id': _get_video_id(extra),
        'has_search': kwargs.get('has_search', False),
        'has_similar': kwargs.get('has_similar', False),
        }
    if type in ('search', 'similar'):
        res['name'] = _get_search_title(obj)
        res['obj'] = obj
    else:
        if type == 'media':
            paths = [os.path.dirname(f) for f in obj.get('files', [])]
            res['paths'] = sorted(list(set(paths)))
            res['urls'] = obj.get('urls', [])
        elif type == 'release':
            res['release'] = obj.get('release')

        if category == 'music':
            if type == 'release':
                res['name'] = '%s - %s' % (obj['artist'], obj['album'])
            else:
                res['name'] = obj['name']
        else:
            res['name'] = obj['name']
            if type == 'media':
                res['subtitles'] = obj.get('subtitles', [])

    return res

@app.route('/media/list/<type>/<int:skip>/<int:limit>',
        methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def list_media(type, skip, limit):
    cache = {}
    spec = {}

    category = request.args.get('category')
    if category:
        if type in ('search', 'similar'):
            spec['category'] = category
        else:
            spec['info.subtype'] = category
    query = request.args.get('query')
    if query:
        spec.update(_get_search_spec(query))

    sort = request.args.get('sort', 'date')
    if sort == 'name':
        sort = [('name', ASCENDING)]
    elif sort == 'rating':
        sort = [('rating', DESCENDING)]
    else:
        sort = [('date', DESCENDING), ('created', DESCENDING)]

    params = {'sort': sort, 'skip': skip, 'limit': limit}
    items = []

    if type == 'media':
        for res in Media.find(spec, **params):
            search = Media.get_search(res)
            items.append(_get_object(res, type=type,
                    has_search=_has_search(cache, search),
                    has_similar=_has_similar(cache, search)))

    elif type == 'release':
        for res in Release.find(spec, **params):
            search = Release.get_search(res)
            items.append(_get_object(res, type=type,
                    has_search=_has_search(cache, search),
                    has_similar=_has_similar(cache, search)))

    elif type == 'search':
        for res in Search.find(spec, **params):
            items.append(_get_object(res, type=type,
                    has_search=True,
                    has_similar=_has_similar(cache, res)))

    elif type == 'similar':
        for res in SimilarSearch.find(spec, **params):
            items.append(_get_object(res, type=type,
                    has_similar=True))

    return serialize({'result': items})

@app.route('/media/search/results', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def get_search_results():
    data = request.json
    if not data.get('id'):
        return jsonify(error='missing id')
    id = ObjectId(data['id'])

    items = {}
    for res in Result.find({'search_id': id}, sort=[('date', DESCENDING)]):
        urls = res['url']
        if not isinstance(urls, (tuple, list)):
            urls = [urls]
        for i, url in enumerate(urls):
            seeds = res.get('seeds')
            if seeds == 0:
                continue
            items.setdefault(res['type'], [])
            item = {'url': url, 'size': res['size'], 'seeds': seeds}
            if len(urls) > 1:
                item['title'] = '%s (%s/%s)' % (res['title'], i + 1, len(urls))
            else:
                item['title'] = res['title']
            items[res['type']].append(item)

    return serialize({'result': items})

@app.route('/media/update/search', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def update_search():
    data = request.json
    if not data.get('_id'):
        return jsonify(error='missing id')
    id = ObjectId(data['_id'])
    if not data.get('name'):
        return jsonify(error='missing name')
    if not data.get('category'):
        return jsonify(error='missing category')
    if not data.get('mode'):
        return jsonify(error='missing mode')

    info = {
        'name': data['name'],
        'category': data['category'],
        'langs': data.get('langs') or [],
        'mode': data['mode'],
        'safe': data.get('safe', True),
        'session': {},
        }
    if data['category'] == 'music':
        info['album'] = data.get('album')
        if not info['album']:
            return jsonify(error='missing album')
    for attr in ('season', 'episode'):
        val = data.get(attr)
        info[attr] = int(val) if val else None
    Search.update({'_id': id}, {'$set': info}, safe=True)

    return jsonify(result=True)

@app.route('/media/update/search/safe', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def update_search_safe():
    data = request.json
    if not data.get('id'):
        return jsonify(error='missing id')
    id = ObjectId(data['id'])
    safe = data.get('safe')
    if not isinstance(safe, bool):
        return jsonify(error='invalid safe value')
    Search.update({'_id': id}, {'$set': {'safe': safe, 'session': {}}},
            safe=True)
    return jsonify(result=True)

@app.route('/media/update/similar', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def update_similar():
    data = request.json
    if not data.get('_id'):
        return jsonify(error='missing id')
    id = ObjectId(data['_id'])
    if not data.get('name'):
        return jsonify(error='missing name')
    if not data.get('category'):
        return jsonify(error='missing category')
    if not data.get('recurrence'):
        return jsonify(error='missing recurrence')

    info = {
        'name': data['name'],
        'category': data['category'],
        'langs': data.get('langs') or [],
        'recurrence': int(data['recurrence']),
        }
    SimilarSearch.update({'_id': id}, {'$set': info}, safe=True)

    return jsonify(result=True)

@app.route('/media/reset/search', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def reset_search():
    data = request.json
    if not data.get('id'):
        return jsonify(error='missing id')
    Search.update({'_id': ObjectId(data['id'])},
            {'$set': {'session': {}}}, safe=True)
    return jsonify(result=True)

@app.route('/media/share', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def share_media():
    data = request.json
    if not data.get('id'):
        return jsonify(error='missing id')
    id = ObjectId(data['id'])
    media = Media.get(id)
    if not media:
        return jsonify(error='media %s not found' % id)
    user = data.get('user')
    if not user:
        return jsonify(error='user %s not found' % user)
    parameters = {
        'id': id,
        'path': data.get('path'),
        }
    if not Sync.add(user=ObjectId(user),
            category=media['info']['subtype'],
            parameters=parameters):
        return jsonify(error='failed to create sync')

    return jsonify(result=True)

@app.route('/media/remove', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def remove_media():
    data = request.json
    ids = data.get('ids')
    if not ids:
        return jsonify(error='missing ids')
    if not isinstance(ids, (tuple, list)):
        ids = [ids]
    spec = {'_id': {'$in': [ObjectId(i) for i in ids]}}
    type = data.get('type')

    if type == 'media':
        for id in ids:
            map(remove_file, Media.get_bases(id))
        Media.remove(spec)
    elif type == 'search':
        Search.remove(spec)
    elif type == 'similar':
        SimilarSearch.remove(spec)
    else:
        return jsonify(error='unknown type %s' % type)

    return jsonify(result=True)


#
# Sync
#
def _get_sync(data):
    if not data.get('user'):
        raise SyncError('missing user')
    if not data.get('dst'):
        raise SyncError('missing dst')
    if not data.get('category'):
        raise SyncError('missing category')
    params = data.get('parameters', {})
    count_max = params.get('count_max')
    size_max = params.get('size_max')
    if not count_max and not size_max:
        raise SyncError('missing count and size limits')

    return {
        'user': ObjectId(data['user']),
        'category': data.get('category'),
        'dst': data['dst'],
        'parameters': {
            'genre_incl': params.get('genre_incl') or [],
            'genre_excl': params.get('genre_excl') or [],
            'count_max': int(count_max) if count_max else None,
            'size_max': int(size_max) if size_max else None,
            },
        }

@app.route('/sync/create', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def create_sync():
    try:
        sync = _get_sync(request.json)
    except SyncError, e:
        return jsonify(error=str(e))
    if not Sync.add(**sync):
        return jsonify(error='failed to create sync')
    return jsonify(result=True)

def _get_user(user):
    return {
        'id': user['_id'],
        'name': user['name'],
        'paths': user.get('paths', {}),
        }

@app.route('/sync/list', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def list_syncs():
    now = datetime.utcnow()
    sync_recurrence = timedelta(minutes=Settings.get_settings('sync')['recurrence'])
    items = []
    for res in Sync.find():
        date_ = res.get('processed')
        if date_ and date_ + sync_recurrence > now:
            res['status'] = 'ok'
        else:
            res['status'] = 'pending'

        media_id = res['parameters'].get('id')
        if not media_id:
            src = res['category']
        else:
            media = Media.get(media_id)
            src = media['name'] if media else media_id

        user = get_user(res['user'])
        dst = user['name'] if user else res['user']
        res['name'] = '%s to %s' % (src, dst)
        items.append(res)

    return serialize({'result': items})

@app.route('/user/list', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def list_users():
    users = [_get_user(u) for u in get_users()]
    return serialize({'result': users})

@app.route('/sync/update', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def update_sync():
    data = request.json
    if not data.get('_id'):
        return jsonify(error='missing id')
    try:
        sync = _get_sync(data)
    except SyncError, e:
        return jsonify(error=str(e))
    Sync.update({'_id': ObjectId(data['_id'])},
            {'$set': sync}, safe=True)
    return jsonify(result=True)

@app.route('/sync/reset', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def reset_sync():
    data = request.json
    if not data.get('id'):
        return jsonify(error='missing id')
    Sync.update({'_id': ObjectId(data['id'])},
            {'$set': {'reserved': None}}, safe=True)
    return jsonify(result=True)

@app.route('/sync/remove', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def remove_sync():
    data = request.json
    if not data.get('id'):
        return jsonify(error='missing id')
    Sync.remove({'_id': ObjectId(data['id'])})
    return jsonify(result=True)


#
# Settings
#
def _set_default_settings(settings):
    for key, val in settings['media_filters'].items():
        for key2, val2 in val.items():
            if key2 in ('genre', 'classification'):
                for key3 in ('include', 'exclude'):
                    if key3 not in val2:
                        val2[key3] = []
            elif key2 in ('rating'):
                if 'min' not in val2:
                    val2['min'] = ''

def _sanitize_settings(settings):
    for key, val in settings['search_filters'].items():
        for key2, val2 in val.items():
            if key2 in ('size_min', 'size_max') \
                    and not isinstance(val2, (int, float)):
                del val[key2]

@app.route('/settings/list', methods=['GET', 'OPTIONS'])
@crossdomain(origin='*')
def list_settings():
    settings = {}
    for section in ('media_filters', 'search_filters', 'media_langs',
            'subtitles_langs', 'sync', 'paths', 'opensubtitles',
            'netflix', 'filestube'):
        settings[section] = Settings.get_settings(section)
    _set_default_settings(settings)
    return serialize({'result': settings})

@app.route('/settings/update', methods=['POST', 'OPTIONS'])
@crossdomain(origin='*')
def update_settings():
    data = request.json
    _sanitize_settings(data)
    for section, settings in data.items():
        Settings.set_settings(section, settings, overwrite=True)
    return jsonify(result=True)
