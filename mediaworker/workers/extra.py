#!/usr/bin/env python
import re
from datetime import datetime, timedelta
import logging

from mediaworker import env, settings

from systools.system import loop, timeout, timer

from mediacore.model.file import File
from mediacore.web.google import Google
from mediacore.web.youtube import Youtube
from mediacore.web.imdb import Imdb
from mediacore.web.tvrage import Tvrage
from mediacore.web.sputnikmusic import Sputnikmusic
from mediacore.util.util import prefix_dict


FILE_SPEC = {'$regex': '^(%s)/' % '|'.join([re.escape(p) for p in settings.PATHS_MEDIA_NEW.values()])}
DELTA_UPDATE = timedelta(hours=24)
DELTA_NO_UPDATE = timedelta(days=7)
UPDATE_LIMIT = 20


logger = logging.getLogger(__name__)


def _update_extra(type, info):
    '''Update the matching files extra info.

    :return: True if successful
    '''
    spec = {
        'file': FILE_SPEC,
        'type': type,
        }
    extra = {}

    if type == 'video' and info.get('full_name') and info.get('name'):
        key = 'name' if info.get('subtype') == 'tv' else 'full_name'
        name = info[key]
        spec['info.%s' % key] = name

        res = Youtube().get_trailer(name, info['date'])
        if res:
            extra.update(prefix_dict(res, 'youtube_'))

        if info.get('subtype') == 'movies':
            res = Imdb().get_info(info['full_name'], info.get('date'))
            prefix = 'imdb_'
        else:
            res = Tvrage().get_info(info['name'])
            prefix = 'tvrage_'
            if not res:
                res = Imdb().get_info(info['full_name'], info.get('date'))
                prefix = 'imdb_'
        if res:
            extra.update(prefix_dict(res, prefix))

    elif type == 'audio' and info.get('artist') and info.get('album'):
        spec['info.artist'] = info['artist']
        spec['info.album'] = info['album']
        name = '%s - %s' % (info['artist'], info['album'])

        res = Youtube().get_track(info['artist'], info['album'])
        if res:
            extra.update(prefix_dict(res, 'youtube_'))

        res = Sputnikmusic().get_album_info(info['artist'], info['album'])
        if res:
            extra.update(prefix_dict(res, 'sputnikmusic_'))

    else:
        return

    File().update(spec=spec, info={'extra': extra, 'updated': datetime.utcnow()})
    logger.info('updated "%s" %s files extra info', name, type)
    return True

def update_extra():
    '''Update the files extra info.
    '''
    for i in range(UPDATE_LIMIT):   # we use find_one since a single update can update multiple files (e.g.: audio)
        file = File().find_one({
            'file': FILE_SPEC,
            'type': {'$in': ['video', 'audio']},
            '$or': [
                {'updated': {'$exists': False}},
                {'updated': {'$lt': datetime.utcnow() - DELTA_UPDATE}},
                ],
            },
            sort=[('updated', 1)])
        if not file:
            break

        if not _update_extra(file.get('type'), file.get('info')):
            # Update date for files which could not be updated
            File().update(id=file['_id'], info={'updated': datetime.utcnow() + DELTA_NO_UPDATE})

@loop(minutes=2)
@timeout(hours=1)
@timer()
def main():
    if Google().accessible:
        update_extra()


if __name__ == '__main__':
    main()
