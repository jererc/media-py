#!/usr/bin/env python
from datetime import datetime, timedelta
import logging

from mediaworker import env

from systools.system import loop, timeout, timer

from mediacore.model.search import Search
from mediacore.web.google import Google
from mediacore.util.title import Title


AGE_OBSOLETE = timedelta(days=90)
AGE_TVSHOW_NEXT_SEASON = timedelta(days=60)
AGE_TVSHOW_CANCELED = timedelta(days=15)


logger = logging.getLogger(__name__)


@loop(hours=8)
@timeout(hours=1)
@timer
def main():
    if not Google().accessible:
        return

    for search in Search().find():
        first_search = search.get('first_search')
        last_activity = search.get('last_activity')

        if search.get('mode') == 'inc' and search.get('category') in ('tv', 'anime'):
            title = Title(search['q'])
            if not title.episode:
                continue

            if last_activity and last_activity < datetime.utcnow() - AGE_TVSHOW_NEXT_SEASON \
                    and title.season and int(title.episode) > 2:
                # Add search for the next season
                query = Search().get_next_season(search['q'])
                if query and not Search().get(q=query):
                    Search().add(query, search['category'], mode=search['mode'], langs=search['langs'])
                    logger.info('added search "%s"', query)

                # Remove if the next season has started
                query_next_season = Search().get_next_episode(query)
                if query_next_season and Search().get(q=query_next_season):
                    Search().remove(search['_id'])
                    logger.info('removed search "%s" (season ended)', search['q'])

        elif search.get('mode') == 'once':
            # Remove obsolete searches
            if first_search and first_search < datetime.utcnow() - AGE_OBSOLETE:
                Search().remove(search['_id'])
                logger.info('removed search "%s" (idle for more than %d days)', search['q'], AGE_OBSOLETE.days)


if __name__ == '__main__':
    main()
