from systools.system import webapp

from media.apps import app

from media import settings


def run():
    webapp.run(app, host='0.0.0.0', port=settings.API_PORT)
