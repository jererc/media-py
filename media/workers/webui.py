from systools.system import webapp

from mediaui import app

from media import settings


def run():
    webapp.run(app, host='0.0.0.0', port=settings.WEBUI_PORT)
