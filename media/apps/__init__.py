import logging

from flask import Flask

logging.basicConfig(level=logging.DEBUG)

app = Flask(__name__)
# app.secret_key = '91823980212783746209418347'
# app.config.from_object('media.apps.settings')

from media.apps import api
