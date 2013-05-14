from flask import Flask

app = Flask(__name__)

from media.apps import api
