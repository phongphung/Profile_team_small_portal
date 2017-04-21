__author__ = 'sunary'


from flask import Flask, render_template
from flask.ext.cache import Cache


flask_app = Flask(__name__)
ALLOWED_EXTENSIONS = {'csv', 'xls', 'xlsx'}
flask_app.config['UPLOAD_FOLDER'] = 'web/upload/'

cache = Cache(flask_app, config={'CACHE_TYPE': 'simple'})
cache.init_app(flask_app)


@flask_app.route('/')
@flask_app.route('/index')
@cache.cached(timeout=3600)
def index():
    return render_template('index.html')

from web.app import kpi, tools