from flask import Flask
import os

app = Flask(__name__)

from app.data import BTSHubMongoDB
db = BTSHubMongoDB(os.environ.get('DATABASE_CLIENT'), 'bts-hub')

from app import routes