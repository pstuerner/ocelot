from pymongo import MongoClient
from ocelot import config

class Scraper(object):

    mc = MongoClient(
        username = config['mongodb']['username'],
        password = config['mongodb']['password'],
        authSource = config['mongodb']['authsource']
    )
    db = mc[config['mongodb']['database']]

    def __init__(self):
        pass

