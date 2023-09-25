import configparser
from pymongo import MongoClient

config=configparser.ConfigParser()
config.read('sprint2/config.ini')
mongo_uri=config.get('database','connection_uri')
db_name=config.get('database','db_name')

def verify_MongoDB(mongo_uri):
    try:
        client=MongoClient(mongo_uri)
        database=client[db_name]
    except Exception as e:
        print(e)

