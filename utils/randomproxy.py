import os
import pymongo
from ..settings import LOCAL_MONGO_HOST, LOCAL_MONGO_PORT, DB_NAME

class randomproxy():
    def __init__(self):
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient[DB_NAME]["proxies"]
        