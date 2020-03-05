# -*- coding: utf-8 -*-
import pymongo
from pymongo.errors import DuplicateKeyError
from sina.items import ProfileItem,ProfileRawItem
from sina.settings import LOCAL_MONGO_HOST, LOCAL_MONGO_PORT, DB_NAME


class MongoDBPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
        db = client[DB_NAME]
        self.Profile = db["user_profiles"]
        self.ProfileRaw = db["user_profiles_raw"]

    def process_item(self, item, spider):
        """ 判断item的类型，并作相应的处理，再入数据库 """
        if isinstance(item, ProfileItem):
            self.insert_item(self.Profile, item)
        elif isinstance(item, ProfileRawItem):
            self.insert_item(self.ProfileRaw, item)

        return item
        
    def insert_item(self, collection, item):
        try:
            collection.insert(dict(item))
        except DuplicateKeyError:
            #print("[ERROR][Function] insert_item")
            pass
