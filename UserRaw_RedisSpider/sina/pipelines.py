# -*- coding: utf-8 -*-
import pymongo
from pymongo.errors import DuplicateKeyError
from sina.items import InformationItem, TweetPageItem, TweetItem, RelationPageItem
from sina.settings import LOCAL_MONGO_HOST, LOCAL_MONGO_PORT, DB_NAME


class MongoDBPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
        db = client[DB_NAME]
        self.Information = db["UserInformationRaw"]
        self.TweetPage = db["UserTimelineRaw"]
        self.RelationPage = db["RelationPageRaw"]
        self.Tweet = db["SingleTweetRaw"]

    def process_item(self, item, spider):
        """ 判断item的类型，并作相应的处理，再入数据库 """
        if isinstance(item, InformationItem):
            self.insert_item(self.Information, item)
        elif isinstance(item, TweetPageItem):
            self.insert_item(self.TweetPage, item)
        elif isinstance(item, RelationPageItem):
            self.insert_item(self.RelationPage, item)
        elif isinstance(item, TweetItem):
            self.insert_item(self.Tweet, item)
        return item
        
    def insert_item(self, collection, item):
        try:
            collection.insert(dict(item))
        except DuplicateKeyError:
            #print("[ERROR][Function] insert_item")
            pass
