# -*- coding: utf-8 -*-
import pymongo
from pymongo.errors import DuplicateKeyError
from sina.items import ProfileUpdateItem, TimelinePageRaw
from sina.settings import LOCAL_MONGO_HOST, LOCAL_MONGO_PORT, DB_NAME


class MongoDBPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
        db = client[DB_NAME]
        self.Profiles = db["user_profiles"]
        self.TimelinePage = db["user_timelines_raw"]

    def process_item(self, item, spider):
        """ 判断item的类型，并作相应的处理，再入数据库 """
        if isinstance(item, ProfileUpdateItem):
            self.update_profile(self.Profiles, item)
        elif isinstance(item, TimelinePageRaw):
            self.insert_item(self.TimelinePage, item)
        return item
        
    def insert_item(self, collection, item):
        try:
            collection.insert(dict(item))
        except DuplicateKeyError:
            #print("[ERROR][Function] insert_item")
            pass

    def update_profile(self,collection, item):
        if item["timelineCrawlJob_current_complete"] == False:
            collection.update_one(
                {"_id": item["uid"]},
                {
                    "$set": {
                        "timelineCrawlJob_current_page": item["timelineCrawlJob_current_page"]
                    }
                }
            )
        else:
            collection.update_one(
                {"_id": item["uid"]},
                {
                    "$set": {
                        "timelineCrawlJob_current_page": item["timelineCrawlJob_current_page"],
                        "timelineCrawlJob_current_complete": True
                    },
                    "$push": {
                        "timelineCrawlJob_run_history": item['timelineCrawlJob_run_history']
                    }
                }
            )