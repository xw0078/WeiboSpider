# -*- coding: utf-8 -*-
import pymongo
from pymongo.errors import DuplicateKeyError
from sina.items import ImageItem,MultiImagePageRawItem
from sina.settings import LOCAL_MONGO_HOST, LOCAL_MONGO_PORT, DB_NAME
from scrapy.pipelines.images import ImagesPipeline

class MongoDBPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
        db = client[DB_NAME]
        
        self.MultiImageRaw = db["multi_image_page_raw"]

    def process_item(self, item, spider):
        """ 判断item的类型，并作相应的处理，再入数据库 """
        if isinstance(item, MultiImagePageRawItem):
            self.insert_item(self.MultiImageRaw, item)
            self.update_status_imgIDs(item)
        return item
        
    def insert_item(self, collection, item):
        try:
            collection.insert(dict(item))
        except DuplicateKeyError:
            print("[ERROR] DuplicateKeyError")
            pass

    def update_status_imgIDs(self, item):
        client = pymongo.MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
        db = client[DB_NAME]
        collection = db["statuses"]
        collection.find_one_and_update(
            {'multi_imgs_page_url': item["page_url"]},
            {
                '$set': {
                    'multi_img_ids': item["multi_img_ids"]
                }
            }
        )

class MyImagesPipeline(ImagesPipeline):
    
    def image_key(self, url):
        #print("TEST: "+url)
        image_guid = url.split("/")[-1]
        return 'full/%s' % (image_guid)
    
