# -*- coding: utf-8 -*-
import pymongo
from pymongo.errors import DuplicateKeyError
from sina.items import StatusPageItem
from sina.settings import LOCAL_MONGO_HOST, LOCAL_MONGO_PORT, DB_NAME


class MongoDBPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
        db = client[DB_NAME]
        self.StatusesRaw = db["statuses_raw"]

    def process_item(self, item, spider):
        """ 判断item的类型，并作相应的处理，再入数据库 """
        if isinstance(item, StatusPageItem):
            self.insert_item(self.StatusesRaw, item)
        return item
        
    def insert_item(self, collection, item):
        try:
            status_raw = dict(item)
            if status_raw["page_url"] == "https://weibo.cn/pub/":
                self.update_trucated_as_lost(status_raw)
            else:
                collection.insert(status_raw) # save the copy
                self.update_trucated_as_crawled(status_raw) # update truncated records
        except DuplicateKeyError:
            print("[ERROR] DuplicateKeyError")
            pass

    def update_trucated_as_crawled(self,status_raw):
        client = pymongo.MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
        db = client[DB_NAME]
        collection = db["statuses"]
        collection.find_one_and_update(
            {'_id': status_raw["_id"]},
            {
                '$set': {
                    'content_truncated': 2
                }
            }
        )

    def update_trucated_as_lost(self,status_raw):
        client = pymongo.MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
        db = client[DB_NAME]
        collection = db["statuses"]
        collection.find_one_and_update(
            {'_id': status_raw["_id"]},
            {
                '$set': {
                    'content_truncated': 3
                }
            }
        )





    def get_full_status_content(item):
        tree_node = etree.HTML(item["page_raw"])
        content_node = tree_node.xpath('//*[@id="M_"]/div[1]')[0]
        status_item = {}
        status_item['_id'] = item["_id"]
        # get content, replace emoji picture to text if applicated
        content_node = tweet_node.xpath('.//span[@class="ctt"]')[-1]
        raw_content = etree.tostring(content_node,encoding='unicode')
        content_node = re.sub('<img alt=\"(.*?)\".*?/>',r"\1",raw_content)
        content_node = etree.HTML(content_node)
        status_item['content'] = str(content_node.xpath('string(.)'))
        # get hashtags
        hashtags = content_node.xpath('.//span[@class="ctt"]/a[contains(text(),"#")]/text()')
        if hashtags:
            status_item['hashtags'] = list(map(lambda x: re.findall('(#.*?#)', str(x))[-1],hashtags))
        else:
            status_item['hashtags'] = ""

        # get embedded links if exist
        embeded_urls = content_node.xpath('.//span[@class="ctt"]/a[not(contains(text(),"#")) and not(contains(text(),"全文")) and not(contains(text(),"@"))]/@href')
        if embeded_urls:
            status_item['embeded_urls'] = list(map(lambda x: str(x),embeded_urls))
        else:
            status_item['embeded_urls'] = ""
        
        # get mentions
        mention_links = tweet_node.xpath('.//span[@class="ctt"]/a[(contains(text(),"@"))]/@href')
        mention_names = tweet_node.xpath('.//span[@class="ctt"]/a[(contains(text(),"@"))]/text()')
        status_item["mention_users"] = []
        if mention_links:
            mention_user_urls = list(map(lambda x: str(x),mention_links))
            mention_user_names = list(map(lambda x: str(x),mention_names))
            for url,name in zip(mention_user_urls,mention_user_names):
                mention_user_pair = {
                    'mention_user_name': name,
                    'mention_user_url': url
                }
                status_item["mention_users"].append(mention_user_pair)
        return status_item

    