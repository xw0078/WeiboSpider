# -*- coding: utf-8 -*-
import pymongo
from pymongo.errors import DuplicateKeyError
from sina.items import RelationshipsItem, TweetsItem, InformationItem, CommentItem, ImgItem,RepostItem
from sina.settings import LOCAL_MONGO_HOST, LOCAL_MONGO_PORT, DB_NAME


class MongoDBPipeline(object):
    def __init__(self):
        client = pymongo.MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
        db = client[DB_NAME]
        self.Information = db["Information"]
        self.Tweets = db["Tweets"]
        self.Comments = db["Comments"]
        self.Relationships = db["Relationships"]
        self.Images = db["Images"]
        self.Reposts = db["Reposts"]

    


    def AddTweetsItem(self, collection,item):
        result = collection.update_one(
            {"_id": item["_id"]},
            {
                "$setOnInsert": { # insert item if id not found
                    "weibo_url" : item["weibo_url"],
                    "created_at" : item["created_at"],
                    "content" : item["content"],
                    "user_id" : item["user_id"],
                    "tool" : item["tool"],
                    "multi_imgs" : item["multi_imgs"],
                    "image_url" : item["image_url"],
                    "video_url" : item["video_url"],
                    "location_map_info" : item["location_map_info"],
                    "retweet" : item["retweet"],
                    "origin_weibo" : item["origin_weibo"]
                },
                "$push": {
                    "like_num": item["like_num"],
                    "repost_num": item["repost_num"],
                    "comment_num": item["comment_num"],
                    "crawl_time_utc": item["crawl_time_utc"]
                }
            },
            upsert=True
        )

    def AddRelationshipsItem(self, collection,item):
        result = collection.update_one(
            {"_id": item["_id"]},
            {
                "$setOnInsert": { # insert item if id not found
                    "fan_id" : item["fan_id"],
                    "followed_id" : item["followed_id"]
                },
                "$push": {"crawl_time_utc": item["crawl_time_utc"]}
            },
            upsert=True
        )


    def AddInformationItem(self, collection,item):
        result = collection.update_one(
            {"_id": item["_id"]},
            {
                "$setOnInsert": { # insert item if id not found
                    "optional_id" : item["optional_id"],
                    "nick_name" : item["nick_name"],
                    "gender" : item["gender"],
                    "province" : item["province"],
                    "city" : item["city"],
                    "brief_introduction" : item["brief_introduction"],
                    "birthday" : item["birthday"],
                    "sex_orientation" : item["sex_orientation"],
                    "vip_level" : item["vip_level"],
                    "authentication" : item["authentication"],
                    "labels" : item["labels"],
                    "sentiment" : item["sentiment"]
                },
                "$push": {
                    "tweets_num": item["tweets_num"],
                    "follows_num": item["follows_num"],
                    "fans_num": item["fans_num"],
                    "crawl_time_utc": item["crawl_time_utc"]
                }
            },
            upsert=True
        )



    def AddCommentItem(self, collection,item):
        collection.update_one(
            {"_id": item["_id"]},
            {
                "$setOnInsert": { # insert item if id not found
                    "comment_user_id" : item["comment_user_id"],
                    "content" : item["content"],
                    "weibo_url" : item["weibo_url"],
                    "created_at" : item["created_at"]
                },
                "$push": {
                    "like_num": item["like_num"],
                    "hot_comment": item["hot_comment"],
                    "crawl_time_utc": item["crawl_time_utc"]
                }
            },
            upsert=True
        )

    def AddImgItem(self, collection,item):
        collection.update_one(
            {"_id": item["_id"]},
            {
                "$setOnInsert": { # insert item if id not found
                    "imgList" : item["imgList"]
                },
                "$push": {"crawl_time_utc": item["crawl_time_utc"]}
            },
            upsert=True
        )


    def AddRepostItem(self, collection,item):
        collection.update_one(
            {"_id": item["_id"]},
            {
                "$setOnInsert": { # insert item if id not found
                    "repost_user_id" : item["repost_user_id"],
                    "content" : item["content"],
                    "weibo_url" : item["weibo_url"],
                    "created_at" : item["created_at"]
                },
                "$push": {"like_num": item["like_num"]},
                "$push": {"hot_repost": item["hot_repost"]},
                "$push": {"crawl_time_utc": item["crawl_time_utc"]}
            },
            upsert=True
        )


    def process_item(self, item, spider):
        """ 判断item的类型，并作相应的处理，再入数据库 """
        if isinstance(item, RelationshipsItem):
            self.insert_item(self.Relationships, item, "RelationshipsItem")
        elif isinstance(item, TweetsItem):
            self.insert_item(self.Tweets, item,"TweetsItem")
        elif isinstance(item, InformationItem):
            self.insert_item(self.Information, item,"InformationItem")
        elif isinstance(item, CommentItem):
            self.insert_item(self.Comments, item, "CommentItem")
        elif isinstance(item, ImgItem):
            self.insert_item(self.Images, item,"ImgItem")
        elif isinstance(item, RepostItem):
            self.insert_item(self.Reposts, item,"RepostItem")
        return item
        
    def insert_item(self, collection, item, itemClass):
        try:
            if itemClass == "TweetsItem":
                self.AddTweetsItem(collection,item)
            elif itemClass == "RelationshipsItem":
                self.AddRelationshipsItem(collection,item)
            elif itemClass == "InformationItem":
                self.AddInformationItem(collection,item)
            elif itemClass == "CommentItem":
                self.AddCommentItem(collection,item)
            elif itemClass == "ImgItem":
                self.AddImgItem(collection,item)
            elif itemClass == "RepostItem":
                self.AddRepostItem(collection,item)
        except DuplicateKeyError:
            print("[ERROR][Function] insert_item")
            pass
