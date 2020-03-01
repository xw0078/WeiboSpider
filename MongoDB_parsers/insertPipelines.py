import pymongo
from pymongo.errors import DuplicateKeyError


class MongoDBPipeline():
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