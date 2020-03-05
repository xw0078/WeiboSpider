#!/usr/bin/env python
# encoding: utf-8
import redis
import sys
import os

sys.path.append(os.getcwd())
from sina.settings import LOCAL_REDIS_HOST, LOCAL_REDIS_PORT,PROXY_BASEURL,LOCAL_MONGO_PORT, LOCAL_MONGO_HOST, DB_NAME
from pymongo import MongoClient


client = MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
profiles_collection = client[DB_NAME]['user_profiles']
seeds = profiles_collection.find(
    {"timelineCrawlJob_current_complete": True}
)

for seed in seeds:
    profiles_collection.update_one(
        {"_id": seed["_id"]},
            {
                "$set": {
                    "timelineCrawlJob_current_page": 1,
                    "timelineCrawlJob_current_complete": False
            }
        }
    )
    
print("All complete reset to false")