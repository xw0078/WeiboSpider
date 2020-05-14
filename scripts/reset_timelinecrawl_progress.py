

import glob
from pymongo import MongoClient
import pymongo
from settings import LOCAL_MONGO_HOST,LOCAL_MONGO_PORT,DB_NAME
import os


client = MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
profiles_collection = client[DB_NAME]['user_profiles']



seeds = profiles_collection.find({})
for seed in seeds:
    profiles_collection.update_one(
        {"_id": seed["_id"]},
        {
            "$set": {
                "timelineCrawlJob_current_complete": False,
            },
        }
    )
    