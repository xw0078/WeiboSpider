'''
Validate truncated tweet crawls and update status in mongo db
'''



import glob
from pymongo import MongoClient
import pymongo
from settings import LOCAL_MONGO_HOST,LOCAL_MONGO_PORT,DB_NAME
import os

print("[INFO] DB: "+DB_NAME)

client = MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
status_collection = client[DB_NAME]['statuses']
status_raw_collection = client[DB_NAME]['statuses_raw']


truncated_status = status_collection.find(
    {"content_crawl_status": 0,"content_truncated": True}
)

print("Current Uncrawlwed Truncated Status:",truncated_status.count())

for x in truncated_status:
    status_id = x["_id"]
    crawled_raw_status = status_raw_collection.find(
        {"_id": status_id}
    )
    if crawled_raw_status.count() > 0:
        status_collection.update_one(
            {"_id": x["_id"]},
            {
                "$set": {
                    "content_crawl_status": 1,
                },
            }
        )

truncated_status = status_collection.find(
    {"content_crawl_status": 0,"content_truncated": True}
)
print("Updated Uncrawlwed Truncated Status:",truncated_status.count())