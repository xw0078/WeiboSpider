#!/usr/bin/env python
# encoding: utf-8

import redis
import sys
import os
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
sys.path.append(os.getcwd())
from settings import LOCAL_REDIS_HOST, LOCAL_REDIS_PORT,PROXY_BASEURL,LOCAL_MONGO_PORT, LOCAL_MONGO_HOST, DB_NAME,CRAWL_BATCH_SIZE
from sina.spiders.utils import get_random_proxy

r = redis.Redis(host=LOCAL_REDIS_HOST, port=LOCAL_REDIS_PORT)
#delete existing keys
for key in r.scan_iter("weibo_status_truncated_spider*"):
    r.delete(key)

# get seeds from mongodb
client = MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
collection = client[DB_NAME]['statuses']
# get status ID for content truncated statuses
mydoc = collection.find(
    {"content_crawl_status": 0}
).limit(CRAWL_BATCH_SIZE)


print("Number of queued url: " + str(mydoc.count(True)))


for x in mydoc:
    if PROXY_BASEURL:
        base_url = get_random_proxy()
    else:
        base_url = "https://weibo.cn"

    status_url = base_url+"/comment/"+x["_id"]+"?ckAll=1"

    #print("[DEBUG] url: "+status_url)
    r.lpush('weibo_status_truncated_spider:start_urls', status_url)

print('Redis initialized')
