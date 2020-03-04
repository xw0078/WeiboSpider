#!/usr/bin/env python
# encoding: utf-8

import redis
import sys
import os
import pymongo
from pymongo.errors import DuplicateKeyError
sys.path.append(os.getcwd())
from sina.settings import LOCAL_REDIS_HOST, LOCAL_REDIS_PORT, PROXY_BASEURL, DB_NAME
import urllib.parse
r = redis.Redis(host=LOCAL_REDIS_HOST, port=LOCAL_REDIS_PORT)

# delete existing keys
# for key in r.scan_iter("weibo_search_timeline_spider*"):
#     r.delete(key)

# get seeds from mongodb
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["2020COVID19WEIBO"]
collection = mydb["Tweets_parsed_v2"]
# get status ID for content truncated statuses
mydoc = collection.find(
    {"content_truncated": True},
    {"created_at_utc": 1}
).limit(1000)

for x in mydoc:
    status_url = PROXY_BASEURL+"/comment/"+x["_id"]+"?ckAll=1"
    r.lpush('weibo_status_truncated_spider:start_urls', status_url)

print('Redis initialized')
