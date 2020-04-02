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
import random


r = redis.Redis(host=LOCAL_REDIS_HOST, port=LOCAL_REDIS_PORT)
#delete existing keys
for key in r.scan_iter("weibo_image_spider*"):
    r.delete(key)

# get seeds from mongodb
client = MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
collection = client[DB_NAME]['statuses']
# get status ID for content truncated statuses
mydoc = collection.find(
    {"img_crawl_status": 0}
).limit(CRAWL_BATCH_SIZE)


print("Number of queued url: " + str(mydoc.count(True)))


for x in mydoc:

    if x["img_truncated"]==False:
        img_id = x["multi_img_ids"]
        if PROXY_BASEURL:
            image_server_number = random.randint(1,4)
            base_url = get_random_proxy("http://wx%d.sinaimg.cn/"%image_server_number)
        else:
            base_url = "http://wx1.sinaimg.cn"

        #img_url = x["single_img_url"].replace("https://weibo.cn",base_url)
        img_url = base_url + "/large/"+img_id
        #print("[DEBUG] url: "+img_url)
        r.lpush('weibo_image_spider:start_urls', img_url)
    else:
        if PROXY_BASEURL:
            base_url = get_random_proxy("https://weibo.cn/")
        else:
            base_url = "https://weibo.cn"
        multi_img_url = x["multi_imgs_page_url"].replace("https://weibo.cn",base_url)
        #print("[DEBUG] url: "+multi_img_url)
        r.lpush('weibo_image_spider:start_urls', multi_img_url)

print('Redis initialized')
