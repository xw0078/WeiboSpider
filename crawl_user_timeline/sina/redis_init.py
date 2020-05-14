#!/usr/bin/env python
# encoding: utf-8
import redis
import sys
import os
import logging
sys.path.append(os.getcwd())
from settings import LOCAL_REDIS_HOST, LOCAL_REDIS_PORT,PROXY_BASEURL,LOCAL_MONGO_PORT, LOCAL_MONGO_HOST, DB_NAME, PROFILE_GROUP
from pymongo import MongoClient
from sina.spiders.utils import get_random_proxy



# init redis 
r = redis.Redis(host=LOCAL_REDIS_HOST, port=LOCAL_REDIS_PORT)
for key in r.scan_iter("weibo_user_timeline_spider*"):
    r.delete(key)

client = MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
profiles_collection = client[DB_NAME]['user_profiles']

if PROFILE_GROUP > 0:
    seeds = profiles_collection.find(
        {"timelineCrawlJob_current_complete": False, "group":PROFILE_GROUP}
    )
else:
    seeds = profiles_collection.find(
        {"timelineCrawlJob_current_complete": False}
    )

print(seeds.count(),"profiles found")
for seed in seeds:
    if PROXY_BASEURL:
        base_url = get_random_proxy()
    else:
        base_url = "https://weibo.cn"
    start_url = base_url + '/{}?page={}'.format(seed['_id'],seed['timelineCrawlJob_current_page'])
    print("[DEBUG] start url: "+start_url)
    r.lpush('weibo_user_timeline_spider:start_urls', start_url)


print('Redis initialized')



# push urls to redis
# for uid in start_uids:
#     start_url = base_url+("%s/info" % uid)
#     r.lpush('weibo_user_timeline_spider:start_urls', start_url)

# for url in start_urls:
#     url = url.replace("https://weibo.cn",PROXY_BASEURL)
#     r.lpush('weibo_user_timeline_spider:start_urls', url)

# print('Redis initialized')
