#!/usr/bin/env python
# encoding: utf-8
import redis
import sys
import os

sys.path.append(os.getcwd())
from sina.settings import LOCAL_REDIS_HOST, LOCAL_REDIS_PORT,PROXY_BASEURL,LOCAL_MONGO_PORT, LOCAL_MONGO_HOST, DB_NAME
from pymongo import MongoClient



# init redis 
r = redis.Redis(host=LOCAL_REDIS_HOST, port=LOCAL_REDIS_PORT)
for key in r.scan_iter("weibo_user_timeline_spider*"):
    r.delete(key)

# get seeds from the seeds file
# file_path = os.getcwd() + '/sina/seeds.txt'
# start_uids = []
# start_urls = []
# with open(file_path, 'r') as f:
#     lines = f.readlines()
#     for line in lines:
#         if line[0].isdigit():
#             line = line.strip()
#             userid = line.split('#')[0].strip()
#             start_uids.append(userid)
#         elif line.startswith("http"):
#             start_urls.append(line)

if PROXY_BASEURL:
    base_url = PROXY_BASEURL
else:
    base_url = "https://weibo.cn"

# resume from seeds status

client = MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
profiles_collection = client[DB_NAME]['user_profiles']
seeds = profiles_collection.find(
    {"timelineCrawlJob_current_complete": False}
)

for seed in seeds:
    start_url = base_url + '/{}?page=1'.format(seed['_id'])
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
