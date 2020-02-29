#!/usr/bin/env python
# encoding: utf-8
import redis
import sys
import os

sys.path.append(os.getcwd())
from sina.settings import LOCAL_REDIS_HOST, LOCAL_REDIS_PORT,PROXY_BASEURL

r = redis.Redis(host=LOCAL_REDIS_HOST, port=LOCAL_REDIS_PORT)

for key in r.scan_iter("weibo_user_raw_spider*"):
    r.delete(key)

# get seeds from the seeds file
file_path = os.getcwd() + '/sina/seeds.txt'
start_uids = []
start_urls = []
with open(file_path, 'r') as f:
    lines = f.readlines()
    for line in lines:
        if line[0].isdigit():
            line = line.strip()
            userid = line.split('#')[0].strip()
            start_uids.append(userid)
        elif line.startswith("http"):
            start_urls.append(line)

if PROXY_BASEURL:
    base_url = PROXY_BASEURL
else:
    base_url = "https://weibo.cn"


# push urls to redis
for uid in start_uids:
    start_url = base_url+("%s/info" % uid)
    r.lpush('weibo_user_raw_spider:start_urls', start_url)

for url in start_urls:
    r.lpush('weibo_user_raw_spider:start_urls', url)

print('Redis initialized')
