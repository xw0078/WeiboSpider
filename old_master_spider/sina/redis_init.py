#!/usr/bin/env python
# encoding: utf-8
import redis
import sys
import os

sys.path.append(os.getcwd())
from sina.settings import LOCAL_REDIS_HOST, LOCAL_REDIS_PORT

r = redis.Redis(host=LOCAL_REDIS_HOST, port=LOCAL_REDIS_PORT)

for key in r.scan_iter("weibo_spider*"):
    r.delete(key)

# get seeds from the seeds file
file_path = os.getcwd() + '/sina/seeds.txt'
start_uids = []
with open(file_path, 'r') as f:
    lines = f.readlines()
    for line in lines:
        line = line.strip()
        userid = line.split('#')[0].strip()
        start_uids.append(userid)

# add 
for uid in start_uids:
    r.lpush('weibo_spider:start_urls', "https://weibo.cn/%s/info" % uid)

print('Redis initialized')
