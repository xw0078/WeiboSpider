#!/usr/bin/env python
# encoding: utf-8

import redis
import sys
import os
sys.path.append(os.getcwd())
from sina.settings import LOCAL_REDIS_HOST, LOCAL_REDIS_PORT, PROXY_BASEURL
import urllib.parse
from sina.spiders.utils import get_random_proxy



r = redis.Redis(host=LOCAL_REDIS_HOST, port=LOCAL_REDIS_PORT)
#delete existing keys
for key in r.scan_iter("weibo_search_timeline_spider*"):
    r.delete(key)

# get seeds from the seeds file
file_path = os.getcwd() + '/sina/seeds.txt'
start_search_keys = []
with open(file_path, 'r') as f:
    lines = f.readlines()
    for line in lines:
        search_key = line.strip()
        start_search_keys.append(search_key)
        #print("[DEBUG] redis entry: " + search_key)

# url setttings
sort_setting = ["time","hot"]
filter_setting = ["hasori","hasv"] # no "all"

# push seeds to redis
for key in start_search_keys:
    for sort in sort_setting:
        for ftr in filter_setting:
            params = {'keyword': key, 'sort':sort, 'filter':ftr}
            if PROXY_BASEURL:
                base_url = get_random_proxy()
            else:
                base_url = "https://weibo.cn"
            search_url = base_url +"/search/mblog?"+ urllib.parse.urlencode(params)+"&page=1"
            print(search_url)
            r.lpush('weibo_search_timeline_spider:start_urls', search_url)

print('Redis initialized')
