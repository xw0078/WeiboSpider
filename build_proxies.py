import os
import pymongo
from pymongo.errors import DuplicateKeyError
from selenium import webdriver
import sys
sys.path.append(os.getcwd())
from crawl_user_timeline.sina.settings import LOCAL_MONGO_HOST, LOCAL_MONGO_PORT, DB_NAME, PROXY_BASEURL
import time

# establish mongodb connection
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
collection = myclient[DB_NAME]["proxies"]

# read proxies.txt
file_path = os.getcwd() + '/proxies.txt'
with open(file_path, 'r') as f:
        lines = f.readlines()
        for line in lines:
            proxy_pair = line.split(" ")
            proxy = proxy_pair[0].strip()
            target = proxy_pair[1].strip()
            try:
                print(proxy)
                print(target)
                collection.insert_one(
                    {"_id": proxy, "target":target,"status": "success"})
            except DuplicateKeyError as e:
                pass

print("Proxies inserted to MongoDB")
