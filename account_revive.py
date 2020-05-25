import os
import pymongo
from pymongo.errors import DuplicateKeyError
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import sys
sys.path.append(os.getcwd())
from crawl_user_timeline.sina.settings import LOCAL_MONGO_HOST, LOCAL_MONGO_PORT, DB_NAME, PROXY_BASEURL
import time
from build_account import WeiboLogin
from apscheduler.schedulers.blocking import BlockingScheduler


def account_revive():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient[DB_NAME]
    mycol = mydb["account"]

    # find error account
    myquery = { 
        "$or": [
            {"status": "error"},
            {"status": {"$exists": False}}
        ]
    }
    mydoc = mycol.find(myquery)

    # revive account
    if mydoc.count():
        for x in mydoc:
            username = x["_id"]
            password = x["password"]
            try:
                print('Reviving: '+str(username))
                cookie_str = WeiboLogin(username, password).run()
                print('获取cookie成功')
                print('Cookie:')
                print(cookie_str)
            except Exception as e:
                print(e)
                continue

            try:
                mycol.insert(
                    {"_id": username, "password": password, "cookie": cookie_str, "status": "success"})
            except DuplicateKeyError as e:
                mycol.find_one_and_update({'_id': username}, {'$set': {'cookie': cookie_str, "status": "success"}})
    else:
        print("No error account found")

scheduler = BlockingScheduler()
scheduler.add_job(account_revive, 'interval', hours=1)
scheduler.start()