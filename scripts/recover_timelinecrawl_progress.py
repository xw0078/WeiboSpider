from pymongo import MongoClient
import pymongo
LOCAL_MONGO_HOST = '127.0.0.1'
LOCAL_MONGO_PORT = 27017


client = MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
profiles_collection = client["2020COVID19WEIBO"]['profiles_03042020']



seeds = profiles_collection.find({})
for seed in seeds:
    timelineraw_collection = client["2020COVID19WEIBO"]['UserTimelineRaw_03042020.json']
    latest_crawl = timelineraw_collection\
        .find(
            {"user_id": seed["_id"]},
            sort=[("crawl_time_utc",pymongo.DESCENDING)], # DESCENDING
            limit = 1
        )
    print(seed["_id"])
    if latest_crawl.count():
        last_page_url = latest_crawl[0]["page_url"]
        last_page_num = int(last_page_url.split("page=")[-1])
        last_page_num = last_page_num - 1
        print(last_page_url)
        print(last_page_num)
        profiles_collection.update_one(
            {"_id": seed["_id"]},
            {
                "$set": {
                    "timelineCrawlJob_current_page": last_page_num,
                },
            }
        )
    else:
        print("not found:"+seed["_id"])

    