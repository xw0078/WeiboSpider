from pymongo import MongoClient
import pymongo
LOCAL_MONGO_HOST = '127.0.0.1'
LOCAL_MONGO_PORT = 27017


client = MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
statuses_collection = client["2020COVID19WEIBO"]['statuses']

unique_hashtags = statuses_collection.distinct("hashtags")

