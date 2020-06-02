'''
Validate downloaded images and update tweet status in mongo db
'''



import glob
from pymongo import MongoClient
import pymongo
from settings import LOCAL_MONGO_HOST,LOCAL_MONGO_PORT,IMAGES_STORE,DB_NAME
import os
from apscheduler.schedulers.blocking import BlockingScheduler

def validate_downloaded_images():
    print("[INFO] DB: "+DB_NAME)
    client = MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
    collection = client[DB_NAME]['statuses']
    image_store = IMAGES_STORE+"/full/"

    # process single image documents
    single_image_documents = collection.find(
        {"img_crawl_status": 0,"img_truncated": False}
    )

    for x in single_image_documents:
        img_id = x["multi_img_ids"]
        img_path = image_store+img_id+".jpg"
        #print(img_path)
        if os.path.exists(img_path):
            collection.update_one(
                {"_id": x["_id"]},
                {
                    "$set": {
                        "img_crawl_status": 1,
                    },
                }
            )
        else:
            print("Image not found:" + img_id)

    # process multi image documents
    multi_image_documents = collection.find(
        {"img_crawl_status": 0,"img_truncated": True}
    )

    for x in multi_image_documents:
        img_id_list = x["multi_img_ids"]
        flag = 0
        for img_id in img_id_list:
            img_path = image_store+img_id+".jpg"   
            #print(img_path)
            if os.path.exists(img_path):
                flag = 1
            else:
                flag = 0
                break
        if flag:
            collection.update_one(
                {"_id": x["_id"]},
                {
                    "$set": {
                        "img_crawl_status": 1,
                        "img_truncated":False
                    },
                }
            )
        else:
            print("Image not found:" + x["multi_imgs_page_url"])

scheduler = BlockingScheduler()
scheduler.add_job(validate_downloaded_images, 'interval', hours=10)
scheduler.start()