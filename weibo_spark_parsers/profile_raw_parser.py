from pyspark import SparkFiles
from pyspark.sql import SparkSession
import logging
import os
from lxml import etree
import datetime
from datetime import datetime as dt
import re
from pyspark.sql.types import StructType
from pyspark.sql.types import StructField
from pyspark.sql.types import StringType
from pyspark.sql.types import Row
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError
import dateutil.parser
from datetime import timezone


readSource = "mongodb://127.0.0.1:27017/2020COVID19WEIBO.UserInformationRaw_03042020"



def main_html_parser(iterator):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['2020COVID19WEIBO']
    collection = db["profiles_03042020"]
    
    for element in iterator:
        profile_item = profile_raw_parser(element)
        try:
            collection.update_one(
                    {"_id": profile_item["_id"]},
                    {
                        "$setOnInsert": { # insert item if id not found
                            "custom_id" : profile_item["custom_id"],
                            "nick_name" : profile_item["nick_name"],
                            "gender" : profile_item["gender"],
                            "province" : profile_item["province"],
                            "city" : profile_item["city"],
                            "brief_introduction" : profile_item["brief_introduction"],
                            "birthday" : profile_item["birthday"],
                            "sex_orientation" : profile_item["sex_orientation"],
                            "sentiment" : profile_item["sentiment"],
                            "vip_level" : profile_item["vip_level"],
                            "authentication" : profile_item["authentication"],
                            "labels" : profile_item["labels"],
                            "timelineCrawlJob_current_page" : 1,
                            "timelineCrawlJob_current_complete" : False,
                            "timelineCrawlJob_run_history" : [],
                        }
                    },
                    upsert=True
                )
        except DuplicateKeyError:
            pass
        yield "Done"




def time_to_utc(timeString):
    time = dateutil.parser.parse(timeString)
    time = time.replace(tzinfo=timezone.utc)
    return time

def profile_raw_parser(profile_raw):
    profile_item = {}
    profile_item["_id"] = profile_raw["_id"]
    profile_item["crawl_time_utc"] = profile_raw["crawl_time_utc"]

    tree_node = etree.HTML(profile_raw["page_raw"])
    basic_info_node = tree_node.xpath('.//div[@class="c"]//text()')
    basic_info_node = ";".join(basic_info_node)
    nick_name = re.findall('昵称;?[：:]?(.*?);', basic_info_node)
    gender = re.findall('性别;?[：:]?(.*?);', basic_info_node)
    place = re.findall('地区;?[：:]?(.*?);', basic_info_node)
    briefIntroduction = re.findall('简介;?[：:]?(.*?);', basic_info_node)
    birthday = re.findall('生日;?[：:]?(.*?);', basic_info_node)
    sex_orientation = re.findall('性取向;?[：:]?(.*?);', basic_info_node)
    sentiment = re.findall('感情状况;?[：:]?(.*?);', basic_info_node)
    vip_level = re.findall('会员等级;?[：:]?(.*?);', basic_info_node)
    authentication = re.findall('认证;?[：:]?(.*?);', basic_info_node)
    labels = re.findall('标签;?[：:]?(.*?)更多>>', basic_info_node)
    optional_id = re.findall("手机版:[^;]*", basic_info_node)[0].split("/")

    if optional_id and optional_id[0]:
        profile_item["custom_id"] = optional_id[-1]
    else:
        profile_item["custom_id"] = ""

    if nick_name:
        profile_item["nick_name"] = nick_name[0].replace(u"\xa0", "")
    else:
        profile_item["nick_name"] = ""

    if gender:
        profile_item["gender"] = gender[0].replace(u"\xa0", "")
    else:
        profile_item["gender"] = ""

    if place:
        place = place[0].replace(u"\xa0", "").split(" ")
        profile_item["province"] = place[0]
        if len(place) > 1:
            profile_item["city"] = place[1]
        else:
            profile_item["city"] = ""
    else:
        profile_item["province"] = ""
        profile_item["city"] = ""

    if briefIntroduction:
        profile_item["brief_introduction"] = briefIntroduction[0].replace(u"\xa0", "")
    else:
        profile_item["brief_introduction"] = ""

    if birthday:
        profile_item['birthday'] = birthday[0]
    else:
        profile_item['birthday'] = ""

    if sex_orientation:
        if sex_orientation[0].replace(u"\xa0", "") == gender[0]:
            profile_item["sex_orientation"] = "同性恋"
        else:
            profile_item["sex_orientation"] = "异性恋"
    else:
        profile_item["sex_orientation"] = ""
    
    if sentiment:
        profile_item["sentiment"] = sentiment[0].replace(u"\xa0", "")
    else:
        profile_item["sentiment"] = ""

    if vip_level:
        profile_item["vip_level"] = vip_level[0].replace(u"\xa0", "")
    else:
        profile_item["vip_level"] = ""

    if authentication:
        profile_item["authentication"] = authentication[0].replace(u"\xa0", "")
    else:
        profile_item["authentication"] = ""

    if labels:
        profile_item["labels"] = labels[0].replace(u"\xa0", ",").replace(';', '').strip(',')
    else:
        profile_item["labels"] = ""


    return profile_item




class weiboPySparkMDBParser:

    def __init__(self):
        logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))
        spark_builder = (
            SparkSession
            .builder
            .master("local")
            .config('spark.mongodb.input.uri', 'mongodb://127.0.0.1:27017/Sina2')
            .config('spark.mongodb.output.uri', 'mongodb://127.0.0.1:27017/Sina2')
            .appName("Weibo Pyspark MongoDB Parser"))
        self.spark = spark_builder.getOrCreate() # get spark session
        logging.info("Spark Session Created")


    def findPotentialUser(self):
        df = self.spark.read.format("com.mongodb.spark.sql.DefaultSource").option("uri",readSource).load()
        df.show(2)
        rdd = df.rdd
        transformed = rdd.mapPartitions(main_html_parser)
        transformed.count()
        #logging.info(transformed.take(2))
        # transformed_df = transformed.toDF()
        # transformed_df.write.format("mongo").mode("append").option("database","SWtest").option("collection", "test1").save()

    

if __name__ == "__main__":
    parser = weiboPySparkMDBParser()
    parser.findPotentialUser()
    logging.info("END")

