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


readSource = "mongodb://127.0.0.1:27017/2020COVID19WEIBO.user_timelines_raw"

def main_html_parser(iterator):
    client = MongoClient('mongodb://localhost:27017/')
    db = client['2020COVID19WEIBO']
    collection = db["statuses"]
    
    for element in iterator:
        tree_node = etree.HTML(element["page_raw"]) 
        tweet_nodes = tree_node.xpath('//div[@class="c" and @id]')
        if len(tweet_nodes) > 0:
            for tweet_node in tweet_nodes:
                #print(element[3])
                tweet_item = tweet_node_parser(tweet_node,element["crawl_time_utc"])
                if tweet_item:
                    try:
                        collection.update_one(
                                {"_id": tweet_item["_id"]},
                                {
                                    "$setOnInsert": { # insert item if id not found
                                        "user_id" : tweet_item["user_id"],
                                        "crawl_time_utc" : tweet_item["crawl_time_utc"],
                                        "created_at_utc" : tweet_item["created_at_utc"],
                                        "exact_created_at" : tweet_item["exact_created_at"],
                                        "content" : tweet_item["content"],
                                        "user_id" : tweet_item["user_id"],
                                        "tool" : tweet_item["tool"],
                                        "multi_imgs_page_url" : tweet_item["multi_imgs_page_url"],
                                        "img_truncated" : tweet_item["img_truncated"],
                                        "multi_img_ids" : tweet_item["multi_img_ids"],
                                        "single_img_url": tweet_item["single_img_url"],
                                        "img_crawl_status":tweet_item["img_crawl_status"],
                                        "video_url" : tweet_item["video_url"],
                                        "video_crawl_status":tweet_item["video_crawl_status"],
                                        "location_url" : tweet_item["location_url"],
                                        "location_name" : tweet_item["location_name"],
                                        "is_repost" : tweet_item["is_repost"],
                                        "origin_status_id" : tweet_item["origin_status_id"],
                                        "origin_user_name" : tweet_item["origin_user_name"],
                                        "origin_user_url" : tweet_item["origin_user_url"],
                                        "content_truncated" : tweet_item["content_truncated"],
                                        "content_crawl_status" : tweet_item["content_crawl_status"],
                                        "content" : tweet_item["content"],
                                        "embeded_urls" : tweet_item["embeded_urls"],
                                        "mention_users" : tweet_item["mention_users"],
                                        "hashtags" : tweet_item["hashtags"],
                                        "crawl_time_utc" : tweet_item["crawl_time_utc"]
                                    },
                                    "$addToSet": {
                                        "status_history": tweet_item['status_history'],
                                        "origin_status_history": tweet_item['origin_status_history']
                                    }
                                },
                                upsert=True
                            )
                    except DuplicateKeyError:
                        pass
            yield "Done"

def get_tweet_id(tweet_url):
    if tweet_url.startswith("https://weibo.cn/comment/"):
        return tweet_url.split("/")[-1].split("?")[0]
    else:
        return ""

def time_fix(time_string,crawl_time_utc):
    if '分钟前' in time_string:
        minutes = re.search(r'^(\d+)分钟', time_string).group(1)
        created_at = crawl_time_utc - datetime.timedelta(minutes=int(minutes))
        return created_at.strftime('%Y-%m-%d %H:%M:%S'),False

    if '小时前' in time_string:
        minutes = re.search(r'^(\d+)小时', time_string).group(1)
        created_at = crawl_time_utc - datetime.timedelta(hours=int(minutes))
        return created_at.strftime('%Y-%m-%d %H:%M:%S'),False

    if '今天' in time_string:
        return time_string.replace('今天', crawl_time_utc.strftime('%Y-%m-%d')),False

    if '月' in time_string:
        time_string = time_string.replace('月', '-').replace('日', '')
        time_string = str(crawl_time_utc.year) + '-' + time_string
        
        return time_string,False

    return time_string,True

def time_to_utc(timeString):
    time = dateutil.parser.parse(timeString)
    time = time.replace(tzinfo=timezone.utc)
    return time

def tweet_node_parser(tweet_node,crawl_time_utc):
    tweet_item = {}
    base_url = "https://weibo.cn"
    
    tweet_item['crawl_time_utc'] = crawl_time_utc
    # get tweet id, user id, weibo url
    tweet_info_url = tweet_node.xpath('.//a[contains(text(),"转发[")]/@href')
    if tweet_info_url:
        tweet_info_url = tweet_info_url[0]
        user_tweet_id = re.search(r'/repost/(.*?)\?uid=(\d+)', tweet_info_url)
    else:
        tweet_info_url = tweet_node.xpath('.//a[contains(text(),"评论[")]/@href')
        if tweet_info_url:
            tweet_info_url = tweet_info_url[0]
            user_tweet_id = re.search(r'/comment/(.*?)\?uid=(\d+)', tweet_info_url)
        else:
            tweet_info_url = tweet_node.xpath('.//a[contains(text(),"赞[")]/@href')
            if tweet_info_url:
                tweet_info_url = tweet_info_url[0]
                user_tweet_id = re.search(r'/((comment)|(repost))/(.*?)\?uid=(\d+)', tweet_info_url)
            else:
                print("No link info found for the status")
                return 0
    
    tweet_item['user_id'] = user_tweet_id.group(2)
    tweet_item['_id'] = '{}'.format(user_tweet_id.group(1))
    # get created time and tool
    create_time_info_node = tweet_node.xpath('.//span[@class="ct"]')[-1]
    create_time_info = create_time_info_node.xpath('string(.)')
    if "来自" in create_time_info:
        time_string,tweet_item['exact_created_at'] = time_fix(create_time_info.split('来自')[0].strip(),crawl_time_utc)
        tweet_item['created_at_utc'] = time_to_utc(time_string)
        tweet_item['tool'] = create_time_info.split('来自')[1].strip()
    else:
        time_string, tweet_item['exact_created_at'] = time_fix(create_time_info.split('来自')[0].strip(),crawl_time_utc)
        tweet_item['created_at_utc'] = time_to_utc(time_string)
        tweet_item['tool'] = ""

    # get repost,commet,like num
    like_num = tweet_node.xpath('.//a[contains(text(),"赞[")]/text()')
    if like_num:
        like_num = int(re.search('\d+', like_num[-1]).group()) # dynamic
    else:
        like_num = tweet_node.xpath('.//span[@class="cmt" and contains(text(),"赞[")]/text()')
        if like_num:
            like_num = int(re.search('\d+', like_num[-1]).group()) # dynamic
        else:
            like_num = -1

    repost_num = tweet_node.xpath('.//a[contains(text(),"转发[")]/text()')
    if repost_num:
        repost_num = int(re.search('\d+', repost_num[-1]).group()) # dynamic
    else:
        repost_num = tweet_node.xpath('.//span[@class="cmt" and contains(text(),"转发[")]/text()')
        if repost_num:
            repost_num = int(re.search('\d+', repost_num[-1]).group()) # dynamic
        else:
            repost_num = -1

    comment_num = tweet_node.xpath('.//a[contains(text(),"评论[") and not(contains(text(),"原文"))]/text()')
    if comment_num:
        comment_num = int(re.search('\d+', comment_num[-1]).group()) # dynamic
    else:
        comment_num = tweet_node.xpath('.//span[@class="cmt" and contains(text(),"评论[") and not(contains(text(),"原文")]/text()')
        if comment_num:
            comment_num = int(re.search('\d+', comment_num[-1]).group()) # dynamic
        else:
            comment_num = -1
    
    # get multi img link if exist
    multi_img_link = tweet_node.xpath('.//a[contains(text(),"组图")]/@href')
    if multi_img_link:
        tweet_item['multi_imgs_page_url'] = str(multi_img_link[-1])
        tweet_item['multi_img_ids'] = ""
        tweet_item['img_truncated'] = True
        tweet_item['img_crawl_status'] = 0
        tweet_item['single_img_url'] = ""
    else:
        tweet_item['multi_imgs_page_url'] = ""
        tweet_item['multi_img_ids'] = ""
        tweet_item['img_truncated'] = False
        tweet_item['img_crawl_status'] = ""
        tweet_item['single_img_url'] = ""

    # first img link
    first_img = tweet_node.xpath('.//img[@alt="图片"]/@src')
    if first_img:
        tweet_item['multi_img_ids'] = str(first_img[0]).split("/")[-1].split(".")[0]
        tweet_item['single_img_url'] = tweet_node.xpath('.//a[contains(text(),"原图")]/@href')[-1]
        tweet_item['img_crawl_status'] = 0
    # get video link
    videos = tweet_node.xpath('.//a[contains(@href,"https://m.weibo.cn/s/video/show?object_id=")]/@href')
    if videos:
        tweet_item['video_url'] = str(videos[0])
        tweet_item['video_crawl_status'] = 0
    else:
        tweet_item['video_url'] = ""
        tweet_item['video_crawl_status'] = ""

    # get map info
    map_node = tweet_node.xpath('.//a[contains(text(),"显示地图")]')
    if map_node:
        map_node = map_node[0]
        tweet_item['location_url'] = map_node.xpath('./@href')[0]
        map_info = re.search(r'xy=(.*?)&', tweet_item['location_url'])
        if map_info:
            map_info = map_info.group(1)
            tweet_item['location_name'] = str(map_info)
        else:
            tweet_item['location_name'] = ""
    else:
        tweet_item['location_url'] = ""
        tweet_item['location_name'] = ""

    # check repost information
    repost_node = tweet_node.xpath('.//a[contains(text(),"原文评论[")]/@href')
    #deleted = tweet_node.xpath('.//span[contains(text(),"此微博已被作者删除")]/text()')
    if repost_node:
        tweet_item['is_repost'] = True
        tweet_item['origin_status_id'] = get_tweet_id(str(repost_node[0]))
        retweet_user_name = tweet_node.xpath('.//span[@class="cmt"]/a/text()')
        if retweet_user_name: # check if available
            tweet_item['origin_user_name'] = str(tweet_node.xpath('.//span[@class="cmt"]/a/text()')[-1])
            tweet_item['origin_user_url'] = str(tweet_node.xpath('.//span[@class="cmt"]/a/@href')[-1])
            retweet_like_num = tweet_node.xpath('.//span[@class="cmt" and contains(text(),"赞[")]/text()')[-1]
            retweet_like_num = int(re.search('\d+', retweet_like_num).group()) # dynamic
            retweet_comment_num = tweet_node.xpath('.//a[contains(text(),"原文评论[")]/text()')[-1]
            retweet_comment_num = int(re.search('\d+', retweet_comment_num).group()) # dynamic
            retweet_repost_num = tweet_node.xpath('.//span[@class="cmt" and contains(text(),"原文转发[")]/text()')[-1]
            retweet_repost_num = int(re.search('\d+', retweet_repost_num).group()) # dynamic
        else:
            tweet_item['origin_user_name'] = ""
            tweet_item['origin_user_url'] = ""
            retweet_like_num = -1
            retweet_comment_num = -1
            retweet_repost_num = -1
    else:
        tweet_item['is_repost'] = False
        tweet_item['origin_status_id'] = ""
        tweet_item['origin_user_name'] = ""
        tweet_item['origin_user_url'] = ""
        retweet_like_num = -1
        retweet_comment_num = -1
        retweet_repost_num = -1

    # get content and check if it is partial
    all_content_link = tweet_node.xpath('.//a[text()="全文" and contains(@href,"ckAll=1")]')
    if all_content_link:
        tweet_item['content_truncated'] = True
        tweet_item['full_tweet_url'] = base_url + all_content_link[0].xpath('./@href')[0]
        tweet_item['content_crawl_status'] = 0
    else:
        tweet_item['content_truncated'] = False
        tweet_item['content_crawl_status'] = ""

    # get content, replace emoji picture to text if applicated
    
    content_node = tweet_node.xpath('.//span[@class="ctt"]')[-1]
    raw_content = etree.tostring(content_node,encoding='unicode')
    content_node = re.sub('<img alt=\"(.*?)\".*?/>',r"\1",raw_content)
    content_node = etree.HTML(content_node)
    tweet_item['content'] = str(content_node.xpath('string(.)'))


    # get hashtags
    hashtags = tweet_node.xpath('.//span[@class="ctt"]/a[contains(text(),"#")]/text()')
    if hashtags:
        #print(hashtags)
        hashtags_list = []
        for h in hashtags:
            hashtag = re.findall('(#.*?#)', str(h))
            if hashtag:
                hashtags_list.append(hashtag[0])
        tweet_item['hashtags'] = hashtags_list
    else:
        tweet_item['hashtags'] = ""

    # get content links if exist
    embeded_urls = tweet_node.xpath('.//span[@class="ctt"]/a[not(contains(text(),"#")) and not(contains(text(),"全文")) and not(contains(text(),"@"))]/@href')
    if embeded_urls:
        tweet_item['embeded_urls'] = list(map(lambda x: str(x),embeded_urls))
    else:
        tweet_item['embeded_urls'] = ""
    
    # get mentions
    mention_links = tweet_node.xpath('.//span[@class="ctt"]/a[(contains(text(),"@"))]/@href')
    mention_names = tweet_node.xpath('.//span[@class="ctt"]/a[(contains(text(),"@"))]/text()')
    tweet_item["mention_users"] = []
    if mention_links:
        mention_user_urls = list(map(lambda x: str(x),mention_links))
        mention_user_names = list(map(lambda x: str(x),mention_names))
        for url,name in zip(mention_user_urls,mention_user_names):
            mention_user_pair = {
                'mention_user_name': name,
                'mention_user_url': url
            }
            tweet_item["mention_users"].append(mention_user_pair)


    # dynamic fields
    #tweet_item['tweet_dynamic_history'] = []
    tweet_dynamics = {
        "like_num": like_num,
        "comment_num": comment_num,
        "repost_num": repost_num,
        "crawl_time_utc": crawl_time_utc
    }
    tweet_item['status_history'] = tweet_dynamics

    #tweet_item['retweet_dynamic_history'] = []
    if repost_node:
        retweet_dynamics = {
            "origin_status_like_num": retweet_like_num,
            "origin_status_comment_num": retweet_comment_num,
            "origin_status_repost_num": retweet_repost_num,
            "crawl_time_utc": crawl_time_utc
        }
        tweet_item['origin_status_history'] = retweet_dynamics
    else:
        tweet_item['origin_status_history'] = ""

    return tweet_item




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

