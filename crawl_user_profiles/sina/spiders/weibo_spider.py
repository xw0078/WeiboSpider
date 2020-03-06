#!/usr/bin/env python
# encoding: utf-8
import re
from lxml import etree
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.utils.project import get_project_settings
from scrapy_redis.spiders import RedisSpider
from sina.items import ProfileItem,ProfileRawItem
import time
from datetime import datetime as dt
import dateutil.parser
from sina.spiders.utils import get_random_proxy

class WeiboSpider(RedisSpider):
    name = "weibo_user_profile_spider"
    
    redis_key = "weibo_user_profile_spider:start_urls"
    all_page_num = 0
    current_page = 0
    weibo_baseurl = "https://weibo.cn"
    def __init__(self, *a, **kw):
        super(WeiboSpider, self).__init__(*a, **kw)
        settings=get_project_settings()
        time_start_str = settings.get('TIME_START')
        self.time_start_from = dt.strptime(time_start_str, "%Y-%m-%d %H:%M")
        self.use_proxy = settings.get('PROXY_BASEURL')

    def time_flag_compare(self, timeString):
        print("[DEBUG] Created Time String: "+timeString)
        time = dateutil.parser.parse(timeString)
        if self.time_start_from > time:
            print("[INFO] Hit Start Time Criteria")
            return 1
        else:
            print("[INFO] Valid Created Time")
            return 0

    def tweet_id_check(self,tweet_url):
        f=open("tweet_url_cache.txt", "a+")
    
    def get_tweet_id(self,tweet_url):
        
        if tweet_url.startswith(self.base_url+"/comment/"):
            return tweet_url.split("/")[-1].split("?")[0]
        else:
            return "NA"

    def get_base_url(self):
        if self.use_proxy:
            return get_random_proxy()
        else:
            return "https://weibo.cn"

    # Default Start
    def parse(self, response):
        print("[DEBUG] response: "+response.url)
        selector = Selector(response)

        profileraw_item = ProfileRawItem()
        uid_from_url = re.findall('(\d+)/info', response.url)
        if uid_from_url:
            profileraw_item['uid'] = re.findall('(\d+)/info', response.url)[0] # get user id
            #print("[DEBUG] response url: "+response.url)      
            profileraw_item['page_url'] = response.url.replace(self.base_url,self.weibo_baseurl)
            profileraw_item['page_raw'] = selector.extract() # get raw page content
            profileraw_item['crawl_time_utc'] = dt.utcnow()
            
            # get parsed profile result
            profile_item = ProfileItem()
            profile_item["_id"] = profileraw_item["uid"]
            profile_item["crawl_time_utc"] = profileraw_item["crawl_time_utc"]

            tree_node = etree.HTML(profileraw_item["page_raw"])
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

            # set seed tracing fields
            profile_item["timelineCrawlJob_current_page"] = 1
            profile_item["timelineCrawlJob_current_complete"] = False
            profile_item["timelineCrawlJob_run_history"] = []

            yield profile_item
            yield profileraw_item
            
        else:
            tree_node = etree.HTML(response.body)
            infopage_url = tree_node.xpath('//div[@class="u"]//a[contains(text(),"资料")]/@href')[-1]
            profileraw_item['_id'] = infopage_url.split("/")[-2]
            yield Request(url=self.get_base_url() + '/{}/info'.format(profileraw_item['_id']),
                    callback=self.parse, meta={'user_id': profileraw_item['_id']},
                    priority=1)
        

    

if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl('weibo_user_profile_spider')
    process.start()
    print("[INFO] Parser Started")