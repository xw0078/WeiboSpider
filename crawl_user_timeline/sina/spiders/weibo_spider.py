#!/usr/bin/env python
# encoding: utf-8
import re
from lxml import etree
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.utils.project import get_project_settings
from scrapy_redis.spiders import RedisSpider
from sina.items import TimelinePageRaw,ProfileUpdateItem
from sina.spiders.utils import time_fix, extract_weibo_content, extract_comment_content
import time
from datetime import datetime as dt
import dateutil.parser
from sina.spiders.utils import get_random_proxy

class WeiboSpider(RedisSpider):
    name = "weibo_user_timeline_spider"
    
    redis_key = "weibo_user_timeline_spider:start_urls"
    all_page_num = 0
    current_page = 0
    weibo_baseurl = "https://weibo.cn"

    def __init__(self, *a, **kw):
        super(WeiboSpider, self).__init__(*a, **kw)
        settings=get_project_settings()
        time_start_str = settings.get('TIME_START')
        self.time_start_from = dt.strptime(time_start_str, "%Y-%m-%d %H:%M")
        self.use_proxy = settings.get('PROXY_BASEURL')


    def get_base_url(self):
        if self.use_proxy:
            return get_random_proxy()
        else:
            return "https://weibo.cn"

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


    def parse(self, response):

        current_page = int(response.url.split("page=")[-1])
        print("[INFO] Crawling Tweets Page: "+str(current_page))
        print("[INFO Crawling URL: " + response.url)


        selector = Selector(response)
        tweetpage_item = TimelinePageRaw()
        tweetpage_item['user_id'] = re.findall("(\d+)\?page",response.url)[0]
        tweetpage_item['page_url'] = re.sub("https://.*?/fireprox",self.weibo_baseurl,response.url)
        tweetpage_item['page_raw'] = selector.extract() # get raw page content
        tweetpage_item['crawl_time_utc'] = dt.utcnow()
        yield tweetpage_item

        time_stop_flag = 0 # stop crawling if hit specified start time

        tree_node = etree.HTML(response.body)
        tweet_nodes = tree_node.xpath('//div[@class="c" and @id]')
        if len(tweet_nodes) < 1: # no information on this page
            update = ProfileUpdateItem()
            update["timelineCrawlJob_current_complete"] = True
            update["timelineCrawlJob_current_page"] = current_page
            update["timelineCrawlJob_run_history"] = tweetpage_item['crawl_time_utc']
            yield update
            return

        for tweet_node in tweet_nodes:
            try:
                create_time_info_node = tweet_node.xpath('.//span[@class="ct"]')[-1]
                create_time_info = create_time_info_node.xpath('string(.)')
                print("[INFO] create_time_info raw: " + create_time_info)
                if "来自" in create_time_info:
                    created_at = time_fix(create_time_info.split('来自')[0].strip())
                    time_stop_flag = self.time_flag_compare(created_at) # time compare to trigger stop flag
                    
                else:
                    created_at = time_fix(create_time_info.strip())
                    time_stop_flag = self.time_flag_compare(created_at) # time compare to trigger stop flag

            except Exception as e:
                self.logger.error(e)

        #  keep looping until hit page with time range limit
        
        print("[DEBUG] timeflag:" + str(time_stop_flag))
        update = ProfileUpdateItem()
        if time_stop_flag == 0: 
            next_page = current_page + 1
            page_url = self.get_base_url() + '/{}?page={}'.format(tweetpage_item['user_id'],next_page)
            #page_url = response.url.replace('page='+str(current_page), 'page={}'.format(next_page))
            update["timelineCrawlJob_current_page"] = current_page
            update["timelineCrawlJob_current_complete"] = False
            update["uid"] = tweetpage_item['user_id']
            yield update
            yield Request(page_url, self.parse, dont_filter=True, meta=response.meta,priority=1)
        else:
            update["timelineCrawlJob_current_complete"] = True
            update["timelineCrawlJob_run_history"] = tweetpage_item['crawl_time_utc']
            update["timelineCrawlJob_current_page"] = current_page
            update["uid"] = tweetpage_item['user_id']
            yield update

if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl('weibo_user_timeline_spider')
    process.start()
    print("[INFO] Parser Started")