#!/usr/bin/env python
# encoding: utf-8
import re
from lxml import etree
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.utils.project import get_project_settings
from scrapy_redis.spiders import RedisSpider
from sina.items import SearchPageItem
from sina.spiders.utils import time_fix, extract_weibo_content, extract_comment_content
import time
from datetime import datetime as dt

class WeiboSpider(RedisSpider):
    name = "weibo_search_raw_spider"
    redis_key = "weibo_search_raw_spider:start_urls"
    all_page_num = 0
    current_page = 0
    weibo_baseurl = "https://weibo.cn"

    def __init__(self, *a, **kw):
        super(WeiboSpider, self).__init__(*a, **kw)
        settings=get_project_settings()
        time_start_str = settings.get('TIME_START')
        self.time_start_from = dt.strptime(time_start_str, "%Y-%m-%d %H:%M")
        if settings.get('PROXY_BASEURL'):
            self.base_url = settings.get('PROXY_BASEURL')
        else:
            self.base_url = "https://weibo.cn"

    def time_flag_compare(self, timeString):
        print("[DEBUG] Created Time String: "+timeString)
        time = dt.strptime(timeString,'%Y-%m-%d %H:%M')
        if self.time_start_from > time:
            print("[INFO] Hit Start Time Criteria")
            return 1
        else:
            return 0

    # Default Start
    def parse(self, response):
        current_page = response.url.split("&")[1].split("=")[-1]
        current_page = int(current_page)
        #print("[DEBUG] current_page:" + str(current_page))
        print("[DEBUG] response.url:" + str(response.url))

        selector = Selector(response)
        searchpage_item = SearchPageItem()
        searchpage_item['page_url'] = response.url.replace(self.base_url,self.weibo_baseurl)
        searchpage_item['page_raw'] = selector.extract() # get raw page content      
        searchpage_item['search_key'] = searchpage_item['page_url'].split("&")[2].split("=")[-1]
        searchpage_item['sort_setting'] = searchpage_item['page_url'].split("&")[3].split("=")[-1]
        searchpage_item['filter_setting'] = searchpage_item['page_url'].split("&")[4].split("=")[-1]
        searchpage_item['crawl_time_utc'] = dt.utcnow()
        yield searchpage_item

        # print("[DEBUG] page content:" + searchpage_item['page_raw'])
        # print("[DEBUG] original url:" + searchpage_item['page_url'])

        tree_node = etree.HTML(response.body)
        tweet_nodes = tree_node.xpath('//div[@class="c" and @id]')
        if len(tweet_nodes) == 0 and current_page != 1:
            if response.meta["empty_page_count"] > 0:
                empty_page_count = response.meta["empty_page_count"] + 1
            else:
                empty_page_count = 1
        else:
            empty_page_count = 0

        
        if empty_page_count != 3:
            next_page = current_page + 1
            page_url = response.url.replace('page='+str(current_page), 'page={}'.format(next_page))
            yield Request(page_url, self.parse, dont_filter=True, meta={'empty_page_count': empty_page_count},priority=1)
        

if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl('weibo_search_raw_spider')
    process.start()
    print("[INFO] Parser Started")