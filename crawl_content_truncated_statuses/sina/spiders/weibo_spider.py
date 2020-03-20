#!/usr/bin/env python
# encoding: utf-8
import re
from lxml import etree
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.utils.project import get_project_settings
from scrapy_redis.spiders import RedisSpider
from sina.items import StatusPageItem
import time
from datetime import datetime as dt

class WeiboSpider(RedisSpider):
    name = "weibo_status_truncated_spider"
    redis_key = "weibo_status_truncated_spider:start_urls"
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


    # Default Start
    def parse(self, response):
        selector = Selector(response)
        statuspage_item = StatusPageItem()
        # get input url to ensure id is correct (not redirected url)
        if response.request.meta.get('redirect_urls'):
            url = response.request.meta['redirect_urls'][0]
        else:
            url = response.request.url
        statuspage_item['page_url'] = re.sub("https://.*?/fireprox",self.weibo_baseurl,url)
        statuspage_item['_id'] = statuspage_item['page_url'].split("/")[-1].split("?")[0]
        statuspage_item['page_raw'] = selector.extract() # get raw page content
        statuspage_item['job'] = "content_truncated"
        statuspage_item['crawl_time_utc'] = dt.utcnow()
        print("[INFO] input url: " + statuspage_item['page_url'])
        yield statuspage_item

if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl('weibo_status_truncated_spider')
    process.start()
    print("[INFO] Parser Started")