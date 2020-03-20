#!/usr/bin/env python
# encoding: utf-8
import re
import redis
from lxml import etree
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.utils.project import get_project_settings
from scrapy_redis.spiders import RedisSpider
from sina.items import ImageItem,MultiImagePageRawItem
import time
from datetime import datetime as dt
from sina.spiders.utils import get_random_proxy


class WeiboSpider(RedisSpider):
    name = "weibo_image_spider"
    redis_key = "weibo_image_spider:start_urls"
    all_page_num = 0
    current_page = 0
    weibo_baseurl = "https://weibo.cn"
 

    def __init__(self, *a, **kw):
        super(WeiboSpider, self).__init__(*a, **kw)
        settings=get_project_settings()
        time_start_str = settings.get('TIME_START')
        self.time_start_from = dt.strptime(time_start_str, "%Y-%m-%d %H:%M")
        self.use_proxy = settings.get('PROXY_BASEURL')
        self.r = redis.Redis(host=settings.get('LOCAL_REDIS_HOST'), port=settings.get('LOCAL_REDIS_PORT'))

    def get_base_url(self):
        if self.use_proxy:
            return get_random_proxy()
        else:
            return "https://weibo.cn"


    def getImageIDs(self,response):
        tree_node = etree.HTML(response.body)
        img_ids = []
        img_urls = tree_node.xpath('//img/@src')
        for url in img_urls:
            img_id = str(url).split("/")[-1].split(".")[0]
            img_ids.append(img_id)
        return img_ids
    
    def getImageUrlsFromIDs(self,img_ids):
        if self.use_proxy:
            base_url = get_random_proxy("http://wx1.sinaimg.cn/")
        else:
            base_url = "http://wx1.sinaimg.cn"
        img_urls = []
        for img_id in img_ids:
            img_url = base_url + "/large/"+img_id
            img_urls.append(img_url)
        return img_urls



    # Default Start
    def parse(self, response):
        # get input url to ensure id is correct (not redirected url)
        img_url = response.url
        print("[DEBUG] response: "+response.url)

        if "/mblog/picAll/" in img_url: # all images
            selector = Selector(response)
            multi_image_item = MultiImagePageRawItem()

            multi_image_item['page_url'] = re.sub("https://.*?/fireprox",self.weibo_baseurl,response.url)
            multi_image_item['_id'] = multi_image_item['page_url'].split("/")[-1].split("?")[0]
            multi_image_item['page_raw'] = selector.extract() # get raw page content
            multi_image_item['crawl_time_utc'] = dt.utcnow()
            multi_image_item['multi_img_ids'] = self.getImageIDs(response)
            yield multi_image_item
            
            img_urls = self.getImageUrlsFromIDs(multi_image_item['multi_img_ids'])
            for img_url in img_urls:
                self.r.lpush('weibo_image_spider:start_urls', img_url)
                #yield Request(url=img_url,callback=self.parse,priority=1)
            return 

        image_item = ImageItem()
        image_item["image_urls"] = [img_url]
        image_name = img_url.split("/")[-1].split(".")[0]
        image_item["images"] = [image_name]
        yield image_item


if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl('weibo_image_spider')
    process.start()
    print("[INFO] Parser Started")