#!/usr/bin/env python
# encoding: utf-8
import re
from lxml import etree
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.utils.project import get_project_settings
from scrapy_redis.spiders import RedisSpider
from sina.items import InformationItem, TweetPageItem, TweetItem, RelationPageItem
from sina.spiders.utils import time_fix, extract_weibo_content, extract_comment_content
import time
from datetime import datetime as dt
import dateutil.parser


class WeiboSpider(RedisSpider):
    name = "weibo_user_raw_spider"
    
    redis_key = "weibo_user_raw_spider:start_urls"
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

        


    # Default Start
    def parse(self, response):
        selector = Selector(response)

        information_item = InformationItem()
        uid_from_url = re.findall('(\d+)/info', response.url)
        if uid_from_url:
            information_item['_id'] = re.findall('(\d+)/info', response.url)[0] # get user id
        else:
            information_item['_id'] = "NA"


        information_item['page_url'] = response.url.replace(self.base_url,self.weibo_baseurl)
        information_item['page_raw'] = selector.extract() # get raw page content
        information_item['crawl_time_utc'] = dt.utcnow()
        yield information_item

        # request tweets page
        if uid_from_url:
            yield Request(url=self.base_url + '/{}/profile?page=1'.format(information_item['_id']),
                      callback=self.parse_tweet, meta={'user_id': information_item['_id']},
                      priority=1)
        else:
            yield Request(url=response.url + '?page=1',callback=self.parse_tweet, meta={'user_id': information_item['_id']},
                      priority=1)

        # 获取关注列表
        # if information_item['follows_num'] < 500: # if no more than 500 follows
        #     yield Request(url=self.base_url + '/{}/follow?page=1'.format(information_item['_id']),
        #                 callback=self.parse_follow,meta={'user_id': information_item['_id']},
        #                 dont_filter=True,priority=3)
        # 获取粉丝列表
        # if information_item['fans_num'] < 500: # if no more than 500 fans
        #     yield Request(url=self.base_url + '/{}/fans?page=1'.format(information_item['_id']),
        #                 callback=self.parse_fans,meta={'user_id': information_item['_id']},
        #                 dont_filter=True,priority=3)



    def parse_tweet(self, response):
        if response.url.endswith('page=1'):
            # if page 1, get all page number
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                self.all_page_num = all_page

        current_page = int(response.url.split("page=")[-1])
        print("[INFO] Crawling Tweets Page: "+str(current_page))
        print("[INFO Crawling URL" + response.url)
        """
        解析本页的数据
        """
        selector = Selector(response)
        tweetpage_item = TweetPageItem()
        tweetpage_item['user_id'] = response.meta["user_id"]
        tweetpage_item['page_url'] = response.url.replace(self.base_url,self.weibo_baseurl)
        tweetpage_item['page_raw'] = selector.extract() # get raw page content
        tweetpage_item['crawl_time_utc'] = dt.utcnow()
        yield tweetpage_item

        time_stop_flag = 0 # stop crawling if hit specified start time

        tree_node = etree.HTML(response.body)
        tweet_nodes = tree_node.xpath('//div[@class="c" and @id]')
        if len(tweet_nodes) < 1:
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

                # 检测由没有阅读全文: 
                # all_content_link = tweet_node.xpath('.//a[text()="全文" and contains(@href,"ckAll=1")]')
                # if all_content_link:
                #     all_content_url = self.base_url + all_content_link[0].xpath('./@href')[0]
                #     yield Request(all_content_url, callback=self.parse_all_content, meta={'user_id': response.meta["user_id"]},
                #                   priority=1)
                
                # 抓取该微博的评论信息
                # comment_url = self.base_url + '/comment/' + tweet_item['weibo_url'].split('/')[-1] + '?page=1'
                # yield Request(url=comment_url, callback=self.parse_comment, meta={'weibo_url': tweet_item['weibo_url']},priority=2)

                # Crawl tweet repost
                # repost_url = self.base_url + '/repost/' + tweet_item['weibo_url'].split('/')[-1] + '?page=1'
                # yield Request(url=repost_url, callback=self.parse_repost, meta={'weibo_url': tweet_item['weibo_url']},priority=2)
         
            except Exception as e:
                self.logger.error(e)

        #  keep looping until hit page with time range limit
        
        print("[DEBUG] timeflag:" + str(time_stop_flag))
        if time_stop_flag == 0: 
            next_page = current_page + 1
            page_url = response.url.replace('page='+str(current_page), 'page={}'.format(next_page))
            yield Request(page_url, self.parse_tweet, dont_filter=True, meta=response.meta,priority=1)
    

    

    def parse_all_content(self, response):
        # 有阅读全文的情况，获取全文
        selector = Selector(response)
        tweet_item = TweetItem()
        
        tweet_item['_id'] = self.get_tweet_id(response.url)
        tweet_item['user_id'] = response.meta["user_id"]
        tweet_item['page_url'] = response.url
        tweet_item['page_raw'] = selector.extract() # get raw page content
        tweet_item['crawl_time_utc'] = dt.utcnow()
        yield tweet_item


    def parse_comment(self,response):
        # 如果是第1页，一次性获取后面的所有页
        if response.url.endswith('page=1'):
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse_comment, dont_filter=True, meta=response.meta, priority = 2)
        
        selector = Selector(response)
        commentpage_item = CommentPageItem()
        commentpage_item['user_id'] = response.meta["user_id"]
        commentpage_item['page_url'] = response.url
        commentpage_item['page_raw'] = selector.extract() # get raw page content
        commentpage_item['crawl_time_utc'] = dt.utcnow()
        yield commentpage_item

    def parse_repost(self,response):
        # 如果是第1页，一次性获取后面的所有页
        if response.url.endswith('page=1'):
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse_repost, dont_filter=True, meta=response.meta, priority = 2)
        
        selector = Selector(response)
        repostpage_item = RepostPageItem()
        repostpage_item['user_id'] = response.meta["user_id"]
        repostpage_item['page_url'] = response.url
        repostpage_item['page_raw'] = selector.extract() # get raw page content
        repostpage_item['crawl_time_utc'] = dt.utcnow()
        yield repostpage_item


    def parse_follow(self, response):
        """
        抓取关注列表
        """
        # 如果是第1页，一次性获取后面的所有页
        if response.url.endswith('page=1'):
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse_follow, dont_filter=True, meta=response.meta, priority = 3)

        selector = Selector(response)
        relationships_item = RelationPageItem()
        relationships_item["page_url"] = response.url
        relationships_item["page_raw"] = selector.extract() # get raw page content
        relationships_item["user_id"] = response.meta["user_id"]
        relationships_item["relationship"] = "follow"
        relationships_item['crawl_time_utc'] = dt.utcnow()
        yield relationships_item

    def parse_fans(self, response):
        """
        抓取粉丝列表
        """
        # 如果是第1页，一次性获取后面的所有页
        if response.url.endswith('page=1'):
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse_fans, dont_filter=True, meta=response.meta,priority = 3)

        selector = Selector(response)
        relationships_item = RelationPageItem()
        relationships_item["page_url"] = response.url
        relationships_item["page_raw"] = selector.extract() # get raw page content
        relationships_item["user_id"] = response.meta["user_id"]
        relationships_item["relationship"] = "fan"
        relationships_item['crawl_time_utc'] = dt.utcnow()
        yield relationships_item


if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl('weibo_user_raw_spider')
    process.start()
    print("[INFO] Parser Started")