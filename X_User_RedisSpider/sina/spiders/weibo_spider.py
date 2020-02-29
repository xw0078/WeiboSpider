#!/usr/bin/env python
# encoding: utf-8
import re
from lxml import etree
from scrapy.crawler import CrawlerProcess
from scrapy.selector import Selector
from scrapy.http import Request
from scrapy.utils.project import get_project_settings
from scrapy_redis.spiders import RedisSpider
from sina.items import TweetsItem, InformationItem, RelationshipsItem, CommentItem, ImgItem, RepostItem
from sina.spiders.utils import time_fix, extract_weibo_content, extract_comment_content
import time
from datetime import datetime as dt

class WeiboSpider(RedisSpider):
    name = "weibo_spider"
    base_url = "https://weibo.cn"
    redis_key = "weibo_spider:start_urls"
    all_page_num = 0
    current_page = 0
    

    custom_settings = {
        'CONCURRENT_REQUESTS': 16,
        "DOWNLOAD_DELAY": 1,
    }

    def __init__(self, *a, **kw):
        super(WeiboSpider, self).__init__(*a, **kw)
        settings=get_project_settings()
        time_start_str = settings.get('TIME_START')
        self.time_start_from = dt.strptime(time_start_str, "%Y-%m-%d %H:%M")


    def time_flag_compare(self, timeString):
        #print("[DEBUG] Created Time String: "+timeString)
        time = dt.strptime(timeString,'%Y-%m-%d %H:%M')
        if self.time_start_from > time:
            print("[INFO] Hit Start Time Criteria")
            return 1
        else:
            return 0

    # 默认初始解析函数
    def parse(self, response):
        """ 抓取个人信息 """
        information_item = InformationItem()

        selector = Selector(response)
        information_item['_id'] = re.findall('(\d+)/info', response.url)[0]
        text1 = ";".join(selector.xpath('body/div[@class="c"]//text()').extract())  # 获取标签里的所有text()
        nick_name = re.findall('昵称;?[：:]?(.*?);', text1)
        gender = re.findall('性别;?[：:]?(.*?);', text1)
        place = re.findall('地区;?[：:]?(.*?);', text1)
        briefIntroduction = re.findall('简介;?[：:]?(.*?);', text1)
        birthday = re.findall('生日;?[：:]?(.*?);', text1)
        sex_orientation = re.findall('性取向;?[：:]?(.*?);', text1)
        sentiment = re.findall('感情状况;?[：:]?(.*?);', text1)
        vip_level = re.findall('会员等级;?[：:]?(.*?);', text1)
        authentication = re.findall('认证;?[：:]?(.*?);', text1)
        labels = re.findall('标签;?[：:]?(.*?)更多>>', text1)
        optional_id = re.findall("手机版:[^;]*", text1)[0].split("/")[-1]

        if optional_id and optional_id[0]:
            information_item["optional_id"] = optional_id
        else:
            information_item["optional_id"] = "NA"

        if nick_name and nick_name[0]:
            information_item["nick_name"] = nick_name[0].replace(u"\xa0", "")
        else:
            information_item["nick_name"] = "NA"

        if gender and gender[0]:
            information_item["gender"] = gender[0].replace(u"\xa0", "")
        else:
            information_item["gender"] = "NA"

        if place and place[0]:
            place = place[0].replace(u"\xa0", "").split(" ")
            information_item["province"] = place[0]
            if len(place) > 1:
                information_item["city"] = place[1]
            else:
                information_item["city"] = "NA"
        else:
            information_item["province"] = "NA"
            information_item["city"] = "NA"

        if briefIntroduction and briefIntroduction[0]:
            information_item["brief_introduction"] = briefIntroduction[0].replace(u"\xa0", "")
        else:
            information_item["brief_introduction"] = "NA"

        if birthday and birthday[0]:
            information_item['birthday'] = birthday[0]
        else:
            information_item['birthday'] = "NA"

        if sex_orientation and sex_orientation[0]:
            if sex_orientation[0].replace(u"\xa0", "") == gender[0]:
                information_item["sex_orientation"] = "同性恋"
            else:
                information_item["sex_orientation"] = "异性恋"
        else:
            information_item["sex_orientation"] = "NA"
        
        if sentiment and sentiment[0]:
            information_item["sentiment"] = sentiment[0].replace(u"\xa0", "")
        else:
            information_item["sentiment"] = "NA"

        if vip_level and vip_level[0]:
            information_item["vip_level"] = vip_level[0].replace(u"\xa0", "")
        else:
            information_item["vip_level"] = "NA"

        if authentication and authentication[0]:
            information_item["authentication"] = authentication[0].replace(u"\xa0", "")
        else:
            information_item["authentication"] = "NA"

        if labels and labels[0]:
            information_item["labels"] = labels[0].replace(u"\xa0", ",").replace(';', '').strip(',')
        else:
            information_item["labels"] = "NA"

        request_meta = response.meta
        request_meta['item'] = information_item
        yield Request(self.base_url + '/u/{}'.format(information_item['_id']),
                      callback=self.parse_further_information,
                      meta=request_meta, dont_filter=True, priority=1)

    def parse_further_information(self, response):
        text = response.text
        information_item = response.meta['item']
        
        tweets_num = re.findall('微博\[(\d+)\]', text)
        if tweets_num:
            information_item['tweets_num'] = int(tweets_num[0])
        else:
            information_item['tweets_num'] = "NA"

        follows_num = re.findall('关注\[(\d+)\]', text)
        if follows_num:
            information_item['follows_num'] = int(follows_num[0])
        else:
            information_item['follows_num'] = "NA"

        fans_num = re.findall('粉丝\[(\d+)\]', text)
        if fans_num:
            information_item['fans_num'] = int(fans_num[0])
        else:
            information_item['fans_num'] = "NA"
        information_item['crawl_time_utc'] = dt.utcnow()
        yield information_item

        # 获取该用户微博
        yield Request(url=self.base_url + '/{}/profile?page=1'.format(information_item['_id']),
                      callback=self.parse_tweet,
                      priority=1)

        # 获取关注列表
        # if information_item['follows_num'] < 500: # if no more than 500 follows
        #     yield Request(url=self.base_url + '/{}/follow?page=1'.format(information_item['_id']),
        #                 callback=self.parse_follow,
        #                 dont_filter=True,priority=3)
        # 获取粉丝列表
        # if information_item['fans_num'] < 500: # if no more than 500 fans
        #     yield Request(url=self.base_url + '/{}/fans?page=1'.format(information_item['_id']),
        #                 callback=self.parse_fans,
        #                 dont_filter=True,priority=3)
 


    def parse_multi_images(self, response):
        img_item = ImgItem()
        img_item['_id'] = response.meta["_id"]
        tree_node = etree.HTML(response.body)
        images = tree_node.xpath('.//img/@src')
        img_item['imgList'] = " ".join(images)
        img_item['crawl_time_utc'] = dt.utcnow()
        #print("[DEBUG] Image List: " + img_item['imgList'])
        #print("[DEBUG] Image List Number: " + str(len(images)))
        yield img_item

    def parse_tweet(self, response):
        if response.url.endswith('page=1'):
            # if page 1, get all page number
            self.current_page = 1
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                self.all_page_num = all_page
        print("[INFO] Crawling Tweets Page: "+str(self.current_page))
        """
        解析本页的数据
        """
        time_stop_flag = 0 # stop crawling if hit specified start time
        tree_node = etree.HTML(response.body)
        tweet_nodes = tree_node.xpath('//div[@class="c" and @id]')
        for tweet_node in tweet_nodes:
            try:
                tweet_item = TweetsItem()
                tweet_item['crawl_time_utc'] = dt.utcnow() # insert datetime timestamp utc
                tweet_repost_url = tweet_node.xpath('.//a[contains(text(),"转发[")]/@href')[0]
                user_tweet_id = re.search(r'/repost/(.*?)\?uid=(\d+)', tweet_repost_url)
                tweet_item['weibo_url'] = 'https://weibo.com/{}/{}'.format(user_tweet_id.group(2),
                                                                           user_tweet_id.group(1))
                tweet_item['user_id'] = user_tweet_id.group(2)
                # if tweet_item['user_id']:
                #     print("[DEBUG] user_id:" + str(tweet_item['user_id']))
                # else:
                #     print("[DEBUG] user_id ERROR")

                tweet_item['_id'] = '{}_{}'.format(user_tweet_id.group(2), user_tweet_id.group(1))
                create_time_info_node = tweet_node.xpath('.//span[@class="ct"]')[-1]
                create_time_info = create_time_info_node.xpath('string(.)')
                if "来自" in create_time_info:
                    tweet_item['created_at'] = time_fix(create_time_info.split('来自')[0].strip())
                    self.time_stop_flag = self.time_flag_compare(tweet_item['created_at']) # time compare to trigger stop flag
                    tweet_item['tool'] = create_time_info.split('来自')[1].strip()
                else:
                    tweet_item['created_at'] = time_fix(create_time_info.strip())
                    self.time_stop_flag = self.time_flag_compare(tweet_item['created_at']) # time compare to trigger stop flag
                    tweet_item['tool'] = ""

                like_num = tweet_node.xpath('.//a[contains(text(),"赞[")]/text()')[-1]
                tweet_item['like_num'] = int(re.search('\d+', like_num).group())
                #print("[DEBUG] like_num:" + str(tweet_item['like_num']))
                repost_num = tweet_node.xpath('.//a[contains(text(),"转发[")]/text()')[-1]
                tweet_item['repost_num'] = int(re.search('\d+', repost_num).group())
                #print("[DEBUG] repost_num:" + str(tweet_item['repost_num']))
                comment_num = tweet_node.xpath(
                    './/a[contains(text(),"评论[") and not(contains(text(),"原文"))]/text()')[-1]
                tweet_item['comment_num'] = int(re.search('\d+', comment_num).group())
                #print("[DEBUG] comment_num:" + str(tweet_item['comment_num']))
                # Add to grab all images 1) test if multi images link exists 2) if not use the 
                multi_img_link = tweet_node.xpath('.//a[contains(text(),"组图")]/@href')
                if multi_img_link:
                    #print("[DEBUG] multi_img_link:" + multi_img_link[-1])
                    tweet_item['multi_imgs'] = True
                    yield Request(url=multi_img_link[-1], callback=self.parse_multi_images, meta={'_id': tweet_item['_id']},priority = 1)
                else:
                    tweet_item['multi_imgs'] = False

                images = tweet_node.xpath('.//img[@alt="图片"]/@src')
                if images:
                    tweet_item['image_url'] = images[0]
                else:
                    tweet_item['image_url'] = "NA"
 
                videos = tweet_node.xpath('.//a[contains(@href,"https://m.weibo.cn/s/video/show?object_id=")]/@href')
                if videos:
                    tweet_item['video_url'] = videos[0]
                else:
                    tweet_item['video_url'] = "NA"

                map_node = tweet_node.xpath('.//a[contains(text(),"显示地图")]')
                if map_node:
                    map_node = map_node[0]
                    map_node_url = map_node.xpath('./@href')[0]
                    map_info = re.search(r'xy=(.*?)&', map_node_url).group(1)
                    tweet_item['location_map_info'] = map_info
                else:
                    tweet_item['location_map_info'] = "NA"

                repost_node = tweet_node.xpath('.//a[contains(text(),"原文评论[")]/@href')
                if repost_node:
                    tweet_item['retweet'] = True
                    tweet_item['origin_weibo'] = repost_node[0]
                    # crawl original weibo
                    # origin_weibo_url = self.base_url + '/repost/' + tweet_item['weibo_url'].split('/')[-1] + '?page=1'
                    # yield Request(url=repost_url, callback=self.parse_repost, meta={'weibo_url': tweet_item['weibo_url']},priority = 2)

                else:
                    tweet_item['retweet'] = False
                    tweet_item['origin_weibo'] = "NA"
                # 检测由没有阅读全文:
                all_content_link = tweet_node.xpath('.//a[text()="全文" and contains(@href,"ckAll=1")]')
                if all_content_link:
                    all_content_url = self.base_url + all_content_link[0].xpath('./@href')[0]
                    yield Request(all_content_url, callback=self.parse_all_content, meta={'item': tweet_item},
                                  priority=1)

                else:
                    tweet_html = etree.tostring(tweet_node, encoding='unicode')
                    tweet_item['content'] = tweet_node.xpath('string(./div/span[@class="ctt"])')
                    tweet_item['content'] = 
                    yield tweet_item

                # 抓取该微博的评论信息
                comment_url = self.base_url + '/comment/' + tweet_item['weibo_url'].split('/')[-1] + '?page=1'
                yield Request(url=comment_url, callback=self.parse_comment, meta={'weibo_url': tweet_item['weibo_url']},priority=2)

                # Crawl tweet repost
                repost_url = self.base_url + '/repost/' + tweet_item['weibo_url'].split('/')[-1] + '?page=1'
                yield Request(url=repost_url, callback=self.parse_repost, meta={'weibo_url': tweet_item['weibo_url']},priority=2)
         
            except Exception as e:
                self.logger.error(e)

        #  keep looping until hit page with time range limit
        self.current_page = self.current_page + 1
        if self.time_stop_flag == 0 and self.current_page < (self.all_page_num + 1) and self.current_page >=2: 
            next_page = self.current_page
            current_page_str = "page="+str(next_page-1)
            page_url = response.url.replace(current_page_str, 'page={}'.format(next_page))
            yield Request(page_url, self.parse_tweet, dont_filter=True, meta=response.meta,priority=1)
 

    def parse_all_content(self, response):
        # 有阅读全文的情况，获取全文
        tree_node = etree.HTML(response.body)
        tweet_item = response.meta['item']
        content_node = tree_node.xpath('//*[@id="M_"]/div[1]')[0]
        tweet_html = etree.tostring(content_node, encoding='unicode')
        tweet_item['content'] = tree_node.xpath('string(//div[@class="c" and @id="M_"]/div/span)')
        yield tweet_item

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
        urls = selector.xpath('//a[text()="关注他" or text()="关注她" or text()="取消关注"]/@href').extract()
        uids = re.findall('uid=(\d+)', ";".join(urls), re.S)
        ID = re.findall('(\d+)/follow', response.url)[0]
        for uid in uids:
            relationships_item = RelationshipsItem()
            relationships_item['crawl_time_utc'] = dt.utcnow()
            relationships_item["fan_id"] = ID
            relationships_item["followed_id"] = uid
            relationships_item["_id"] = ID + '-' + uid
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
        urls = selector.xpath('//a[text()="关注他" or text()="关注她" or text()="移除"]/@href').extract()
        uids = re.findall('uid=(\d+)', ";".join(urls), re.S)
        ID = re.findall('(\d+)/fans', response.url)[0]
        for uid in uids:
            relationships_item = RelationshipsItem()
            relationships_item['crawl_time_utc'] = dt.utcnow()
            relationships_item["fan_id"] = uid
            relationships_item["followed_id"] = ID
            relationships_item["_id"] = uid + '-' + ID
            yield relationships_item

    def parse_comment(self, response):
        # 如果是第1页，一次性获取后面的所有页
        if response.url.endswith('page=1'):
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse_comment, dont_filter=True, meta=response.meta, priority = 2)
        tree_node = etree.HTML(response.body)
        comment_nodes = tree_node.xpath('//div[@class="c" and contains(@id,"C_") and .//span[contains(@class,"cc")]]')
        for comment_node in comment_nodes:
            comment_user_url = comment_node.xpath('.//a[contains(@href,"/")]/@href')
            if not comment_user_url:
                continue
            comment_item = CommentItem()
            comment_item['crawl_time_utc'] = dt.utcnow()
            comment_item['weibo_url'] = response.meta['weibo_url']
            comment_item['comment_user_id'] = re.search(r'(/u/(\d+))|(/(\w+))', comment_user_url[0]).group(0)
            comment_item['content'] = extract_comment_content(etree.tostring(comment_node, encoding='unicode'))
            comment_item['_id'] = comment_node.xpath('./@id')[0]
            created_at_info = comment_node.xpath('.//span[@class="ct"]/text()')[0]
            like_num = comment_node.xpath('.//a[contains(text(),"赞[")]/text()')[-1]
            comment_item['like_num'] = int(re.search('\d+', like_num).group())
            comment_item['created_at'] = time_fix(created_at_info.split('\xa0')[0])
            hot_comment = comment_node.xpath('.//span[@class="kt"]/text()')
            if hot_comment:
                comment_item['hot_comment'] = True
            else:
                comment_item['hot_comment'] = False
            yield comment_item

    def parse_repost(self, response):
        # 如果是第1页，一次性获取后面的所有页
        if response.url.endswith('page=1'):
            all_page = re.search(r'/>&nbsp;1/(\d+)页</div>', response.text)
            if all_page:
                all_page = all_page.group(1)
                all_page = int(all_page)
                for page_num in range(2, all_page + 1):
                    page_url = response.url.replace('page=1', 'page={}'.format(page_num))
                    yield Request(page_url, self.parse_repost, dont_filter=True, meta=response.meta, priority = 2)
        tree_node = etree.HTML(response.body)
        repost_nodes = tree_node.xpath('//div[@class="c" and not(@id="M_") and .//span[contains(@class,"cc")]]')
        for repost_node in repost_nodes:
            repost_user_url = repost_node.xpath('.//a[contains(@href,"/")]/@href')
            if not repost_user_url:
                continue
            repost_item = RepostItem()
            repost_item['crawl_time_utc'] = dt.utcnow()
            repost_item['weibo_url'] = response.meta['weibo_url']
            repost_item['repost_user_id'] = re.search(r'(/u/(\d+))|(/(\w+))', repost_user_url[0]).group(0)
            repost_item['content'] = repost_node.xpath('.//text()')[0]
            like_num = repost_node.xpath('.//a[contains(text(),"赞[")]/text()')[-1]
            repost_item['like_num'] = int(re.search('\d+', like_num).group())
            created_at_info = repost_node.xpath('.//span[@class="ct"]/text()')[0]
            #print("[DEBUG] repost CT:"+created_at_info)
            #print("[DEBUG] repost CT:"+created_at_info.split('\xa0')[0])
            repost_item['created_at'] = time_fix(created_at_info.strip('\xa0').split('\xa0')[0])
            #print("[DEBUG] repost CT:"+repost_item['created_at'])
            repost_item['_id'] = repost_item['repost_user_id']+repost_item['weibo_url']
            hot_repost = repost_node.xpath('.//span[@class="kt"]/text()')
            if hot_repost:
                repost_item['hot_repost'] = True
            else:
                repost_item['hot_repost'] = False

            yield repost_item


if __name__ == "__main__":
    process = CrawlerProcess(get_project_settings())
    process.crawl('weibo_spider')
    process.start()
    print("[INFO] Parser Started")