# -*- coding: utf-8 -*-
from scrapy import Item, Field

class ProfileItem(Item):
    """ 个人信息 """
    _id = Field()  # 用户ID
    custom_id = Field() #optional user url id
    nick_name = Field()  # 昵称
    gender = Field()  # 性别
    province = Field()  # 所在省
    city = Field()  # 所在城市
    brief_introduction = Field()  # 简介
    birthday = Field()  # 生日
    tweets_num = Field()  # 微博数 array
    follows_num = Field()  # 关注数 array
    fans_num = Field()  # 粉丝数 array
    sex_orientation = Field()  # 性取向
    sentiment = Field()  # 感情状况
    vip_level = Field()  # 会员等级
    authentication = Field()  # 认证
    labels = Field()  # 标签
    crawl_time_utc = Field()  # 抓取时间戳 array
    timelineCrawlJob_current_page = Field()
    timelineCrawlJob_current_complete = Field()
    timelineCrawlJob_run_history = Field()

class ProfileRawItem(Item):
    _id = Field()
    uid = Field()
    page_url = Field()
    page_raw = Field()
    crawl_time_utc = Field()