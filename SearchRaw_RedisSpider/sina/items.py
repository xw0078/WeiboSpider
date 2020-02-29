# -*- coding: utf-8 -*-
from scrapy import Item, Field

class SearchPageItem(Item):
    crawl_time_utc = Field()
    page_url = Field()
    page_raw = Field()
    search_key = Field()
    sort_setting = Field()
    filter_setting = Field()


class InformationItem(Item):
    """ 个人信息 """
    _id = Field()  # 用户ID
    crawl_time_utc = Field()
    page_url = Field()
    page_raw = Field()


class TweetPageItem(Item):
    """ 用户关系，只保留与关注的关系 """
    _id = Field()  # System Object ID
    crawl_time_utc = Field()
    page_url = Field()
    page_raw = Field()
    user_id = Field()

class TweetItem(Item):
    _id = Field() # System Object ID
    crawl_time_utc = Field()
    page_url = Field()
    page_raw = Field()
    user_id = Field()

class CommentPageItem(Item):
    _id = Field() # System Object ID
    crawl_time_utc = Field()
    page_url = Field()
    page_raw = Field()
    user_id = Field()

class RepostPageItem(Item):
    _id = Field() # System Object ID
    crawl_time_utc = Field()
    page_url = Field()
    page_raw = Field()
    user_id = Field()

class RelationPageItem(Item):
    """ 用户关系，只保留与关注的关系 """
    _id = Field()  # System Object ID
    crawl_time_utc = Field()
    page_url = Field()
    page_raw = Field()
    user_id = Field()
    relationship = Field()
    