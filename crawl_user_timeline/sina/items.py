# -*- coding: utf-8 -*-
from scrapy import Item, Field

class ProfileUpdateItem(Item):
    """ 个人信息 """
    timelineCrawlJob_current_page = Field()
    timelineCrawlJob_current_complete = Field()
    timelineCrawlJob_run_history = Field()
    uid = Field()

class TimelinePageRaw(Item):
    _id = Field()  # System Object ID
    crawl_time_utc = Field()
    page_url = Field()
    page_raw = Field()
    user_id = Field()
