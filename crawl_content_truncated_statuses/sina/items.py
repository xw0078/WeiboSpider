# -*- coding: utf-8 -*-
from scrapy import Item, Field

class StatusPageItem(Item):
    _id = Field()  # System Object ID
    crawl_time_utc = Field()
    page_url = Field()
    page_raw = Field()


    