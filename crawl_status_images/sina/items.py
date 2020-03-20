# -*- coding: utf-8 -*-
from scrapy import Item, Field

class ImageItem(Item):
    image_urls = Field()
    images = Field()

class MultiImagePageRawItem(Item):
    _id = Field()
    crawl_time_utc = Field()
    page_url = Field()
    page_raw = Field()
    multi_img_ids = Field()


    