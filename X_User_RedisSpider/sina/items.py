# -*- coding: utf-8 -*-
from scrapy import Item, Field


class TweetsItem(Item):
    """ 微博信息 """
    _id = Field()  # 微博id
    weibo_url = Field()  # 微博URL
    created_at = Field()  # 微博发表时间
    like_num = Field()  # 点赞数 array
    repost_num = Field()  # 转发数 array
    comment_num = Field()  # 评论数 array
    content = Field()  # 微博内容
    content_links = Field() # links in the content
    user_id = Field()  # 发表该微博用户的id
    tool = Field()  # 发布微博的工具
    multi_imgs = Field() # If has multiple images
    image_url = Field()  # 图片
    video_url = Field()  # 视频
    location_map_info = Field()  # 定位的经纬度信息
    retweet = Field() # if retweet
    origin_weibo = Field()  # 原始微博，只有转发的微博才有这个字段
    crawl_time_utc = Field()  # 抓取时间戳 array
class ImgItem(Item):
    """ 微博信息 """
    _id = Field()  # 微博id
    imgList = Field()  # 图片
    crawl_time_utc = Field() # crawl timestamp, array
    
class InformationItem(Item):
    """ 个人信息 """
    _id = Field()  # 用户ID
    optional_id = Field() #optional user url id
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


class RelationshipsItem(Item):
    """ 用户关系，只保留与关注的关系 """
    _id = Field()
    fan_id = Field()  # 关注者,即粉丝的id
    followed_id = Field()  # 被关注者的id
    crawl_time_utc = Field()  # 抓取时间戳 array


class CommentItem(Item):
    """
    微博评论信息
    """
    _id = Field()
    comment_user_id = Field()  # 评论用户的id
    content = Field()  # 评论的内容
    weibo_url = Field()  # 评论的微博的url
    created_at = Field()  # 评论发表时间
    like_num = Field()  # 点赞数 array
    hot_comment = Field() # if hot comment, array
    crawl_time_utc = Field()  # 抓取时间戳 array

class RepostItem(Item):
    """
    Repost Information
    """
    _id = Field() # custom id
    repost_user_id = Field()  # repost user id
    content = Field()  # repost content
    weibo_url = Field()  # tweet url
    created_at = Field()  # repost time
    like_num = Field()  # repost likes, array
    hot_repost = Field() # if repost is marked as hot repost, array
    crawl_time_utc = Field()  # crawl time array

     