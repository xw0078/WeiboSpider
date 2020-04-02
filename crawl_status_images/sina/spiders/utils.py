#!/usr/bin/env python
# encoding: utf-8
import re
import datetime
import dateutil.parser
from sina.settings import DB_NAME,LOCAL_MONGO_HOST,LOCAL_MONGO_PORT
import pymongo

def get_random_proxy(proxy_type):
    myclient = pymongo.MongoClient(LOCAL_MONGO_HOST, LOCAL_MONGO_PORT)
    mydb = myclient[DB_NAME]["proxies"]
    print("[INFO] proxy type:"+proxy_type)
    if proxy_type == "https://weibo.cn/":
        pipeline = [
            { "$match": { "status": "success" }},
            { "$match": { "target": "https://weibo.cn/" }},
            { "$sample": { "size": 1 } }
        ]
    elif "sinaimg.cn" in proxy_type:
        pipeline = [
            { "$match": { "status": "success" }},
            { "$match": { "target": proxy_type }},
            { "$sample": { "size": 1 } }
        ]
    else:
        print("[ERROR] Wrong proxy type")
        return 

    results = mydb.aggregate(pipeline)
    results = list(results)
    if results:
        proxy = results[0]["_id"]
        return proxy
    else:
        raise Exception('all proxies are bad')


def time_fix(time_string):
    now_time = datetime.datetime.now()
    if '分钟前' in time_string:
        minutes = re.search(r'^(\d+)分钟', time_string).group(1)
        created_at = now_time - datetime.timedelta(minutes=int(minutes))
        return created_at.strftime('%Y-%m-%d %H:%M')

    if '小时前' in time_string:
        minutes = re.search(r'^(\d+)小时', time_string).group(1)
        created_at = now_time - datetime.timedelta(hours=int(minutes))
        return created_at.strftime('%Y-%m-%d %H:%M')

    if '今天' in time_string:
        return time_string.replace('今天', now_time.strftime('%Y-%m-%d'))

    if '月' in time_string:
        time_string = time_string.replace('月', '-').replace('日', '')
        time_string = str(now_time.year) + '-' + time_string
        return time_string

    return time_string


def extract_weibo_content(weibo_html):
    s = weibo_html
    if '转发理由' in s:
        s = s.split('转发理由:', maxsplit=1)[1]
    if 'class="ctt">' in s:
        s = s.split('class="ctt">', maxsplit=1)[1]
    s = s.split('赞', maxsplit=1)[0]
    s = keyword_re.sub('', s)
    s = emoji_re.sub('', s)
    s = url_re.sub('', s)
    s = div_re.sub('', s)
    s = image_re.sub('', s)
    if '<span class="ct">' in s:
        s = s.split('<span class="ct">')[0]
    s = white_space_re.sub(' ', s)
    s = s.replace('\xa0', '')
    s = s.strip(':')
    s = s.strip()
    return s


def extract_comment_content(comment_html):
    s = comment_html
    if 'class="ctt">' in s:
        s = s.split('class="ctt">', maxsplit=1)[1]
    s = s.split('举报', maxsplit=1)[0]
    s = emoji_re.sub('', s)
    s = keyword_re.sub('', s)
    s = url_re.sub('', s)
    s = div_re.sub('', s)
    s = image_re.sub('', s)
    s = white_space_re.sub(' ', s)
    s = s.replace('\xa0', '')
    s = s.strip(':')
    s = s.strip()
    return s

def extract_repost_content(comment_html):
    s = comment_html
    if 'class="ctt">' in s:
        s = s.split('class="ctt">', maxsplit=1)[1]
    s = s.split('举报', maxsplit=1)[0]
    s = emoji_re.sub('', s)
    s = keyword_re.sub('', s)
    s = url_re.sub('', s)
    s = div_re.sub('', s)
    s = image_re.sub('', s)
    s = white_space_re.sub(' ', s)
    s = s.replace('\xa0', '')
    s = s.strip(':')
    s = s.strip()
    return s















