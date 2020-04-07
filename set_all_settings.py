
# set all settings to be the same as the one in main directory
import shutil

setting_paths = [
    'scripts/settings.py',
    'crawl_user_profiles/sina/settings.py',
    'crawl_user_timeline/sina/settings.py',
    'crawl_content_truncated_statuses/sina/settings.py',
    'crawl_search/sina/settings.py',
    'crawl_status_images/sina/settings.py'
]

for path in setting_paths:
    newfile = shutil.copy('settings.py', path)
    print("Setting copied: "+newfile)

# add image item to image crawl spider

with open('crawl_status_images/sina/settings.py', "a") as image_crawl_setting:
    image_crawl_setting.write("""
    
ITEM_PIPELINES = {
'sina.pipelines.MongoDBPipeline': 300,
'sina.pipelines.MyImagesPipeline': 100
}
    """)