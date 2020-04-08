
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

image_settings_file = open('crawl_status_images/sina/settings.py', "r")
content = image_settings_file.read()
content = content.replace("'sina.pipelines.MongoDBPipeline': 300,","'sina.pipelines.MongoDBPipeline': 300,\n\t'sina.pipelines.MyImagesPipeline': 100")
image_settings_file.close()

image_settings_file = open('crawl_status_images/sina/settings.py', "wt")
image_settings_file.write(content)
image_settings_file.close()