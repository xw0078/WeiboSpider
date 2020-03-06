
# set all settings to be the same as the one in main directory
import shutil

setting_paths = [
    'crawl_user_profiles/sina/settings.py',
    'crawl_user_timeline/sina/settings.py',
    'crawl_content_truncated_statuses/sina/settings.py',
    'crawl_search/sina/settings.py'
]

for path in setting_paths:
    newfile = shutil.copy('settings.py', path)
    print("Setting copied: "+newfile)