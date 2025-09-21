import os
# import pandas as pd
from apiclient.discovery import build
from dotenv import load_dotenv

# .envファイルを読み込む
load_dotenv() 

CHANNEL_ID = os.environ.get("CHANNEL_ID")
API_KEY = os.environ.get("API_KEY")

youtube = build('youtube',
                'v3',
                developerKey=API_KEY
                )

channels_response = youtube.channels().list(
    part='statistics, contentDetails, brandingSettings',
    id=CHANNEL_ID
).execute()

# レスポンスから情報を抽出
channel_info = channels_response["items"][0]
statistics_info = channel_info["statistics"]
content_info = channel_info['contentDetails']
branding_info = channel_info['brandingSettings']

print(statistics_info)
print(content_info)
print(branding_info)
