import os
import pandas as pd
from apiclient.discovery import build
from dotenv import load_dotenv

# .envファイルを読み込む
load_dotenv() 

VIDEO_ID = os.environ.get("VIDEO_ID")
API_KEY = os.environ.get("API_KEY")

youtube = build('youtube',
                'v3',
                developerKey=API_KEY
                )

videos_response = youtube.videos().list(
    part='snippet,statistics',
    id='{},'.format(VIDEO_ID)
).execute()

# snippet
snippetInfo = videos_response["items"][0]["snippet"]

# 動画タイトル
title = snippetInfo['title']

# チャンネル名
channeltitle = snippetInfo['channelTitle']
print(channeltitle)
print(title)